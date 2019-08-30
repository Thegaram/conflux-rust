// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use cfx_types::{Bloom, H160, H256};
use primitives::{
    filter::{Filter, FilterError},
    log_entry::{LocalizedLogEntry, LogEntry},
    Account, Receipt, SignedTransaction, StateRoot,
};
use std::sync::Arc;

extern crate futures;
use futures::{future, stream, Future, Stream};

use crate::{
    consensus::ConsensusGraph,
    network::{NetworkService, PeerId},
    parameters::consensus::DEFERRED_STATE_EPOCH_COUNT,
    statedb::StorageKey,
    storage,
    sync::SynchronizationGraph,
};

use super::{
    common::{poll_next, LedgerInfo},
    handler::QueryResult,
    message::{GetStateEntry, GetStateRoot, GetTxs},
    Error, ErrorKind, Handler as LightHandler, LIGHT_PROTOCOL_ID,
    LIGHT_PROTOCOL_VERSION,
};

pub struct QueryService {
    // shared consensus graph
    consensus: Arc<ConsensusGraph>,

    // ...
    handler: Arc<LightHandler>,

    // helper API for retrieving ledger information
    ledger: LedgerInfo,

    // ...
    network: Arc<NetworkService>,
}

impl QueryService {
    pub fn new(
        consensus: Arc<ConsensusGraph>, graph: Arc<SynchronizationGraph>,
        network: Arc<NetworkService>,
    ) -> Self
    {
        let handler = Arc::new(LightHandler::new(consensus.clone(), graph));
        let ledger = LedgerInfo::new(consensus.clone());

        QueryService {
            consensus,
            handler,
            ledger,
            network,
        }
    }

    pub fn register(&self) -> Result<(), String> {
        self.network
            .register_protocol(
                self.handler.clone(),
                LIGHT_PROTOCOL_ID,
                &[LIGHT_PROTOCOL_VERSION],
            )
            .map_err(|e| {
                format!("failed to register protocol QueryService: {:?}", e)
            })
    }

    pub fn query_state_root(
        &self, peer: PeerId, epoch: u64,
    ) -> Result<StateRoot, Error> {
        // TODO(thegaram): retrieve from cache
        info!("query_state_root epoch={:?}", epoch);

        let req = GetStateRoot {
            request_id: 0,
            epoch,
        };

        self.network.with_context(LIGHT_PROTOCOL_ID, |io| {
            match self.handler.query.execute(io, peer, req)? {
                QueryResult::StateRoot(sr) => Ok(sr),
                _ => Err(ErrorKind::UnexpectedResponse.into()),
            }
        })
    }

    pub fn query_state_entry(
        &self, peer: PeerId, epoch: u64, key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Error> {
        info!("query_state_entry epoch={:?} key={:?}", epoch, key);

        let req = GetStateEntry {
            request_id: 0,
            epoch,
            key,
        };

        self.network.with_context(LIGHT_PROTOCOL_ID, |io| {
            match self.handler.query.execute(io, peer, req)? {
                QueryResult::StateEntry(entry) => Ok(entry),
                _ => Err(ErrorKind::UnexpectedResponse.into()),
            }
        })
    }

    pub fn query_account(
        &self, peer: PeerId, epoch: u64, address: H160,
    ) -> Result<Option<Account>, Error> {
        info!(
            "query_account peer={:?} epoch={:?} address={:?}",
            peer, epoch, address
        );

        // retrieve state root from peer
        let state_root = self.query_state_root(peer, epoch)?;

        // calculate corresponding state trie key
        let key = {
            let padding = storage::MultiVersionMerklePatriciaTrie::padding(
                &state_root.snapshot_root,
                &state_root.intermediate_delta_root,
            );

            StorageKey::new_account_key(&address, &padding)
                .as_ref()
                .to_vec()
        };

        // retrieve state entry from peer
        let entry = self.query_state_entry(peer, epoch, key)?;

        let account = match entry {
            None => None,
            Some(entry) => Some(rlp::decode(&entry[..])?),
        };

        Ok(account)
    }

    pub fn get_account(&self, epoch: u64, address: H160) -> Option<Account> {
        info!("get_account epoch={:?} address={:?}", epoch, address);

        // try each peer until we succeed
        // TODO(thegaram): only query peers who already have `epoch`
        for peer in self.handler.peers.all_peers_shuffled() {
            match self.query_account(peer, epoch, address) {
                Ok(account) => return account,
                Err(e) => {
                    warn!(
                        "Failed to get account from peer={:?}: {:?}",
                        peer, e
                    );
                }
            };
        }

        None
    }

    /// Relay raw transaction to all peers.
    // TODO(thegaram): consider returning TxStatus instead of bool,
    // e.g. Failed, Sent/Pending, Confirmed, etc.
    pub fn send_raw_tx(&self, raw: Vec<u8>) -> bool {
        debug!("send_raw_tx raw={:?}", raw);

        let mut success = false;

        for peer in self.handler.peers.all_peers_shuffled() {
            // relay to peer
            let res = self.network.with_context(LIGHT_PROTOCOL_ID, |io| {
                self.handler.send_raw_tx(io, peer, raw.clone())
            });

            // check error
            match res {
                Err(e) => warn!("Failed to relay to peer={:?}: {:?}", peer, e),
                Ok(_) => {
                    debug!("Tx relay to peer {:?} successful", peer);
                    success = true;
                }
            }
        }

        success
    }

    pub fn query_txs(
        &self, peer: PeerId, hashes: Vec<H256>,
    ) -> Result<Vec<SignedTransaction>, Error> {
        info!("query_txs peer={:?} hashes={:?}", peer, hashes);

        let req = GetTxs {
            request_id: 0,
            hashes,
        };

        self.network.with_context(LIGHT_PROTOCOL_ID, |io| {
            match self.handler.query.execute(io, peer, req)? {
                QueryResult::Txs(txs) => Ok(txs),
                _ => Err(ErrorKind::UnexpectedResponse.into()),
            }
        })
    }

    pub fn get_tx(&self, hash: H256) -> Option<SignedTransaction> {
        info!("get_tx hash={:?}", hash);

        // try each peer until we succeed
        for peer in self.handler.peers.all_peers_shuffled() {
            match self.query_txs(peer, vec![hash]) {
                Err(e) => {
                    warn!("Failed to get txs from peer={:?}: {:?}", peer, e);
                }
                Ok(txs) => {
                    match txs.iter().find(|tx| tx.hash() == hash).cloned() {
                        Some(tx) => return Some(tx),
                        None => {
                            warn!(
                                "Peer {} returned {:?}, target tx not found!",
                                peer, txs
                            );
                        }
                    }
                }
            };
        }

        None
    }

    fn handle_receipt_logs(
        hash: H256, transaction_index: usize, mut logs: Vec<LogEntry>,
        filter: Filter,
    ) -> impl Iterator<Item = LocalizedLogEntry>
    {
        let num_logs = logs.len();

        // process logs in reverse order
        logs.reverse();

        logs.into_iter()
            .enumerate()
            .filter(move |(_, entry)| filter.matches(&entry))
            .map(move |(ii, entry)| LocalizedLogEntry {
                block_hash: hash,
                block_number: 0, // TODO
                entry,
                log_index: 0,           // TODO
                transaction_hash: hash, // will fill later
                transaction_index,
                transaction_log_index: num_logs - ii - 1,
            })
    }

    fn handle_block_receipts(
        hash: H256, mut receipts: Vec<Receipt>, filter: Filter,
    ) -> impl Iterator<Item = LocalizedLogEntry> {
        let num_receipts = receipts.len();

        // process block receipts in reverse order
        receipts.reverse();

        receipts.into_iter().map(|r| r.logs).enumerate().flat_map(
            move |(ii, logs)| {
                debug!("block_hash {:?} logs = {:?}", hash, logs);
                Self::handle_receipt_logs(
                    hash,
                    num_receipts - ii - 1,
                    logs,
                    filter.clone(),
                )
            },
        )
    }

    fn filter_logs(
        &self, epoch: u64, mut receipts: Vec<Vec<Receipt>>, filter: Filter,
    ) -> impl Iterator<Item = LocalizedLogEntry> {
        // get epoch blocks in execution order
        let mut hashes: Vec<H256> = self.ledger.block_hashes_in(epoch).unwrap(); // TODO

        // process epoch receipts in reverse order
        receipts.reverse();
        hashes.reverse();

        receipts
            .into_iter()
            .zip(hashes)
            .flat_map(move |(receipts, hash)| {
                debug!("block_hash {:?} receipts = {:?}", hash, receipts);
                Self::handle_block_receipts(hash, receipts, filter.clone())
            })
    }

    pub fn get_logs(
        &self, filter: Filter,
    ) -> Result<Vec<LocalizedLogEntry>, FilterError> {
        // TODO: filter.block_hashes

        let best = self.consensus.best_epoch_number();
        let latest_verified = self.handler.sync.witnesses.latest_verified();
        let latest_verifiable = latest_verified - DEFERRED_STATE_EPOCH_COUNT;
        // TODO: underflow...
        info!(
            "best={}, latest_verified={}, latest_verifiable={}",
            best, latest_verified, latest_verifiable
        );

        if latest_verified < DEFERRED_STATE_EPOCH_COUNT {
            return Ok(vec![]);
        }

        // at most best_epoch
        let from_epoch = match self
            .consensus
            .get_height_from_epoch_number(filter.from_epoch.clone())
        {
            Ok(num) => std::cmp::min(num, latest_verifiable),
            Err(_) => return Ok(vec![]),
        };

        // at most best_epoch
        let to_epoch = std::cmp::min(
            latest_verifiable,
            self.consensus
                .get_height_from_epoch_number(filter.to_epoch.clone())
                .unwrap_or(best),
        );

        if from_epoch > to_epoch {
            return Err(FilterError::InvalidEpochNumber {
                from_epoch,
                to_epoch,
            });
        }

        let blooms = filter.bloom_possibilities();
        let bloom_match = |block_log_bloom: &Bloom| {
            blooms
                .iter()
                .any(|bloom| block_log_bloom.contains_bloom(bloom))
        };

        let limit = filter.limit.map(|x| x as u64).unwrap_or(::std::u64::MAX);

        let mut epochs: Vec<u64> = (from_epoch..(to_epoch + 1)).collect();
        epochs.reverse();
        debug!("executing filter on epochs {:?}", epochs);

        let mut stream =
            // stream epochs in chunks
            stream::iter_ok::<_, Error>(epochs)

            // retrieve blooms
            .map(|epoch| {
                debug!("Requesting blooms for {:?}", epoch);
                self.handler.sync.blooms.request(epoch).map(move |bloom| (epoch, bloom))
            })

            // we first request up to 100 blooms and then wait for them and process them one by one
            // NOTE: we wrap our future into a future because we don't want to wait for the value yet
            .map(future::ok)
            .buffered(100)
            .and_then(|x| x)

            // find the epochs that match
            .filter_map(|(epoch, epoch_bloom)| {
                    debug!("epoch {:?} bloom = {:?}", epoch, epoch_bloom);

                    match bloom_match(&epoch_bloom) {
                    // match true {
                        true => Some(epoch),
                        false => None,
                    }
                },
            )

            // retrieve receipts
            .map(|epoch| {
                debug!("Requesting receipts for {:?}", epoch);
                self.handler.sync.receipts.request(epoch).map(move |receipts| (epoch, receipts))
            })

            // we first request up to 100 blooms and then wait for them and process them one by one
            .map(future::ok)
            .buffered(100)
            .and_then(|x| x)

            // filter logs in epoch
            .map(|(epoch, receipts)| {
                debug!("epoch {:?} receipts = {:?}", epoch, receipts);
                self.filter_logs(epoch, receipts, filter.clone()).collect::<Vec<LocalizedLogEntry>>()
            })

            // Stream<Vec<Log>> -> Stream<Log>
            .map(|logs| stream::iter_ok::<_, Error>(logs))
            .flatten()

            // retrieve block txs
            .map(|log| {
                debug!("Requesting block txs for {:?}", log.block_hash);
                self.handler.sync.block_txs.request(log.block_hash).map(move |x| (log.clone(), x))
            })

            // we first request up to 100 blooms and then wait for them and process them one by one
            .map(future::ok)
            .buffered(100)
            .and_then(|x| x)

            .map(|(mut log, tx_hashes)| {
                debug!("log = {:?}, tx_hashes = {:?}", log, tx_hashes);

                // TODO: check over-indexing
                log.transaction_hash = tx_hashes[log.transaction_index].hash();
                log
            })

            .take(limit);

        let mut matching = vec![];

        while let Some(x) = poll_next(&mut stream) {
            matching.push(x);
        }

        // TODO: wait without timeout at the end
        // propagate timeout error

        debug!("matching logs = {:?}", matching);
        matching.reverse();
        Ok(matching)
    }
}
