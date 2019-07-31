// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use jsonrpc_core::{Error as RpcError, Result as RpcResult};
use rlp::Rlp;
use std::sync::Arc;

use cfx_types::{H160, H256};
use cfxcore::{ConsensusGraph, SynchronizationService, TransactionPool};

use primitives::{
    filter::FilterError, Action, SignedTransaction, Transaction,
    TransactionWithSignature,
};

use crate::rpc::{
    traits::cfx::public::Cfx,
    types::{
        Block as RpcBlock, Bytes, EpochNumber, Filter as RpcFilter,
        Log as RpcLog, Receipt, Transaction as RpcTransaction, H160 as RpcH160,
        H256 as RpcH256, U256 as RpcU256, U64 as RpcU64,
    },
};

#[derive(Clone)]
pub struct CfxHandler {
    pub consensus: Arc<ConsensusGraph>,
    sync: Arc<SynchronizationService>,
    tx_pool: Arc<TransactionPool>,
}

impl CfxHandler {
    pub fn new(
        consensus: Arc<ConsensusGraph>, sync: Arc<SynchronizationService>,
        tx_pool: Arc<TransactionPool>,
    ) -> Self
    {
        CfxHandler {
            consensus,
            sync,
            tx_pool,
        }
    }
}

impl Cfx for CfxHandler {
    //    fn account(
    //        &self, address: RpcH160, include_txs: bool, num_txs: RpcU64,
    //        epoch_num: Option<EpochNumber>,
    //    ) -> RpcResult<Account>
    //    {
    //        let inner = &mut *self.consensus.inner.write();
    //
    //        let address: H160 = address.into();
    //        let num_txs = num_txs.as_usize();
    //        let epoch_num = epoch_num.unwrap_or(EpochNumber::LatestState);
    //        info!(
    //            "RPC Request: cfx_getAccount address={:?} include_txs={:?}
    // num_txs={:?} epoch_num={:?}",            address, include_txs,
    // num_txs, epoch_num        );
    //        self.consensus
    //            .get_account(
    //                address,
    //                num_txs,
    //                epoch_num.into(),
    //            )
    //            .and_then(|(balance, transactions)| {
    //                Ok(Account {
    //                    balance: balance.into(),
    //                    transactions: BlockTransactions::new(
    //                        &transactions,
    //                        include_txs,
    //                        inner,
    //                    ),
    //                })
    //            })
    //            .map_err(|err| RpcError::invalid_params(err))
    //    }

    fn balance(
        &self, address: RpcH160, num: Option<EpochNumber>,
    ) -> RpcResult<RpcU256> {
        let num = num.unwrap_or(EpochNumber::LatestState);
        let address: H160 = address.into();
        info!(
            "RPC Request: cfx_getBalance address={:?} epoch_num={:?}",
            address, num
        );

        self.consensus
            .get_balance(address, num.into())
            .map(|x| x.into())
            .map_err(|err| RpcError::invalid_params(err))
    }

    fn best_block_hash(&self) -> RpcResult<RpcH256> {
        info!("RPC Request: cfx_getBestBlockHash()");
        Ok(self.consensus.best_block_hash().into())
    }

    fn block_by_epoch_number(
        &self, epoch_num: EpochNumber, include_txs: bool,
    ) -> RpcResult<RpcBlock> {
        let inner = &*self.consensus.inner.read();
        info!("RPC Request: cfx_getBlockByEpochNumber epoch_number={:?} include_txs={:?}", epoch_num, include_txs);
        let epoch_height = self
            .consensus
            .get_height_from_epoch_number(epoch_num.into())
            .map_err(|err| RpcError::invalid_params(err))?;
        inner
            .get_hash_from_epoch_number(epoch_height)
            .map_err(|err| RpcError::invalid_params(err))
            .and_then(|hash| {
                let block = self
                    .consensus
                    .data_man
                    .block_by_hash(&hash, false)
                    .unwrap();
                Ok(RpcBlock::new(&*block, inner, include_txs))
            })
    }

    fn block_by_hash_with_pivot_assumption(
        &self, block_hash: RpcH256, pivot_hash: RpcH256, epoch_number: RpcU64,
    ) -> RpcResult<RpcBlock> {
        let inner = &*self.consensus.inner.read();

        let block_hash: H256 = block_hash.into();
        let pivot_hash: H256 = pivot_hash.into();
        let epoch_number = epoch_number.as_usize() as u64;
        info!(
            "RPC Request: cfx_getBlockByHashWithPivotAssumption block_hash={:?} pivot_hash={:?} epoch_number={:?}",
            block_hash, pivot_hash, epoch_number
        );

        inner
            .check_block_pivot_assumption(&pivot_hash, epoch_number)
            .map_err(|err| RpcError::invalid_params(err))
            .and_then(|_| {
                if let Some(block) =
                    self.consensus.data_man.block_by_hash(&block_hash, false)
                {
                    debug!("Build RpcBlock {}", block.hash());
                    let result_block = RpcBlock::new(&*block, inner, true);
                    Ok(result_block)
                } else {
                    Err(RpcError::invalid_params(
                        "Error: can not find expected block".to_owned(),
                    ))
                }
            })
    }

    fn block_by_hash(
        &self, hash: RpcH256, include_txs: bool,
    ) -> RpcResult<Option<RpcBlock>> {
        let hash: H256 = hash.into();
        info!(
            "RPC Request: cfx_getBlockByHash hash={:?} include_txs={:?}",
            hash, include_txs
        );
        let inner = &*self.consensus.inner.read();

        if let Some(block) = self.consensus.data_man.block_by_hash(&hash, false)
        {
            let result_block = Some(RpcBlock::new(&*block, inner, include_txs));
            Ok(result_block)
        } else {
            Ok(None)
        }
    }

    fn blocks_by_epoch(&self, num: EpochNumber) -> RpcResult<Vec<RpcH256>> {
        info!("RPC Request: cfx_getBlocks epoch_number={:?}", num);

        self.consensus
            .block_hashes_by_epoch(num.into())
            .map_err(|err| RpcError::invalid_params(err))
            .and_then(|vec| Ok(vec.into_iter().map(|x| x.into()).collect()))
    }

    fn call(
        &self, rpc_tx: RpcTransaction, epoch: Option<EpochNumber>,
    ) -> RpcResult<Bytes> {
        let epoch = epoch.unwrap_or(EpochNumber::LatestState);

        let tx = Transaction {
            nonce: rpc_tx.nonce.into(),
            gas: rpc_tx.gas.into(),
            gas_price: rpc_tx.gas_price.into(),
            value: rpc_tx.value.into(),
            action: match rpc_tx.to {
                Some(to) => Action::Call(to.into()),
                None => Action::Create,
            },
            data: rpc_tx.data.into(),
        };
        debug!("RPC Request: cfx_call");
        let mut signed_tx = SignedTransaction::new_unsigned(
            TransactionWithSignature::new_unsigned(tx),
        );
        signed_tx.sender = rpc_tx.from.into();
        trace!("call tx {:?}", signed_tx);
        self.consensus
            .call_virtual(&signed_tx, epoch.into())
            .map(|output| Bytes::new(output.0))
            .map_err(|e| RpcError::invalid_params(e))
    }

    fn epoch_number(
        &self, epoch_num: Option<EpochNumber>,
    ) -> RpcResult<RpcU256> {
        let epoch_num = epoch_num.unwrap_or(EpochNumber::LatestMined);
        info!("RPC Request: cfx_epochNumber({:?})", epoch_num);
        match self
            .consensus
            .get_height_from_epoch_number(epoch_num.into())
        {
            Ok(height) => Ok(height.into()),
            Err(e) => Err(RpcError::invalid_params(e)),
        }
    }

    fn estimate_gas(&self, rpc_tx: RpcTransaction) -> RpcResult<RpcU256> {
        let tx = Transaction {
            nonce: rpc_tx.nonce.into(),
            gas: rpc_tx.gas.into(),
            gas_price: rpc_tx.gas_price.into(),
            value: rpc_tx.value.into(),
            action: match rpc_tx.to {
                Some(to) => Action::Call(to.into()),
                None => Action::Create,
            },
            data: rpc_tx.data.into(),
        };
        let mut signed_tx = SignedTransaction::new_unsigned(
            TransactionWithSignature::new_unsigned(tx),
        );
        signed_tx.sender = rpc_tx.from.into();
        trace!("call tx {:?}", signed_tx);
        let result = self.consensus.estimate_gas(&signed_tx);
        result
            .map_err(|e| {
                warn!("Transaction execution error {:?}", e);
                RpcError::internal_error()
            })
            .map(|x| x.into())
    }

    fn gas_price(&self) -> RpcResult<RpcU256> {
        info!("RPC Request: cfx_gasPrice()");
        Ok(self.consensus.gas_price().unwrap_or(0.into()).into())
    }

    fn get_logs(&self, filter: RpcFilter) -> RpcResult<Vec<RpcLog>> {
        info!("RPC Request: cfx_getLogs({:?})", filter);
        self.consensus
            .logs(filter.into())
            .map_err(|e| match e {
                FilterError::InvalidEpochNumber { .. } => {
                    RpcError::invalid_params(format!("{}", e))
                }
            })
            .map(|logs| logs.iter().cloned().map(RpcLog::from).collect())
    }

    fn send_raw_transaction(&self, raw: Bytes) -> RpcResult<RpcH256> {
        info!("RPC Request: cfx_sendRawTransaction bytes={:?}", raw);
        Rlp::new(&raw.into_vec())
            .as_val()
            .map_err(|err| {
                RpcError::invalid_params(format!("Error: {:?}", err))
            })
            .and_then(|tx| {
                let (signed_trans, failed_trans) = self.tx_pool.insert_new_transactions(
                    &vec![tx],
                );
                if signed_trans.len() + failed_trans.len() > 1 {
                    // This should never happen
                    error!("insert_new_transactions failed, invalid length of returned result vector {}", signed_trans.len() + failed_trans.len());
                    Ok(H256::new().into())
                } else if signed_trans.len() + failed_trans.len() == 0 {
                    // For tx in transactions_pubkey_cache, we simply ignore them
                    debug!("insert_new_transactions ignores inserted transactions");
                    Err(RpcError::invalid_params(String::from("tx already exist")))
                } else {
                    if signed_trans.is_empty() {
                        let mut tx_err = String::from("");
                        for (_, e) in failed_trans.iter() {
                            tx_err = e.clone();
                            break;
                        }
                        Err(RpcError::invalid_params(tx_err))
                    } else {
                        let tx_hash = signed_trans[0].hash();
                        self.sync.append_received_transactions(signed_trans);
                        Ok(tx_hash.into())
                    }
                }
            })
    }

    fn transaction_by_hash(
        &self, hash: RpcH256,
    ) -> RpcResult<Option<RpcTransaction>> {
        let hash: H256 = hash.into();
        info!("RPC Request: cfx_getTransactionByHash({:?})", hash);

        if let Some((transaction, receipt, tx_address)) =
            self.consensus.get_transaction_info_by_hash(&hash)
        {
            Ok(Some(RpcTransaction::from_signed(
                &transaction,
                Some(Receipt::new(transaction.clone(), receipt, tx_address)),
            )))
        } else {
            if let Some(transaction) = self.tx_pool.get_transaction(&hash) {
                return Ok(Some(RpcTransaction::from_signed(
                    &transaction,
                    None,
                )));
            }

            Ok(None)
        }
    }

    fn transaction_count(
        &self, address: RpcH160, num: Option<EpochNumber>,
    ) -> RpcResult<RpcU256> {
        let num = num.unwrap_or(EpochNumber::LatestState);
        info!(
            "RPC Request: cfx_getTransactionCount address={:?} epoch_num={:?}",
            address, num
        );

        self.consensus
            .transaction_count(address.into(), num.into())
            .map_err(|err| RpcError::invalid_params(err))
            .map(|x| x.into())
    }
}
