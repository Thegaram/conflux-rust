// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use jsonrpc_core::{Error as RpcError, Result as RpcResult};
use parking_lot::{Condvar, Mutex};
use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use cfx_types::H256;
use cfxcore::{ConsensusGraph, PeerInfo, TransactionPool};

use network::{
    node_table::{NodeEndpoint, NodeEntry, NodeId},
    NetworkService,
};

use crate::rpc::{
    traits::cfx::test::TestRpc,
    types::{Block, Receipt, Status, H256 as RpcH256},
};

#[derive(Clone)]
pub struct TestHandler {
    consensus: Arc<ConsensusGraph>,
    exit: Arc<(Mutex<bool>, Condvar)>,
    network: Arc<NetworkService>,
    tx_pool: Arc<TransactionPool>,
}

impl TestHandler {
    pub fn new(
        consensus: Arc<ConsensusGraph>, exit: Arc<(Mutex<bool>, Condvar)>,
        network: Arc<NetworkService>, tx_pool: Arc<TransactionPool>,
    ) -> Self
    {
        TestHandler {
            consensus,
            exit,
            network,
            tx_pool,
        }
    }
}

impl TestRpc for TestHandler {
    fn add_latency(&self, id: NodeId, latency_ms: f64) -> RpcResult<()> {
        match self.network.add_latency(id, latency_ms) {
            Ok(_) => Ok(()),
            Err(_) => Err(RpcError::internal_error()),
        }
    }

    fn add_peer(&self, node_id: NodeId, address: SocketAddr) -> RpcResult<()> {
        let node = NodeEntry {
            id: node_id,
            endpoint: NodeEndpoint {
                address,
                udp_port: address.port(),
            },
        };
        info!("RPC Request: add_peer({:?})", node.clone());
        match self.network.add_peer(node) {
            Ok(x) => Ok(x),
            Err(_) => Err(RpcError::internal_error()),
        }
    }

    fn chain(&self) -> RpcResult<Vec<Block>> {
        info!("RPC Request: cfx_getChain");
        let inner = &*self.consensus.inner.read();
        Ok(inner
            .all_blocks_with_topo_order()
            .iter()
            .map(|x| {
                Block::new(
                    self.consensus
                        .data_man
                        .block_by_hash(x, false)
                        .expect("Error to get block by hash")
                        .as_ref(),
                    inner,
                    true,
                )
            })
            .collect())
    }

    fn drop_peer(&self, node_id: NodeId, address: SocketAddr) -> RpcResult<()> {
        let node = NodeEntry {
            id: node_id,
            endpoint: NodeEndpoint {
                address,
                udp_port: address.port(),
            },
        };
        info!("RPC Request: drop_peer({:?})", node.clone());
        match self.network.drop_peer(node) {
            Ok(_) => Ok(()),
            Err(_) => Err(RpcError::internal_error()),
        }
    }

    fn get_best_block_hash(&self) -> RpcResult<H256> {
        info!("RPC Request: get_best_block_hash()");
        Ok(self.consensus.best_block_hash())
    }

    fn get_block_count(&self) -> RpcResult<u64> {
        info!("RPC Request: get_block_count()");
        Ok(self.consensus.block_count())
    }

    fn get_goodput(&self) -> RpcResult<isize> {
        info!("RPC Request: get_goodput");
        let mut set = HashSet::new();
        let mut min = std::u64::MAX;
        let mut max: u64 = 0;
        for key in self.consensus.inner.read().hash_to_arena_indices.keys() {
            if let Some(block) =
                self.consensus.data_man.block_by_hash(key, false)
            {
                let timestamp = block.block_header.timestamp();
                if timestamp < min && timestamp > 0 {
                    min = timestamp;
                }
                if timestamp > max {
                    max = timestamp;
                }
                for transaction in &block.transactions {
                    set.insert(transaction.hash());
                }
            }
        }
        if max != min {
            Ok(set.len() as isize / (max - min) as isize)
        } else {
            Ok(-1)
        }
    }

    fn get_nodeid(&self, challenge: Vec<u8>) -> RpcResult<Vec<u8>> {
        match self.network.sign_challenge(challenge) {
            Ok(r) => Ok(r),
            Err(_) => Err(RpcError::internal_error()),
        }
    }

    fn get_peer_info(&self) -> RpcResult<Vec<PeerInfo>> {
        info!("RPC Request: get_peer_info");
        match self.network.get_peer_info() {
            None => Ok(Vec::new()),
            Some(peers) => Ok(peers),
        }
    }

    fn get_status(&self) -> RpcResult<Status> {
        let best_hash = self.consensus.best_block_hash();
        let block_number = self.consensus.block_count();
        let tx_count = self.tx_pool.total_unpacked();
        if let Some(epoch_number) =
            self.consensus.get_block_epoch_number(&best_hash)
        {
            Ok(Status {
                best_hash: RpcH256::from(best_hash),
                epoch_number,
                block_number,
                pending_tx_number: tx_count,
            })
        } else {
            Err(RpcError::internal_error())
        }
    }

    /// The first element is true if the tx is executed in a confirmed block.
    /// The second element indicate the execution result (standin
    /// for receipt)
    fn get_transaction_receipt(
        &self, tx_hash: H256,
    ) -> RpcResult<Option<Receipt>> {
        let maybe_receipt = self
            .consensus
            .get_transaction_info_by_hash(&tx_hash)
            .map(|(tx, receipt, address)| Receipt::new(tx, receipt, address));
        Ok(maybe_receipt)
    }

    fn say_hello(&self) -> RpcResult<String> { Ok("Hello, world".into()) }

    fn stop(&self) -> RpcResult<()> {
        *self.exit.0.lock() = true;
        self.exit.1.notify_all();

        Ok(())
    }
}
