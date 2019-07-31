// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use jsonrpc_core::Result as RpcResult;
use std::{collections::BTreeMap, sync::Arc};

use cfx_types::H256 as CfxH256;
use cfxcore::TransactionPool;
use primitives::{Action, SignedTransaction};

use network::{
    get_high_priority_packets,
    node_table::{Node, NodeId},
    throttling::{self, THROTTLING_SERVICE},
    NetworkService, SessionDetails,
};

use crate::rpc::{
    traits::cfx::debug::DebugRpc,
    types::{Transaction, H256},
};

fn grouped_txs<T, F>(
    txs: Vec<Arc<SignedTransaction>>, converter: F,
) -> BTreeMap<String, BTreeMap<usize, Vec<T>>>
where F: Fn(Arc<SignedTransaction>) -> T {
    let mut addr_grouped_txs: BTreeMap<String, BTreeMap<usize, Vec<T>>> =
        BTreeMap::new();

    for tx in txs {
        let addr = format!("{:?}", tx.sender());
        let addr_entry: &mut BTreeMap<usize, Vec<T>> =
            addr_grouped_txs.entry(addr).or_insert(BTreeMap::new());

        let nonce = tx.nonce().as_usize();
        let nonce_entry: &mut Vec<T> =
            addr_entry.entry(nonce).or_insert(Vec::new());

        nonce_entry.push(converter(tx));
    }

    addr_grouped_txs
}

#[derive(Clone)]
pub struct DebugHandler {
    network: Arc<NetworkService>,
    tx_pool: Arc<TransactionPool>,
}

impl DebugHandler {
    pub fn new(
        network: Arc<NetworkService>, tx_pool: Arc<TransactionPool>,
    ) -> Self {
        DebugHandler { network, tx_pool }
    }
}

impl DebugRpc for DebugHandler {
    fn clear_tx_pool(&self) -> RpcResult<()> {
        self.tx_pool.clear_tx_pool();
        Ok(())
    }

    fn net_high_priority_packets(&self) -> RpcResult<usize> {
        Ok(get_high_priority_packets())
    }

    fn net_node(&self, id: NodeId) -> RpcResult<Option<(String, Node)>> {
        match self.network.get_node(&id) {
            None => Ok(None),
            Some((trusted, node)) => {
                if trusted {
                    Ok(Some(("trusted".into(), node)))
                } else {
                    Ok(Some(("untrusted".into(), node)))
                }
            }
        }
    }

    fn net_sessions(
        &self, node_id: Option<NodeId>,
    ) -> RpcResult<Vec<SessionDetails>> {
        match self.network.get_detailed_sessions(node_id.into()) {
            None => Ok(Vec::new()),
            Some(sessions) => Ok(sessions),
        }
    }

    fn net_throttling(&self) -> RpcResult<throttling::Service> {
        Ok(THROTTLING_SERVICE.read().clone())
    }

    fn tx_inspect(&self, hash: H256) -> RpcResult<BTreeMap<String, String>> {
        let mut ret: BTreeMap<String, String> = BTreeMap::new();
        let hash: CfxH256 = hash.into();
        if let Some(tx) = self.tx_pool.get_transaction(&hash) {
            ret.insert("exist".into(), "true".into());
            if self.tx_pool.check_tx_packed_in_deferred_pool(&hash) {
                ret.insert("packed".into(), "true".into());
            } else {
                ret.insert("packed".into(), "false".into());
            }
            let (local_nonce, local_balance) =
                self.tx_pool.get_local_account_info(&tx.sender());
            let (state_nonce, state_balance) =
                self.tx_pool.get_state_account_info(&tx.sender());
            ret.insert(
                "local nonce".into(),
                serde_json::to_string(&local_nonce).unwrap(),
            );
            ret.insert(
                "local balance".into(),
                serde_json::to_string(&local_balance).unwrap(),
            );
            ret.insert(
                "state nonce".into(),
                serde_json::to_string(&state_nonce).unwrap(),
            );
            ret.insert(
                "state balance".into(),
                serde_json::to_string(&state_balance).unwrap(),
            );
        } else {
            ret.insert("exist".into(), "false".into());
        }
        Ok(ret)
    }

    fn txpool_content(
        &self,
    ) -> RpcResult<
        BTreeMap<String, BTreeMap<String, BTreeMap<usize, Vec<Transaction>>>>,
    > {
        let (ready_txs, deferred_txs) = self.tx_pool.content();
        let converter = |tx: Arc<SignedTransaction>| -> Transaction {
            Transaction::from_signed(&tx, None)
        };

        let mut ret: BTreeMap<
            String,
            BTreeMap<String, BTreeMap<usize, Vec<Transaction>>>,
        > = BTreeMap::new();
        ret.insert("ready".into(), grouped_txs(ready_txs, converter));
        ret.insert("deferred".into(), grouped_txs(deferred_txs, converter));

        Ok(ret)
    }

    fn txpool_inspect(
        &self,
    ) -> RpcResult<
        BTreeMap<String, BTreeMap<String, BTreeMap<usize, Vec<String>>>>,
    > {
        let (ready_txs, deferred_txs) = self.tx_pool.content();
        let converter = |tx: Arc<SignedTransaction>| -> String {
            let to = match tx.action {
                Action::Create => "<Create contract>".into(),
                Action::Call(addr) => format!("{:?}", addr),
            };

            format!(
                "{}: {:?} wei + {:?} gas * {:?} wei",
                to, tx.value, tx.gas, tx.gas_price
            )
        };

        let mut ret: BTreeMap<
            String,
            BTreeMap<String, BTreeMap<usize, Vec<String>>>,
        > = BTreeMap::new();
        ret.insert("ready".into(), grouped_txs(ready_txs, converter));
        ret.insert("deferred".into(), grouped_txs(deferred_txs, converter));

        Ok(ret)
    }

    fn txpool_status(&self) -> RpcResult<BTreeMap<String, usize>> {
        let (ready_len, deferred_len, received_len, unexecuted_len) =
            self.tx_pool.stats();

        let mut ret: BTreeMap<String, usize> = BTreeMap::new();
        ret.insert("ready".into(), ready_len);
        ret.insert("deferred".into(), deferred_len);
        ret.insert("received".into(), received_len);
        ret.insert("unexecuted".into(), unexecuted_len);

        Ok(ret)
    }
}
