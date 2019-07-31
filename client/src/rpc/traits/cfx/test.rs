// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use super::super::super::types::{
    Block, Receipt as RpcReceipt, Status as RpcStatus,
};
use cfx_types::H256;
use cfxcore::PeerInfo;
use jsonrpc_core::Result as RpcResult;
use jsonrpc_derive::rpc;
use network::node_table::NodeId;
use std::net::SocketAddr;

#[rpc]
pub trait TestRpc {
    #[rpc(name = "sayhello")]
    fn say_hello(&self) -> RpcResult<String>;

    #[rpc(name = "getbestblockhash")]
    fn get_best_block_hash(&self) -> RpcResult<H256>;

    #[rpc(name = "getblockcount")]
    fn get_block_count(&self) -> RpcResult<u64>;

    #[rpc(name = "getgoodput")]
    fn get_goodput(&self) -> RpcResult<isize>;

    #[rpc(name = "addnode")]
    fn add_peer(&self, id: NodeId, addr: SocketAddr) -> RpcResult<()>;

    #[rpc(name = "removenode")]
    fn drop_peer(&self, id: NodeId, addr: SocketAddr) -> RpcResult<()>;

    #[rpc(name = "getpeerinfo")]
    fn get_peer_info(&self) -> RpcResult<Vec<PeerInfo>>;

    /// Returns the JSON of whole chain
    #[rpc(name = "cfx_getChain")]
    fn chain(&self) -> RpcResult<Vec<Block>>;

    #[rpc(name = "stop")]
    fn stop(&self) -> RpcResult<()>;

    #[rpc(name = "getnodeid")]
    fn get_nodeid(&self, challenge: Vec<u8>) -> RpcResult<Vec<u8>>;

    #[rpc(name = "getstatus")]
    fn get_status(&self) -> RpcResult<RpcStatus>;

    #[rpc(name = "addlatency")]
    fn add_latency(&self, id: NodeId, latency_ms: f64) -> RpcResult<()>;

    #[rpc(name = "gettransactionreceipt")]
    fn get_transaction_receipt(
        &self, tx_hash: H256,
    ) -> RpcResult<Option<RpcReceipt>>;
}
