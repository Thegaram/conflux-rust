// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use super::super::super::types::{BlameInfo, Bytes};
use cfx_types::H256;
use jsonrpc_core::Result as RpcResult;
use jsonrpc_derive::rpc;

#[rpc]
pub trait GenerateRpc {
    #[rpc(name = "test_generateblockwithblameinfo")]
    fn generate_block_with_blame_info(
        &self, num_txs: usize, block_size_limit: usize, blame_info: BlameInfo,
    ) -> RpcResult<H256>;

    #[rpc(name = "test_generateblockwithfaketxs")]
    fn generate_block_with_fake_txs(
        &self, raw: Bytes, tx_data_len: Option<usize>,
    ) -> RpcResult<H256>;

    #[rpc(name = "test_generatecustomblock")]
    fn generate_custom_block(
        &self, parent: H256, referees: Vec<H256>, raw: Bytes,
        adaptive: Option<bool>,
    ) -> RpcResult<H256>;

    #[rpc(name = "generatefixedblock")]
    fn generate_fixed_block(
        &self, parent_hash: H256, referee: Vec<H256>, num_txs: usize,
        adaptive: bool, difficulty: Option<u64>,
    ) -> RpcResult<H256>;

    #[rpc(name = "generateoneblockspecial")]
    fn generate_one_block_special(
        &self, num_txs: usize, block_size_limit: usize, num_txs_simple: usize,
        num_txs_erc20: usize,
    ) -> RpcResult<()>;

    #[rpc(name = "generateoneblock")]
    fn generate_one_block(
        &self, num_txs: usize, block_size_limit: usize,
    ) -> RpcResult<H256>;

    #[rpc(name = "generate")]
    fn generate(
        &self, num_blocks: usize, num_txs: usize,
    ) -> RpcResult<Vec<H256>>;
}
