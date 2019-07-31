// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

#![allow(unused)]

use crate::rpc::{
    traits::cfx::generate::GenerateRpc,
    types::{BlameInfo, Bytes},
};
use cfx_types::H256;
use jsonrpc_core::{Error as RpcError, Result as RpcResult};

#[derive(Clone)]
pub struct GenerateHandler {}

impl GenerateHandler {
    pub fn new() -> Self { GenerateHandler {} }
}

impl GenerateRpc for GenerateHandler {
    fn generate_block_with_blame_info(
        &self, num_txs: usize, block_size_limit: usize, blame_info: BlameInfo,
    ) -> RpcResult<H256> {
        Err(RpcError::method_not_found())
    }

    fn generate_block_with_fake_txs(
        &self, raw_txs_without_data: Bytes, tx_data_len: Option<usize>,
    ) -> RpcResult<H256> {
        Err(RpcError::method_not_found())
    }

    fn generate_custom_block(
        &self, parent_hash: H256, referee: Vec<H256>, raw_txs: Bytes,
        adaptive: Option<bool>,
    ) -> RpcResult<H256>
    {
        Err(RpcError::method_not_found())
    }

    fn generate_fixed_block(
        &self, parent_hash: H256, referee: Vec<H256>, num_txs: usize,
        adaptive: bool, difficulty: Option<u64>,
    ) -> RpcResult<H256>
    {
        Err(RpcError::method_not_found())
    }

    fn generate_one_block_special(
        &self, num_txs: usize, mut block_size_limit: usize,
        num_txs_simple: usize, num_txs_erc20: usize,
    ) -> RpcResult<()>
    {
        Err(RpcError::method_not_found())
    }

    fn generate_one_block(
        &self, num_txs: usize, block_size_limit: usize,
    ) -> RpcResult<H256> {
        Err(RpcError::method_not_found())
    }

    fn generate(
        &self, num_blocks: usize, num_txs: usize,
    ) -> RpcResult<Vec<H256>> {
        Err(RpcError::method_not_found())
    }
}
