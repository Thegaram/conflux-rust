// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use jsonrpc_core::{Error as RpcError, Result as RpcResult};
use rlp::Rlp;
use std::sync::Arc;

use blockgen::BlockGenerator;
use cfx_types::H256;

use primitives::{
    block::MAX_BLOCK_SIZE_IN_BYTES, SignedTransaction, TransactionWithSignature,
};

use crate::rpc::{
    traits::cfx::generate::GenerateRpc,
    types::{BlameInfo, Bytes},
};

fn decode_raw_txs(
    raw_txs: Bytes, tx_data_len: usize,
) -> RpcResult<Vec<Arc<SignedTransaction>>> {
    let txs: Vec<TransactionWithSignature> =
        Rlp::new(&raw_txs.into_vec()).as_list().map_err(|err| {
            RpcError::invalid_params(format!("Decode error: {:?}", err))
        })?;

    let mut transactions = Vec::new();

    for tx in txs {
        match tx.recover_public() {
            Ok(public) => {
                let mut signed_tx = SignedTransaction::new(public, tx);
                if tx_data_len > 0 {
                    signed_tx.transaction.unsigned.data = vec![0; tx_data_len];
                }
                transactions.push(Arc::new(signed_tx));
            }
            Err(e) => {
                return Err(RpcError::invalid_params(format!(
                    "Recover public error: {:?}",
                    e
                )));
            }
        }
    }

    Ok(transactions)
}

#[derive(Clone)]
pub struct GenerateHandler {
    block_gen: Arc<BlockGenerator>,
}

impl GenerateHandler {
    pub fn new(block_gen: Arc<BlockGenerator>) -> Self {
        GenerateHandler { block_gen }
    }
}

impl GenerateRpc for GenerateHandler {
    fn generate_block_with_blame_info(
        &self, num_txs: usize, block_size_limit: usize, blame_info: BlameInfo,
    ) -> RpcResult<H256> {
        Ok(self.block_gen.generate_block_with_blame_info(
            num_txs,
            block_size_limit,
            vec![],
            blame_info.blame,
            blame_info.deferred_state_root.and_then(|x| Some(x.into())),
            blame_info
                .deferred_receipts_root
                .and_then(|x| Some(x.into())),
            blame_info
                .deferred_logs_bloom_hash
                .and_then(|x| Some(x.into())),
        ))
    }

    fn generate_block_with_fake_txs(
        &self, raw_txs_without_data: Bytes, tx_data_len: Option<usize>,
    ) -> RpcResult<H256> {
        let transactions =
            decode_raw_txs(raw_txs_without_data, tx_data_len.unwrap_or(0))?;
        Ok(self.block_gen.generate_custom_block(transactions))
    }

    fn generate_custom_block(
        &self, parent_hash: H256, referee: Vec<H256>, raw_txs: Bytes,
        adaptive: Option<bool>,
    ) -> RpcResult<H256>
    {
        info!("RPC Request: generate_custom_block()");

        let transactions = decode_raw_txs(raw_txs, 0)?;

        match self.block_gen.generate_custom_block_with_parent(
            parent_hash,
            referee,
            transactions,
            adaptive.unwrap_or(false),
        ) {
            Ok(hash) => Ok(hash),
            Err(e) => Err(RpcError::invalid_params(e)),
        }
    }

    fn generate_fixed_block(
        &self, parent_hash: H256, referee: Vec<H256>, num_txs: usize,
        adaptive: bool, difficulty: Option<u64>,
    ) -> RpcResult<H256>
    {
        info!(
            "RPC Request: generate_fixed_block({:?}, {:?}, {:?}, {:?})",
            parent_hash, referee, num_txs, difficulty
        );
        match self.block_gen.generate_fixed_block(
            parent_hash,
            referee,
            num_txs,
            difficulty.unwrap_or(0),
            adaptive,
        ) {
            Ok(hash) => Ok(hash),
            Err(e) => Err(RpcError::invalid_params(e)),
        }
    }

    fn generate_one_block_special(
        &self, num_txs: usize, mut block_size_limit: usize,
        num_txs_simple: usize, num_txs_erc20: usize,
    ) -> RpcResult<()>
    {
        info!("RPC Request: generate_one_block_special()");

        let block_gen = &self.block_gen;
        let special_transactions = block_gen.generate_special_transactions(
            &mut block_size_limit,
            num_txs_simple,
            num_txs_erc20,
        );

        block_gen.generate_block(
            num_txs,
            block_size_limit,
            special_transactions,
        );

        Ok(())
    }

    fn generate_one_block(
        &self, num_txs: usize, block_size_limit: usize,
    ) -> RpcResult<H256> {
        info!("RPC Request: generate_one_block()");
        let hash =
            self.block_gen
                .generate_block(num_txs, block_size_limit, vec![]);
        Ok(hash)
    }

    fn generate(
        &self, num_blocks: usize, num_txs: usize,
    ) -> RpcResult<Vec<H256>> {
        info!("RPC Request: generate({:?})", num_blocks);
        let mut hashes = Vec::new();
        for _i in 0..num_blocks {
            hashes.push(self.block_gen.generate_block_with_transactions(
                num_txs,
                MAX_BLOCK_SIZE_IN_BYTES,
            ));
        }
        Ok(hashes)
    }
}
