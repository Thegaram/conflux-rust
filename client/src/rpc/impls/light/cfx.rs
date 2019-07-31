// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

#![allow(unused)]

use jsonrpc_core::{Error as RpcError, Result as RpcResult};
use std::sync::Arc;

use cfx_types::H160 as CfxH160;
use cfxcore::{ConsensusGraph, QueryService};

use crate::rpc::{
    traits::cfx::public::Cfx,
    types::{
        Block, Bytes, EpochNumber, Filter, Log, Transaction, H160, H256, U256,
        U64,
    },
};

#[derive(Clone)]
pub struct CfxHandler {
    consensus: Arc<ConsensusGraph>,
    query_service: Arc<QueryService>,
}

impl CfxHandler {
    pub fn new(
        consensus: Arc<ConsensusGraph>, query_service: Arc<QueryService>,
    ) -> Self {
        CfxHandler {
            consensus,
            query_service,
        }
    }
}

impl Cfx for CfxHandler {
    fn balance(
        &self, address: H160, num: Option<EpochNumber>,
    ) -> RpcResult<U256> {
        let num = num.unwrap_or(EpochNumber::LatestState);
        let address: CfxH160 = address.into();
        info!(
            "RPC Request: cfx_getBalance address={:?} epoch_num={:?}",
            address, num
        );

        let epoch = self
            .consensus
            .get_height_from_epoch_number(num.into())
            .map_err(|err| RpcError::invalid_params(err))?;

        let balance = self
            .query_service
            .get_account(epoch, address)
            .map(|account| account.balance.into())
            .unwrap_or_default();

        Ok(balance)
    }

    fn best_block_hash(&self) -> RpcResult<H256> { unimplemented!() }

    fn block_by_epoch_number(
        &self, epoch_num: EpochNumber, include_txs: bool,
    ) -> RpcResult<Block> {
        unimplemented!()
    }

    fn block_by_hash_with_pivot_assumption(
        &self, block_hash: H256, pivot_hash: H256, epoch_number: U64,
    ) -> RpcResult<Block> {
        unimplemented!()
    }

    fn block_by_hash(
        &self, hash: H256, include_txs: bool,
    ) -> RpcResult<Option<Block>> {
        unimplemented!()
    }

    fn blocks_by_epoch(&self, num: EpochNumber) -> RpcResult<Vec<H256>> {
        unimplemented!()
    }

    fn call(
        &self, rpc_tx: Transaction, epoch: Option<EpochNumber>,
    ) -> RpcResult<Bytes> {
        unimplemented!()
    }

    fn epoch_number(&self, epoch_num: Option<EpochNumber>) -> RpcResult<U256> {
        unimplemented!()
    }

    fn estimate_gas(&self, rpc_tx: Transaction) -> RpcResult<U256> {
        unimplemented!()
    }

    fn gas_price(&self) -> RpcResult<U256> { unimplemented!() }

    fn get_logs(&self, filter: Filter) -> RpcResult<Vec<Log>> {
        unimplemented!()
    }

    fn send_raw_transaction(&self, raw: Bytes) -> RpcResult<H256> {
        unimplemented!()
    }

    fn transaction_by_hash(
        &self, hash: H256,
    ) -> RpcResult<Option<Transaction>> {
        unimplemented!()
    }

    fn transaction_count(
        &self, address: H160, num: Option<EpochNumber>,
    ) -> RpcResult<U256> {
        unimplemented!()
    }
}
