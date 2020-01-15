// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use super::{
    consensus_types::{block::Block, executed_block::ExecutedBlock},
    counters,
    state_replication::StateComputer,
};
use anyhow::{ensure, Result};
use cfx_types::H256;
use libra_logger::prelude::*;
use libra_types::{
    account_config,
    crypto_proxies::{
        LedgerInfoWithSignatures, ValidatorChangeProof, ValidatorSet,
    },
    transaction::{SignedTransaction, Transaction},
};
//use state_synchronizer::StateSyncClient;
use super::super::executor::{Executor, ProcessedVMOutput};
use libra_types::event::EventKey;
use serde::{Deserialize, Serialize};
use std::{
    convert::TryFrom,
    sync::Arc,
    time::{Duration, Instant},
};
//use vm_runtime::LibraVM;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PivotBlockDecision {
    height: u64,
    block_hash: H256,
}

impl PivotBlockDecision {
    pub fn pivot_select_event_key() -> EventKey {
        EventKey::new_from_address(
            &account_config::pivot_chain_select_address(),
            2,
        )
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        lcs::from_bytes(bytes).map_err(Into::into)
    }
}

/// Basic communication with the Execution module;
/// implements StateComputer traits.
pub struct ExecutionProxy {
    executor: Arc<Executor>,
    //synchronizer: Arc<StateSyncClient>,
    last_pivot_height: u64,
}

impl ExecutionProxy {
    pub fn new(
        executor: Arc<Executor>, /* , synchronizer: Arc<StateSyncClient> */
    ) -> Self {
        Self {
            executor,
            //synchronizer,
            // FIXME: initialize last_pivot_height properly.
            last_pivot_height: 0,
        }
    }

    fn transactions_from_block(
        block: &Block<Vec<SignedTransaction>>,
    ) -> Vec<Transaction> {
        let mut transactions = vec![Transaction::BlockMetadata(block.into())];
        transactions.extend(
            block
                .payload()
                .unwrap_or(&vec![])
                .iter()
                .map(|txn| Transaction::UserTransaction(txn.clone())),
        );
        transactions
    }
}

#[async_trait::async_trait]
impl StateComputer for ExecutionProxy {
    type Payload = Vec<SignedTransaction>;

    fn compute(
        &self,
        // The block to be executed.
        block: &Block<Self::Payload>,
    ) -> Result<ProcessedVMOutput>
    {
        //let pre_execution_instant = Instant::now();
        // TODO: figure out error handling for the prologue txn
        self.executor
            .execute_block(
                Self::transactions_from_block(block),
                //parent_executed_trees,
                //committed_trees,
                block.parent_id(),
                block.id(),
            )
            .and_then(|output| {
                if let Some(p) = output.pivot_block.as_ref() {}
                Ok(output)
            })
    }

    /// Send a successful commit. A future is fulfilled when the state is
    /// finalized.
    async fn commit(
        &self,
        blocks: Vec<&ExecutedBlock<Self::Payload>>,
        finality_proof: LedgerInfoWithSignatures,
        //committed_trees: &ExecutedTrees,
    ) -> Result<()>
    {
        let version = finality_proof.ledger_info().version();
        counters::LAST_COMMITTED_VERSION.set(version as i64);

        let pre_commit_instant = Instant::now();

        let committable_blocks = blocks
            .into_iter()
            .map(|executed_block| {
                (
                    Self::transactions_from_block(executed_block.block()),
                    Arc::clone(executed_block.output()),
                )
            })
            .collect();

        self.executor
            .commit_blocks(committable_blocks, finality_proof)?;
        counters::BLOCK_COMMIT_DURATION_S
            .observe_duration(pre_commit_instant.elapsed());
        /*
        if let Err(e) = self.synchronizer.commit().await {
            error!("failed to notify state synchronizer: {:?}", e);
        }
        */
        Ok(())
    }

    /// Synchronize to a commit that not present locally.
    async fn sync_to(&self, target: LedgerInfoWithSignatures) -> Result<()> {
        /*
        counters::STATE_SYNC_COUNT.inc();
        self.synchronizer.sync_to(target).await
        */
        Ok(())
    }

    async fn get_epoch_proof(
        &self, start_epoch: u64, end_epoch: u64,
    ) -> Result<ValidatorChangeProof> {
        /*
        self.synchronizer
            .get_epoch_proof(start_epoch, end_epoch)
            .await
            */
        Ok(ValidatorChangeProof::new(Vec::new(), false))
    }
}
