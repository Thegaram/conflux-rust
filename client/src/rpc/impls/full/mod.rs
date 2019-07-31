// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

mod cfx;
mod generate;

use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

use blockgen::BlockGenerator;
use cfxcore::{ConsensusGraph, SynchronizationService, TransactionPool};
use network::NetworkService;

pub use super::common::{DebugHandler, TestHandler};
pub use cfx::CfxHandler;
pub use generate::GenerateHandler;

pub struct Rpc {
    pub cfx: CfxHandler,
    pub debug: DebugHandler,
    pub generate: GenerateHandler,
    pub test: TestHandler,
}

impl Rpc {
    pub fn new(
        consensus: Arc<ConsensusGraph>, block_gen: Arc<BlockGenerator>,
        exit: Arc<(Mutex<bool>, Condvar)>, network: Arc<NetworkService>,
        sync: Arc<SynchronizationService>, tx_pool: Arc<TransactionPool>,
    ) -> Self
    {
        Rpc {
            cfx: CfxHandler::new(consensus.clone(), sync, tx_pool.clone()),
            debug: DebugHandler::new(network.clone(), tx_pool.clone()),
            generate: GenerateHandler::new(block_gen),
            test: TestHandler::new(consensus, exit, network, tx_pool),
        }
    }
}
