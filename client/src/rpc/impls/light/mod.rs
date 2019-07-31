// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

mod cfx;
mod generate;

use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

use cfxcore::{ConsensusGraph, QueryService, TransactionPool};
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
        consensus: Arc<ConsensusGraph>, exit: Arc<(Mutex<bool>, Condvar)>,
        network: Arc<NetworkService>, query_service: Arc<QueryService>,
        tx_pool: Arc<TransactionPool>,
    ) -> Self
    {
        Rpc {
            cfx: CfxHandler::new(consensus.clone(), query_service),
            debug: DebugHandler::new(network.clone(), tx_pool.clone()),
            generate: GenerateHandler::new(),
            test: TestHandler::new(consensus, exit, network, tx_pool),
        }
    }
}
