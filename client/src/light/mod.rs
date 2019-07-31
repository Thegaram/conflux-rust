// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use std::{
    any::Any,
    sync::{Arc, Weak},
    thread,
    time::{Duration, Instant},
};

use ctrlc::CtrlC;
use db::SystemDB;
use parking_lot::{Condvar, Mutex};
use threadpool::ThreadPool;

use cfx_types::U256;
use network::NetworkService;
use secret_store::SecretStore;

use cfxcore::{
    block_data_manager::BlockDataManager, genesis,
    pow::WORKER_COMPUTATION_PARALLELISM, statistics::Statistics,
    storage::StorageManager, sync::SyncPhaseType,
    transaction_pool::DEFAULT_MAX_BLOCK_GAS_LIMIT, vm_factory::VmFactory,
    ConsensusGraph, QueryService as LightQueryService, SynchronizationGraph,
    SynchronizationService, TransactionPool,
};

use crate::{
    configuration::Configuration,
    rpc::{
        impls::light::Rpc as LightNodeRpc, setup_debug_rpc_apis_light,
        setup_public_rpc_apis_light,
    },
};

use super::{
    http::Server as HttpServer, tcp::Server as TcpServer, TESTNET_VERSION,
};

pub struct LightClientHandle {
    pub debug_rpc_http_server: Option<HttpServer>,
    pub rpc_tcp_server: Option<TcpServer>,
    pub rpc_http_server: Option<HttpServer>,
    pub consensus: Arc<ConsensusGraph>,
    pub txpool: Arc<TransactionPool>,
    pub secret_store: Arc<SecretStore>,
    pub ledger_db: Weak<SystemDB>,
}

impl LightClientHandle {
    pub fn into_be_dropped(self) -> (Weak<SystemDB>, Box<Any>) {
        (
            self.ledger_db,
            Box::new((
                self.consensus,
                self.debug_rpc_http_server,
                self.rpc_tcp_server,
                self.rpc_http_server,
                self.txpool,
                self.secret_store,
            )),
        )
    }
}

pub struct LightClient {}

impl LightClient {
    // Start all key components of Conflux and pass out their handles
    pub fn start(
        conf: Configuration, exit: Arc<(Mutex<bool>, Condvar)>,
    ) -> Result<LightClientHandle, String> {
        info!("Working directory: {:?}", std::env::current_dir());

        if conf.raw_conf.metrics_enabled {
            metrics::enable();
            let reporter = metrics::FileReporter::new(
                conf.raw_conf.metrics_output_file.clone(),
            );
            metrics::report_async(
                reporter,
                Duration::from_millis(conf.raw_conf.metrics_report_interval_ms),
            );
        }

        let worker_thread_pool = Arc::new(Mutex::new(ThreadPool::with_name(
            "Tx Recover".into(),
            WORKER_COMPUTATION_PARALLELISM,
        )));

        let network_config = conf.net_config()?;
        let cache_config = conf.cache_config();

        let db_config = conf.db_config();
        let ledger_db = db::open_database(
            conf.raw_conf.db_dir.as_ref().unwrap(),
            &db_config,
        )
        .map_err(|e| format!("Failed to open database {:?}", e))?;

        let secret_store = Arc::new(SecretStore::new());
        let storage_manager = Arc::new(StorageManager::new(
            ledger_db.clone(),
            conf.storage_config(),
        ));
        {
            let storage_manager_log_weak_ptr = Arc::downgrade(&storage_manager);
            let exit_clone = exit.clone();
            thread::spawn(move || loop {
                let mut exit_lock = exit_clone.0.lock();
                if exit_clone
                    .1
                    .wait_for(&mut exit_lock, Duration::from_millis(5000))
                    .timed_out()
                {
                    let manager = storage_manager_log_weak_ptr.upgrade();
                    match manager {
                        None => return,
                        Some(manager) => manager.log_usage(),
                    };
                } else {
                    return;
                }
            });
        }

        let genesis_accounts = if conf.raw_conf.test_mode {
            match conf.raw_conf.genesis_accounts {
                Some(ref file) => genesis::load_file(file)?,
                None => genesis::default(secret_store.as_ref()),
            }
        } else {
            genesis::default(secret_store.as_ref())
        };

        // FIXME: move genesis block to a dedicated directory near all conflux
        // FIXME: parameters.
        let genesis_block = storage_manager.initialize(
            genesis_accounts,
            DEFAULT_MAX_BLOCK_GAS_LIMIT.into(),
            TESTNET_VERSION.into(),
            U256::zero(),
        );
        debug!("Initialize genesis_block={:?}", genesis_block);

        let data_man = Arc::new(BlockDataManager::new(
            cache_config,
            Arc::new(genesis_block),
            ledger_db.clone(),
            storage_manager,
            worker_thread_pool,
            conf.data_mananger_config(),
        ));

        let txpool =
            Arc::new(TransactionPool::with_capacity(0, data_man.clone()));

        let statistics = Arc::new(Statistics::new());

        let vm = VmFactory::new(1024 * 32);
        let pow_config = conf.pow_config();
        let consensus = Arc::new(ConsensusGraph::new(
            conf.consensus_config(),
            vm.clone(),
            txpool.clone(),
            statistics.clone(),
            data_man.clone(),
            pow_config.clone(),
        ));

        let protocol_config = conf.protocol_config();
        let verification_config = conf.verification_config();

        let sync_graph = Arc::new(SynchronizationGraph::new(
            consensus.clone(),
            verification_config,
            pow_config,
            true,
        ));

        let network = {
            let mut network = NetworkService::new(network_config);
            network.start().unwrap();
            Arc::new(network)
        };

        // TODO(thegaram): make sure light nodes do header-only sync
        let initial_sync_phase = SyncPhaseType::CatchUpRecoverBlockHeaderFromDB;
        let sync = Arc::new(SynchronizationService::new(
            true,
            network.clone(),
            sync_graph.clone(),
            protocol_config,
            initial_sync_phase,
        ));
        sync.register().unwrap();

        let query_service = Arc::new(LightQueryService::new(
            consensus.clone(),
            network.clone(),
        ));
        query_service.register().unwrap();

        let rpc = Arc::new(LightNodeRpc::new(
            consensus.clone(),
            exit,
            network.clone(),
            query_service,
            txpool.clone(),
        ));

        let debug_rpc_http_server = super::rpc::new_http(
            super::rpc::HttpConfiguration::new(
                Some((127, 0, 0, 1)),
                conf.raw_conf.jsonrpc_local_http_port,
                conf.raw_conf.jsonrpc_cors.clone(),
                conf.raw_conf.jsonrpc_http_keep_alive,
            ),
            setup_debug_rpc_apis_light(rpc.clone()),
        )?;

        let rpc_tcp_server = super::rpc::new_tcp(
            super::rpc::TcpConfiguration::new(
                None,
                conf.raw_conf.jsonrpc_tcp_port,
            ),
            if conf.raw_conf.test_mode {
                setup_debug_rpc_apis_light(rpc.clone())
            } else {
                setup_public_rpc_apis_light(rpc.clone())
            },
        )?;

        let rpc_http_server = super::rpc::new_http(
            super::rpc::HttpConfiguration::new(
                None,
                conf.raw_conf.jsonrpc_http_port,
                conf.raw_conf.jsonrpc_cors.clone(),
                conf.raw_conf.jsonrpc_http_keep_alive,
            ),
            if conf.raw_conf.test_mode {
                setup_debug_rpc_apis_light(rpc.clone())
            } else {
                setup_public_rpc_apis_light(rpc.clone())
            },
        )?;

        Ok(LightClientHandle {
            ledger_db: Arc::downgrade(&ledger_db),
            debug_rpc_http_server,
            rpc_http_server,
            rpc_tcp_server,
            txpool,
            consensus,
            secret_store,
        })
    }

    /// Use a Weak pointer to ensure that other Arc pointers are released
    fn wait_for_drop<T>(w: Weak<T>) {
        let sleep_duration = Duration::from_secs(1);
        let warn_timeout = Duration::from_secs(10);
        let max_timeout = Duration::from_secs(60);
        let instant = Instant::now();
        let mut warned = false;
        while instant.elapsed() < max_timeout {
            if w.upgrade().is_none() {
                return;
            }
            if !warned && instant.elapsed() > warn_timeout {
                warned = true;
                warn!("Shutdown is taking longer than expected.");
            }
            thread::sleep(sleep_duration);
        }
        warn!("Shutdown timeout reached, exiting uncleanly.");
    }

    pub fn close(handle: LightClientHandle) {
        let (ledger_db, to_drop) = handle.into_be_dropped();
        drop(to_drop);

        // Make sure ledger_db is properly dropped, so rocksdb can be closed
        // cleanly
        LightClient::wait_for_drop(ledger_db);
    }

    pub fn run_until_closed(
        exit: Arc<(Mutex<bool>, Condvar)>, keep_alive: LightClientHandle,
    ) {
        CtrlC::set_handler({
            let e = exit.clone();
            move || {
                *e.0.lock() = true;
                e.1.notify_all();
            }
        });

        let mut lock = exit.0.lock();
        if !*lock {
            let _ = exit.1.wait(&mut lock);
        }

        LightClient::close(keep_alive);
    }
}
