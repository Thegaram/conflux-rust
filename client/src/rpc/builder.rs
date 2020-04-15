// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use jsonrpc_core::MetaIoHandler;
use std::sync::Arc;

use crate::{
    configuration::Configuration,
    rpc::interceptor::{RpcProxy, ThrottleInterceptor},
};

use super::{
    impls::{
        cfx::{CfxHandler, LocalRpcImpl, RpcImpl, TestRpcImpl},
        common::RpcImpl as CommonImpl,
        light::{
            CfxHandler as LightCfxHandler, DebugRpcImpl as LightDebugRpcImpl,
            RpcImpl as LightImpl, TestRpcImpl as LightTestRpcImpl,
        },
        pubsub::PubSubClient,
    },
    metadata::Metadata,
    traits::{cfx::Cfx, debug::LocalRpc, pubsub::PubSub, test::TestRpc},
    RpcImplConfiguration,
};

pub enum Api {
    Cfx(&'static str),
    Local,
    Test,
    PubSub,
}

use blockgen::BlockGenerator;
use cfxcore::{
    ConsensusGraph, LightQueryService, Notifications, SynchronizationService,
    TransactionPool,
};
use network::NetworkService;
use parking_lot::{Condvar, Mutex};
use runtime::Runtime;
use txgen::{DirectTransactionGenerator, TransactionGenerator};

pub trait RpcDependencies {
    fn extend_api(
        &self, conf: &Configuration, handler: &mut MetaIoHandler<Metadata>,
        apis: Vec<Api>,
    );

    fn setup_apis(
        &self, conf: &Configuration, apis: Vec<Api>,
    ) -> MetaIoHandler<Metadata>;
}

pub struct FullDependencies {
    pub blockgen: Arc<BlockGenerator>,
    pub conf: RpcImplConfiguration,
    pub consensus: Arc<ConsensusGraph>,
    pub exit: Arc<(Mutex<bool>, Condvar)>,
    pub maybe_direct_txgen: Option<Arc<Mutex<DirectTransactionGenerator>>>,
    pub maybe_txgen: Option<Arc<TransactionGenerator>>,
    pub network: Arc<NetworkService>,
    pub notifications: Arc<Notifications>,
    pub runtime: Arc<Runtime>,
    pub sync: Arc<SynchronizationService>,
    pub txpool: Arc<TransactionPool>,
}

impl RpcDependencies for FullDependencies {
    fn extend_api(
        &self, conf: &Configuration, handler: &mut MetaIoHandler<Metadata>,
        apis: Vec<Api>,
    )
    {
        let common_impl = Arc::new(CommonImpl::new(
            self.exit.clone(),
            self.consensus.clone(),
            self.network.clone(),
            self.txpool.clone(),
        ));

        let rpc_impl = Arc::new(RpcImpl::new(
            self.consensus.clone(),
            self.sync.clone(),
            self.blockgen.clone(),
            self.txpool.clone(),
            self.maybe_txgen.clone(),
            self.maybe_direct_txgen.clone(),
            self.conf.clone(),
        ));

        for api in apis {
            match api {
                Api::Cfx(throttling_section) => {
                    let api =
                        CfxHandler::new(common_impl.clone(), rpc_impl.clone())
                            .to_delegate();

                    let interceptor = ThrottleInterceptor::new(
                        &conf.raw_conf.throttling_conf,
                        throttling_section,
                    );
                    handler.extend_with(RpcProxy::new(api, interceptor));
                }
                Api::Local => {
                    let api = LocalRpcImpl::new(
                        common_impl.clone(),
                        rpc_impl.clone(),
                    )
                    .to_delegate();

                    handler.extend_with(api);
                }
                Api::Test => {
                    let api =
                        TestRpcImpl::new(common_impl.clone(), rpc_impl.clone())
                            .to_delegate();

                    handler.extend_with(api);
                }
                Api::PubSub => {
                    let api = PubSubClient::new(
                        self.runtime.executor(),
                        self.consensus.clone(),
                        self.notifications.clone(),
                    )
                    .to_delegate();

                    handler.extend_with(api);
                }
            }
        }
    }

    fn setup_apis(
        &self, conf: &Configuration, apis: Vec<Api>,
    ) -> MetaIoHandler<Metadata> {
        let mut handler = MetaIoHandler::default();
        self.extend_api(conf, &mut handler, apis);
        handler
    }
}

pub struct LightDependencies {
    pub consensus: Arc<ConsensusGraph>,
    pub exit: Arc<(Mutex<bool>, Condvar)>,
    pub light: Arc<LightQueryService>,
    pub network: Arc<NetworkService>,
    pub notifications: Arc<Notifications>,
    pub runtime: Arc<Runtime>,
    pub txpool: Arc<TransactionPool>,
}

impl RpcDependencies for LightDependencies {
    fn extend_api(
        &self, conf: &Configuration, handler: &mut MetaIoHandler<Metadata>,
        apis: Vec<Api>,
    )
    {
        let common_impl = Arc::new(CommonImpl::new(
            self.exit.clone(),
            self.consensus.clone(),
            self.network.clone(),
            self.txpool.clone(),
        ));

        let rpc_impl = Arc::new(LightImpl::new(self.light.clone()));

        for api in apis {
            match api {
                Api::Cfx(throttling_section) => {
                    let api = LightCfxHandler::new(
                        common_impl.clone(),
                        rpc_impl.clone(),
                    )
                    .to_delegate();

                    let interceptor = ThrottleInterceptor::new(
                        &conf.raw_conf.throttling_conf,
                        throttling_section,
                    );
                    handler.extend_with(RpcProxy::new(api, interceptor));
                }
                Api::Local => {
                    let api = LightDebugRpcImpl::new(
                        common_impl.clone(),
                        rpc_impl.clone(),
                    )
                    .to_delegate();

                    handler.extend_with(api);
                }
                Api::Test => {
                    let api = LightTestRpcImpl::new(
                        common_impl.clone(),
                        rpc_impl.clone(),
                    )
                    .to_delegate();

                    handler.extend_with(api);
                }
                Api::PubSub => {
                    let api = PubSubClient::new(
                        self.runtime.executor(),
                        self.consensus.clone(),
                        self.notifications.clone(),
                    )
                    .to_delegate();

                    handler.extend_with(api);
                }
            }
        }
    }

    fn setup_apis(
        &self, conf: &Configuration, apis: Vec<Api>,
    ) -> MetaIoHandler<Metadata> {
        let mut handler = MetaIoHandler::default();
        self.extend_api(conf, &mut handler, apis);
        handler
    }
}
