// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

mod authcodes;
mod builder;
mod config;
mod extractor;
mod helpers;
mod http_common;
mod impls;
mod informant;
mod interceptor;
mod metadata;
mod traits;
mod types;

use builder::{Api, RpcDependencies};
use extractor::RpcExtractor;
use metadata::Metadata;
use types::Origin;

pub use builder::{FullDependencies, LightDependencies};
pub use config::{
    HttpConfiguration, RpcImplConfiguration, TcpConfiguration, WsConfiguration,
};

use crate::configuration::Configuration;

use jsonrpc_core::MetaIoHandler;
use jsonrpc_http_server::{
    Server as HttpServer, ServerBuilder as HttpServerBuilder,
};
use jsonrpc_tcp_server::{
    Server as TcpServer, ServerBuilder as TcpServerBuilder,
};
use jsonrpc_ws_server::{Server as WsServer, ServerBuilder as WsServerBuilder};

mod error {
    use crate::rpc::helpers::errors::codes::EXCEPTION_ERROR;
    use cfxcore::storage::Error as StorageError;
    use jsonrpc_core::{Error as JsonRpcError, Result as JsonRpcResult};
    use primitives::filter::FilterError;
    use rlp::DecoderError;

    error_chain! {
        links {
        }

        foreign_links {
            FilterError(FilterError);
            Storage(StorageError);
            Decoder(DecoderError);
        }

        errors {
            JsonRpcError(e: JsonRpcError) {
                description("JsonRpcError directly constructed to return to Rpc peer.")
                display("JsonRpcError directly constructed to return to Rpc peer. Error: {}", e)
            }
        }
    }

    impl From<JsonRpcError> for Error {
        fn from(j: JsonRpcError) -> Self { ErrorKind::JsonRpcError(j).into() }
    }

    impl From<Error> for JsonRpcError {
        fn from(e: Error) -> Self {
            match e.0 {
                ErrorKind::JsonRpcError(j) => j,
                _ => JsonRpcError {
                    code: jsonrpc_core::ErrorCode::ServerError(EXCEPTION_ERROR),
                    message: format!("Error processing request: {}", e),
                    data: None,
                },
            }
        }
    }

    pub fn into_jsonrpc_result<T>(r: Result<T>) -> JsonRpcResult<T> {
        match r {
            Ok(t) => Ok(t),
            Err(e) => Err(e.into()),
        }
    }
}

pub use error::{
    into_jsonrpc_result, Error as RpcError,
    ErrorKind::JsonRpcError as JsonRpcErrorKind, Result as RpcResult,
};

pub fn start_tcp<H>(
    conf: TcpConfiguration, handler: H,
) -> Result<Option<TcpServer>, String>
where H: Into<MetaIoHandler<Metadata>> {
    if !conf.enabled {
        return Ok(None);
    }

    match TcpServerBuilder::with_meta_extractor(handler, RpcExtractor)
        .start(&conf.address)
    {
        Ok(server) => Ok(Some(server)),
        Err(io_error) => {
            Err(format!("TCP error: {} (addr = {})", io_error, conf.address))
        }
    }
}

pub fn start_ws<H>(
    conf: WsConfiguration, handler: H,
) -> Result<Option<WsServer>, String>
where H: Into<MetaIoHandler<Metadata>> {
    if !conf.enabled {
        return Ok(None);
    }

    match WsServerBuilder::with_meta_extractor(handler, RpcExtractor)
        .start(&conf.address)
    {
        Ok(server) => Ok(Some(server)),
        Err(io_error) => {
            Err(format!("WS error: {} (addr = {})", io_error, conf.address))
        }
    }
}

pub fn start_http(
    conf: HttpConfiguration, handler: MetaIoHandler<Metadata>,
) -> Result<Option<HttpServer>, String> {
    if !conf.enabled {
        return Ok(None);
    }

    match HttpServerBuilder::new(handler)
        .keep_alive(conf.keep_alive)
        .cors(conf.cors_domains.clone())
        .start_http(&conf.address)
    {
        Ok(server) => Ok(Some(server)),
        Err(io_error) => Err(format!(
            "HTTP error: {} (addr = {})",
            io_error, conf.address
        )),
    }
}

pub struct RpcHandle {
    pub debug_rpc_http_server: Option<HttpServer>,
    pub rpc_http_server: Option<HttpServer>,
    pub rpc_tcp_server: Option<TcpServer>,
    pub rpc_ws_server: Option<WsServer>,
}

pub fn set_up_rpc(
    conf: Configuration, rpc: impl RpcDependencies,
) -> Result<RpcHandle, String> {
    // set up local http
    let debug_rpc_http_server = start_http(
        conf.local_http_config(),
        rpc.setup_apis(
            &conf,
            vec![Api::Cfx("rpc_local"), Api::Local, Api::Test],
        ),
    )?;

    // set up http
    let mut http_apis = rpc.setup_apis(&conf, vec![Api::Cfx("rpc")]);

    if conf.is_test_or_dev_mode() {
        rpc.extend_api(&conf, &mut http_apis, vec![Api::Local, Api::Test]);
    }

    let rpc_http_server = start_http(conf.http_config(), http_apis)?;

    // set up tcp
    let mut tcp_apis =
        rpc.setup_apis(&conf, vec![Api::Cfx("rpc"), Api::PubSub]);

    if conf.is_test_or_dev_mode() {
        rpc.extend_api(&conf, &mut tcp_apis, vec![Api::Local, Api::Test]);
    }

    let rpc_tcp_server = start_tcp(conf.tcp_config(), tcp_apis)?;

    // set up ws
    let mut ws_apis = rpc.setup_apis(&conf, vec![Api::Cfx("rpc"), Api::PubSub]);

    if conf.is_test_or_dev_mode() {
        rpc.extend_api(&conf, &mut ws_apis, vec![Api::Local, Api::Test]);
    }

    let rpc_ws_server = start_ws(conf.ws_config(), ws_apis)?;

    Ok(RpcHandle {
        debug_rpc_http_server,
        rpc_http_server,
        rpc_tcp_server,
        rpc_ws_server,
    })
}
