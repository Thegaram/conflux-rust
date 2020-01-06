// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use super::{HSB_PROTOCOL_ID, HSB_PROTOCOL_VERSION};
use crate::{
    alliance_tree_graph::{
        bft::consensus::chained_bft::network::NetworkTask,
        hsb_sync_protocol::message::{
            block_retrieval::BlockRetrievalRpcRequest, msgid,
        },
    },
    hotstuff_types::{
        common::Payload,
        proposal_msg::{ProposalMsg, ProposalUncheckedSignatures},
        sync_info::SyncInfo,
        vote_msg::VoteMsg,
    },
    message::{Message, MsgId},
    network::{
        NetworkContext, NetworkProtocolHandler, NetworkService, PeerId,
        UpdateNodeOperation,
    },
    sync::{
        msg_sender::NULL,
        request_manager::{
            request_handler::AsAny, RequestManager, RequestMessage,
        },
        Error, ErrorKind,
    },
};

use cfx_types::H256;
use io::TimerToken;
use keccak_hash::keccak;
use network::node_table::NodeId;
use parking_lot::RwLock;
use primitives::TransactionWithSignature;
use serde::Deserialize;
use std::{
    any::Any, cmp::Eq, collections::HashMap, fmt::Debug, hash::Hash, sync::Arc,
};

#[derive(Default)]
pub struct PeerState {
    id: PeerId,
    peer_hash: H256,
}

impl PeerState {
    pub fn get_id(&self) -> PeerId { self.id }
}

#[derive(Default)]
pub struct Peers<T: Default, K: Eq + Hash + Default + Copy>(
    RwLock<HashMap<K, Arc<RwLock<T>>>>,
);

impl<T, K> Peers<T, K>
where
    T: Default,
    K: Eq + Hash + Default + Copy,
{
    pub fn new() -> Peers<T, K> { Self::default() }

    pub fn get(&self, peer: &K) -> Option<Arc<RwLock<T>>> {
        self.0.read().get(peer).cloned()
    }

    pub fn insert(&self, peer: K) {
        self.0
            .write()
            .entry(peer)
            .or_insert(Arc::new(RwLock::new(T::default())));
    }

    pub fn is_empty(&self) -> bool { self.0.read().is_empty() }

    pub fn contains(&self, peer: &K) -> bool {
        self.0.read().contains_key(peer)
    }

    pub fn remove(&self, peer: &K) -> Option<Arc<RwLock<T>>> {
        self.0.write().remove(peer)
    }

    pub fn all_peers_satisfying<F>(&self, mut predicate: F) -> Vec<K>
    where F: FnMut(&mut T) -> bool {
        self.0
            .read()
            .iter()
            .filter_map(|(id, state)| {
                if predicate(&mut *state.write()) {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn fold<B, F>(&self, init: B, f: F) -> B
    where F: FnMut(B, &Arc<RwLock<T>>) -> B {
        self.0.write().values().fold(init, f)
    }
}

pub struct Context<'a, P> {
    pub io: &'a dyn NetworkContext,
    pub peer: PeerId,
    pub peer_hash: H256,
    pub manager: &'a HotStuffSynchronizationProtocol<P>,
}

impl<'a, P: Payload> Context<'a, P> {
    pub fn match_request(
        &self, request_id: u64,
    ) -> Result<RequestMessage, Error> {
        self.manager
            .request_manager
            .match_request(self.io, self.peer, request_id)
    }

    pub fn send_response(&self, response: &dyn Message) -> Result<(), Error> {
        response.send(self.io, self.peer)?;
        Ok(())
    }
}

pub struct HotStuffSynchronizationProtocol<P> {
    pub own_node_hash: H256,
    pub peers: Arc<Peers<PeerState, H256>>,
    pub request_manager: Arc<RequestManager>,
    pub network_task: NetworkTask<P>,
}

impl<P: Payload> HotStuffSynchronizationProtocol<P> {
    pub fn new(
        own_node_hash: H256, request_manager: Arc<RequestManager>,
        network_task: NetworkTask<P>,
    ) -> Self
    {
        HotStuffSynchronizationProtocol {
            own_node_hash,
            peers: Arc::new(Peers::new()),
            request_manager,
            network_task,
        }
    }

    pub fn register(
        self: Arc<Self>, network: Arc<NetworkService>,
    ) -> Result<(), String> {
        network
            .register_protocol(self, HSB_PROTOCOL_ID, &[HSB_PROTOCOL_VERSION])
            .map_err(|e| {
                format!(
                    "failed to register HotStuffSynchronizationProtocol: {:?}",
                    e
                )
            })
    }

    /// In the event two peers simultaneously dial each other we need to be able
    /// to do tie-breaking to determine which connection to keep and which
    /// to drop in a deterministic way. One simple way is to compare our
    /// local PeerId with that of the remote's PeerId and
    /// keep the connection where the peer with the greater PeerId is the
    /// dialer.
    ///
    /// Returns `true` if the existing connection should be dropped and `false`
    /// if the new connection should be dropped.
    fn simultaneous_dial_tie_breaking(
        own_peer_id: H256, remote_peer_id: H256, existing_origin: bool,
        new_origin: bool,
    ) -> bool
    {
        match (existing_origin, new_origin) {
            // If the remote dials while an existing connection is open, the
            // older connection is dropped.
            (false /* in-bound */, false /* in-bound */) => true,
            (false /* in-bound */, true /* out-bound */) => {
                remote_peer_id < own_peer_id
            }
            (true /* out-bound */, false /* in-bound */) => {
                own_peer_id < remote_peer_id
            }
            // We should never dial the same peer twice, but if we do drop the
            // new connection
            (true /* out-bound */, true /* out-bound */) => false,
        }
    }

    fn handle_error(
        &self, _io: &dyn NetworkContext, _peer: PeerId, _msg_id: MsgId,
        _e: Error,
    )
    {

    }

    fn dispatch_message(
        &self, io: &dyn NetworkContext, peer: PeerId, msg_id: MsgId, msg: &[u8],
    ) -> Result<(), Error> {
        trace!("Dispatching message: peer={:?}, msg_id={:?}", peer, msg_id);
        let peer_hash = if peer != NULL {
            let node_id = io.get_peer_node_id(peer);
            if node_id == NodeId::default() {
                return Err(ErrorKind::UnknownPeer.into());
            }
            keccak(&node_id)
        } else {
            self.own_node_hash.clone()
        };

        let ctx = Context {
            peer_hash,
            peer,
            io,
            manager: self,
        };

        if !handle_serialized_message(msg_id, &ctx, msg)? {
            warn!("Unknown message: peer={:?} msgid={:?}", peer, msg_id);
            let reason =
                format!("unknown sync protocol message id {:?}", msg_id);
            io.disconnect_peer(
                peer,
                Some(UpdateNodeOperation::Remove),
                reason.as_str(),
            );
        }

        Ok(())
    }
}

pub fn handle_serialized_message<P>(
    id: MsgId, ctx: &Context<P>, msg: &[u8],
) -> Result<bool, Error>
where P: Payload {
    match id {
        msgid::PROPOSAL => {
            let msg: ProposalMsg<P> = lcs::from_bytes(msg)?;
            let msg_id = msg.msg_id();
            let msg_name = msg.msg_name();
            let req_id = msg.get_request_id();

            let proposal = ProposalUncheckedSignatures(msg);

            // FIXME: add throttling.

            if let Err(e) = proposal.handle(ctx) {
                info!(
                    "failed to handle sync protocol message, peer = {}, id = {}, name = {}, request_id = {:?}, error_kind = {:?}",
                    ctx.peer, msg_id, msg_name, req_id, e.0,
                );
                return Err(e);
            }
        }
        msgid::VOTE => handle_message::<VoteMsg, P>(ctx, msg)?,
        msgid::SYNC_INFO => handle_message::<SyncInfo, P>(ctx, msg)?,
        msgid::BLOCK_RETRIEVAL => {
            handle_message::<BlockRetrievalRpcRequest, P>(ctx, msg)?
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn handle_message<'a, M, P>(
    ctx: &Context<P>, msg: &'a [u8],
) -> Result<(), Error>
where M: Deserialize<'a> + Handleable<P> + Message {
    let msg: M = lcs::from_bytes(msg)?;

    let msg_id = msg.msg_id();
    let msg_name = msg.msg_name();
    let req_id = msg.get_request_id();

    trace!(
        "handle sync protocol message, peer = {:?}, id = {}, name = {}, request_id = {:?}",
        ctx.peer_hash, msg_id, msg_name, req_id,
    );

    // FIXME: add throttling.

    if let Err(e) = msg.handle(ctx) {
        info!(
            "failed to handle sync protocol message, peer = {}, id = {}, name = {}, request_id = {:?}, error_kind = {:?}",
            ctx.peer, msg_id, msg_name, req_id, e.0,
        );

        return Err(e);
    }

    Ok(())
}

impl<P: Payload> NetworkProtocolHandler for HotStuffSynchronizationProtocol<P> {
    fn initialize(&self, _io: &dyn NetworkContext) {}

    fn on_message(&self, io: &dyn NetworkContext, peer: PeerId, raw: &[u8]) {
        let len = raw.len();
        if len < 2 {
            // Empty message.
            return self.handle_error(
                io,
                peer,
                msgid::INVALID,
                ErrorKind::InvalidMessageFormat.into(),
            );
        }

        let msg_id = raw[len - 1];
        debug!("on_message: peer={:?}, msgid={:?}", peer, msg_id);

        self.dispatch_message(io, peer, msg_id.into(), raw)
            .unwrap_or_else(|e| self.handle_error(io, peer, msg_id.into(), e));
    }

    fn on_peer_connected(&self, io: &dyn NetworkContext, peer: PeerId) {
        let new_originated = io.get_peer_connection_origin(peer);
        if new_originated.is_none() {
            debug!("Peer does not exist when just connected");
            return;
        }
        let new_originated = new_originated.unwrap();
        let node_id = io.get_peer_node_id(peer);
        let peer_hash = keccak(&node_id);

        let add_new_peer = if let Some(old_peer) = self.peers.remove(&peer_hash)
        {
            let old_peer_id = old_peer.read().id;
            let old_originated = io.get_peer_connection_origin(old_peer_id);
            if old_originated.is_none() {
                debug!("Old session does not exist.");
                true
            } else {
                let old_originated = old_originated.unwrap();
                if Self::simultaneous_dial_tie_breaking(
                    self.own_node_hash.clone(),
                    peer_hash.clone(),
                    old_originated,
                    new_originated,
                ) {
                    // Drop the existing connection and replace it with the new
                    // connection.
                    io.disconnect_peer(
                        old_peer_id,
                        Some(UpdateNodeOperation::Failure),
                        "remove old peer connection",
                    );
                    true
                } else {
                    // Drop the new connection.
                    false
                }
            }
        } else {
            true
        };

        if add_new_peer {
            self.peers.insert(peer_hash.clone());
            let peer_state =
                self.peers.get(&peer_hash).expect("peer not found");
            let mut peer_state = peer_state.write();
            peer_state.id = peer;
            peer_state.peer_hash = peer_hash;
        } else {
            io.disconnect_peer(
                peer,
                Some(UpdateNodeOperation::Failure),
                "remove new peer connection",
            );
        }
    }

    fn on_peer_disconnected(&self, io: &dyn NetworkContext, peer: PeerId) {
        let node_id = io.get_peer_node_id(peer);
        let peer_hash = keccak(&node_id);
        info!("on_peer_disconnected: peer={:?}", peer_hash);
        self.peers.remove(&peer_hash);
    }

    fn on_timeout(&self, _io: &dyn NetworkContext, _timer: TimerToken) {}
}

pub trait Handleable<P> {
    fn handle(self, ctx: &Context<P>) -> Result<(), Error>;
}

pub trait RpcResponse: Send + Sync + Debug + AsAny {}

impl From<lcs::Error> for Error {
    fn from(_: lcs::Error) -> Self { ErrorKind::InvalidMessageFormat.into() }
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        ErrorKind::InternalError(format!("{}", error)).into()
    }
}