// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use cfx_types::{Bloom, H160, H256};
use rlp_derive::{RlpDecodable, RlpEncodable};

use super::NodeType;
use crate::{
    message::RequestId,
    storage::{StateProof, StorageRootProof},
};

use primitives::{
    BlockHeader as PrimitiveBlockHeader, BlockReceipts, ChainIdParams,
    SignedTransaction, StateRoot as PrimitiveStateRoot,
    StorageRoot as PrimitiveStorageRoot,
};

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct StatusPingDeprecatedV1 {
    pub genesis_hash: H256,
    pub node_type: NodeType,
    pub protocol_version: u8,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct StatusPongDeprecatedV1 {
    pub best_epoch: u64,
    pub genesis_hash: H256,
    pub node_type: NodeType,
    pub protocol_version: u8,
    pub terminals: Vec<H256>,
}

#[derive(Clone, Debug, RlpEncodable, RlpDecodable)]
pub struct StatusPingV2 {
    pub chain_id: ChainIdParams,
    pub genesis_hash: H256,
    pub node_type: NodeType,
}

#[derive(Clone, Debug, RlpEncodable, RlpDecodable)]
pub struct StatusPongV2 {
    pub best_epoch: u64,
    pub chain_id: ChainIdParams,
    pub genesis_hash: H256,
    pub node_type: NodeType,
    pub terminals: Vec<H256>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetBlockHashesByEpoch {
    pub request_id: RequestId,
    pub epochs: Vec<u64>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct BlockHashes {
    pub request_id: RequestId,
    pub hashes: Vec<H256>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetBlockHeaders {
    pub request_id: RequestId,
    pub hashes: Vec<H256>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct BlockHeaders {
    pub request_id: RequestId,
    pub headers: Vec<PrimitiveBlockHeader>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct NewBlockHashes {
    pub hashes: Vec<H256>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct SendRawTx {
    pub raw: Vec<u8>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetReceipts {
    pub request_id: RequestId,
    pub epochs: Vec<u64>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct ReceiptsWithEpoch {
    pub epoch: u64,
    pub epoch_receipts: Vec<BlockReceipts>,
}

#[derive(Clone, Debug, RlpEncodable, RlpDecodable)]
pub struct Receipts {
    pub request_id: RequestId,
    pub receipts: Vec<ReceiptsWithEpoch>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetTxs {
    pub request_id: RequestId,
    pub hashes: Vec<H256>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct Txs {
    pub request_id: RequestId,
    pub txs: Vec<SignedTransaction>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetWitnessInfo {
    pub request_id: RequestId,
    pub witnesses: Vec<u64>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct WitnessInfoWithHeight {
    pub height: u64,
    pub state_roots: Vec<H256>,
    pub receipt_hashes: Vec<H256>,
    pub bloom_hashes: Vec<H256>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct WitnessInfo {
    pub request_id: RequestId,
    pub infos: Vec<WitnessInfoWithHeight>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetBlooms {
    pub request_id: RequestId,
    pub epochs: Vec<u64>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct BloomWithEpoch {
    pub epoch: u64,
    pub bloom: Bloom,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct Blooms {
    pub request_id: RequestId,
    pub blooms: Vec<BloomWithEpoch>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetBlockTxs {
    pub request_id: RequestId,
    pub hashes: Vec<H256>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct BlockTxsWithHash {
    pub hash: H256,
    pub block_txs: Vec<SignedTransaction>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct BlockTxs {
    pub request_id: RequestId,
    pub block_txs: Vec<BlockTxsWithHash>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetStateRoots {
    pub request_id: RequestId,
    pub epochs: Vec<u64>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct StateRootWithEpoch {
    pub epoch: u64,
    pub state_root: PrimitiveStateRoot,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct StateRoots {
    pub request_id: RequestId,
    pub state_roots: Vec<StateRootWithEpoch>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, Hash, RlpEncodable, RlpDecodable,
)]
pub struct StateKey {
    pub epoch: u64,
    pub key: Vec<u8>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetStateEntries {
    pub request_id: RequestId,
    pub keys: Vec<StateKey>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct StateEntryWithKey {
    pub key: StateKey,
    pub entry: Option<Vec<u8>>,
    pub proof: StateProof,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct StateEntries {
    pub request_id: RequestId,
    pub entries: Vec<StateEntryWithKey>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetTxInfos {
    pub request_id: RequestId,
    pub hashes: Vec<H256>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct TxInfo {
    pub epoch: u64,
    pub block_hash: H256,
    pub index: usize,
    pub epoch_receipts: Vec<BlockReceipts>,
    pub block_txs: Vec<SignedTransaction>,
    pub tx_hash: H256,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct TxInfos {
    pub request_id: RequestId,
    pub infos: Vec<TxInfo>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, Hash, RlpEncodable, RlpDecodable,
)]
pub struct StorageRootKey {
    pub epoch: u64,
    pub address: H160,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct GetStorageRoots {
    pub request_id: RequestId,
    pub keys: Vec<StorageRootKey>,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct StorageRootWithKey {
    pub key: StorageRootKey,
    pub root: Option<PrimitiveStorageRoot>,
    pub proof: StorageRootProof,
}

#[derive(Clone, Debug, Default, RlpEncodable, RlpDecodable)]
pub struct StorageRoots {
    pub request_id: RequestId,
    pub roots: Vec<StorageRootWithKey>,
}
