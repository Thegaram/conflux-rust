// Copyright 2020 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

pub struct RecordingState<Storage: StateTrait> {
    storage: Storage,
    proof_in_progress: parking_lot::Mutex<StateProof>,
}

impl<Storage: StateTrait> RecordingState<Storage> {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            proof_in_progress: Default::default(),
        }
    }

    pub fn extract_proof(&self) -> StateProof {
        let mut empty = StateProof::default();
        std::mem::swap(&mut empty, &mut *self.proof_in_progress.lock());
        empty
    }
}

impl<Storage: StateTrait> StateTrait for RecordingState<Storage> {
    delegate! {
        to self.storage {
            fn get_with_proof(&self, access_key: StorageKey) -> Result<(Option<Box<[u8]>>, StateProof)>;
            // fn get_node_merkle_all_versions<WithProof: StaticBool>(&self, access_key: StorageKey) -> Result<(NodeMerkleTriplet, NodeMerkleProof)>;
            fn set(&mut self, access_key: StorageKey, value: Box<[u8]>) -> Result<()>;
            fn delete(&mut self, access_key: StorageKey) -> Result<()>;
            fn delete_test_only(&mut self, access_key: StorageKey) -> Result<Option<Box<[u8]>>>;
            // fn delete_all<AM: access_mode::AccessMode>(&mut self, access_key_prefix: StorageKey) -> Result<Option<Vec<MptKeyValue>>>;
            fn compute_state_root(&mut self) -> Result<StateRootWithAuxInfo>;
            fn get_state_root(&self) -> Result<StateRootWithAuxInfo>;
            fn commit(&mut self, epoch_id: EpochId) -> Result<StateRootWithAuxInfo>;
            fn revert(&mut self);
        }
    }

    fn get(&self, access_key: StorageKey) -> Result<Option<Box<[u8]>>> {
        let (val, proof) = self.storage.get_with_proof(access_key)?;

        let mut proof_in_progress = self.proof_in_progress.lock();
        *proof_in_progress = proof_in_progress
            .clone()
            .merge(proof)
            .expect("proof is valid"); // TODO: do not clone and handle error properly

        Ok(val)
    }

    fn get_node_merkle_all_versions<WithProof: StaticBool>(
        &self, access_key: StorageKey,
    ) -> Result<(NodeMerkleTriplet, NodeMerkleProof)> {
        self.storage
            .get_node_merkle_all_versions::<WithProof>(access_key)
    }

    fn delete_all<AM: access_mode::AccessMode>(
        &mut self, access_key_prefix: StorageKey,
    ) -> Result<Option<Vec<MptKeyValue>>> {
        self.storage.delete_all::<AM>(access_key_prefix)
    }
}

use crate::{
    impls::{
        errors::*, merkle_patricia_trie::MptKeyValue,
        node_merkle_proof::NodeMerkleProof, state_proof::StateProof,
    },
    state::*,
    utils::access_mode,
};
use cfx_internal_common::StateRootWithAuxInfo;
use delegate::delegate;
use primitives::{EpochId, NodeMerkleTriplet, StaticBool, StorageKey};