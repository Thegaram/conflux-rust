// Copyright 2020 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

pub struct ProvingState {
    proof: StateProof,
    root: StateRoot,
    entries_set: HashMap<Vec<u8>, Box<[u8]>>,
}

impl ProvingState {
    pub fn new(proof: StateProof, root: StateRoot) -> Self {
        Self {
            proof,
            root,
            entries_set: Default::default(),
        }
    }
}

impl StateTrait for ProvingState {
    fn commit(&mut self, epoch_id: EpochId) -> Result<StateRootWithAuxInfo> {
        unimplemented!();
    }

    fn compute_state_root(&mut self) -> Result<StateRootWithAuxInfo> {
        unimplemented!();
    }

    fn delete(&mut self, access_key: StorageKey) -> Result<()> {
        unimplemented!();
    }

    fn delete_all<AM: access_mode::AccessMode>(&mut self, access_key_prefix: StorageKey) -> Result<Option<Vec<MptKeyValue>>> {
        unimplemented!();
    }

    fn delete_test_only(&mut self, access_key: StorageKey) -> Result<Option<Box<[u8]>>> {
        unimplemented!();
    }

    fn get(&self, access_key: StorageKey) -> Result<Option<Box<[u8]>>> {
        match self.entries_set.get(&access_key.to_key_bytes()) {
            Some(x) => Ok(Some(x.clone())),
            None => {
                match self.proof.get_value(access_key, &self.root, None) {
                    (false, _) => panic!("AA"), // TODO
                    (true, None) => Ok(None),
                    (true, Some(v)) => Ok(Some(v.to_vec().into_boxed_slice())), // TODO
                }
            }
        }
    }

    fn get_node_merkle_all_versions<WithProof: StaticBool>(&self, access_key: StorageKey) -> Result<(NodeMerkleTriplet, NodeMerkleProof)> {
        unimplemented!();
    }

    fn get_state_root(&self) -> Result<StateRootWithAuxInfo> {
        unimplemented!();
    }

    fn get_with_proof(&self, access_key: StorageKey) -> Result<(Option<Box<[u8]>>, StateProof)> {
        unimplemented!();
    }

    fn revert(&mut self) {
        unimplemented!();
    }

    fn set(&mut self, access_key: StorageKey, value: Box<[u8]>) -> Result<()> {
        self.entries_set.insert(access_key.to_key_bytes(), value);
        Ok(())
    }
}

use crate::{
    impls::{
        errors::*, merkle_patricia_trie::MptKeyValue,
        node_merkle_proof::NodeMerkleProof,
    },
    state::*,
    utils::access_mode,
    StateProof,
};
use cfx_internal_common::StateRootWithAuxInfo;
use primitives::{EpochId, NodeMerkleTriplet, StaticBool, StateRoot, StorageKey};
use std::collections::HashMap;