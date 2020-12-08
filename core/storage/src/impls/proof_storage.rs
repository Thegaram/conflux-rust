// Copyright 2020 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

pub struct ProofStorage {
    proof: StateProof,
    root: StateRoot,
    maybe_intermediate_padding: Option<DeltaMptKeyPadding>,
}

impl ProofStorage {
    pub fn new(
        proof: StateProof, root: StateRoot,
        maybe_intermediate_padding: Option<DeltaMptKeyPadding>,
    ) -> Self
    {
        Self {
            proof,
            root,
            maybe_intermediate_padding,
        }
    }
}

impl StateTrait for ProofStorage {
    fn commit(&mut self, epoch_id: EpochId) -> Result<StateRootWithAuxInfo> {
        bail!(
            "ProvingState is read-only; unexpected call: commit({:?})",
            epoch_id
        );
    }

    fn compute_state_root(&mut self) -> Result<StateRootWithAuxInfo> {
        bail!("Unexpected call on ProvingState: compute_state_root()");
    }

    fn delete(&mut self, access_key: StorageKey) -> Result<()> {
        bail!(
            "ProvingState is read-only; unexpected call: delete({:?})",
            access_key
        );
    }

    fn delete_all<AM: access_mode::AccessMode>(
        &mut self, access_key_prefix: StorageKey,
    ) -> Result<Option<Vec<MptKeyValue>>> {
        trace!(
            "ProvingState::delete_all<{}>({:?})",
            AM::is_read_only(),
            access_key_prefix
        );

        if !AM::is_read_only() {
            bail!("ProvingState is read-only; unexpected call: delete_all<Write>({:?})", access_key_prefix);
        }

        match self.proof.traverse(
            access_key_prefix,
            &self.root,
            &self.maybe_intermediate_padding,
        ) {
            (false, _) => bail!(
                "Call failed on ProvingState: delete_all({:?})",
                access_key_prefix
            ), // TODO
            (true, kvs) if kvs.is_empty() => Ok(None),
            (true, kvs) => Ok(Some(kvs)),
        }
    }

    fn delete_test_only(
        &mut self, access_key: StorageKey,
    ) -> Result<Option<Box<[u8]>>> {
        bail!(
            "Unexpected call on ProvingState: delete_test_only({:?})",
            access_key
        );
    }

    fn get(&self, access_key: StorageKey) -> Result<Option<Box<[u8]>>> {
        trace!("ProvingState::get({:?})", access_key);

        match self.proof.get_value(
            access_key,
            &self.root,
            &self.maybe_intermediate_padding,
        ) {
            (false, _) => {
                bail!("Call failed on ProvingState: get({:?})", access_key)
            } // TODO
            (true, None) => Ok(None),
            (true, Some(v)) => Ok(Some(v.to_vec().into_boxed_slice())), // TODO
        }
    }

    fn get_node_merkle_all_versions<WithProof: StaticBool>(
        &self, access_key: StorageKey,
    ) -> Result<(NodeMerkleTriplet, NodeMerkleProof)> {
        bail!("Unexpected call on ProvingState: get_node_merkle_all_versions({:?})", access_key);
    }

    fn get_state_root(&self) -> Result<StateRootWithAuxInfo> {
        bail!("Unexpected call on ProvingState: get_state_root()");
    }

    fn get_with_proof(
        &self, access_key: StorageKey,
    ) -> Result<(Option<Box<[u8]>>, StateProof)> {
        bail!(
            "Unexpected call on ProvingState: get_with_proof({:?})",
            access_key
        );
    }

    fn set(&mut self, access_key: StorageKey, value: Box<[u8]>) -> Result<()> {
        bail!(
            "ProvingState is read-only; unexpected call: set({:?}, {:?})",
            access_key,
            value
        );
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
use primitives::{
    DeltaMptKeyPadding, EpochId, NodeMerkleTriplet, StateRoot, StaticBool,
    StorageKey,
};
