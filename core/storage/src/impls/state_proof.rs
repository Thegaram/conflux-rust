// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

// FIXME: What's the proper way to express: 1) Proof not available;
// FIXME: 2) What if Intermediate Delta Root is MERKLE_NULL_NODE.
// TODO: Maybe create a new class for special situation when
// TODO: a full node does not have full state proof, but it
// TODO: could provide a shortcut proof with snapshot_proof
// TODO: at intermediate_epoch_id with delta_proof.
#[derive(Clone, Debug, Default, PartialEq, RlpEncodable, RlpDecodable)]
pub struct StateProof {
    pub delta_proof: Option<TrieProof>,
    pub intermediate_proof: Option<TrieProof>,
    pub snapshot_proof: Option<TrieProof>,
}

impl StateProof {
    pub fn with_delta(
        &mut self, maybe_delta_proof: Option<TrieProof>,
    ) -> &mut Self {
        self.delta_proof = maybe_delta_proof;
        self
    }

    pub fn with_intermediate(
        &mut self, maybe_intermediate_proof: Option<TrieProof>,
    ) -> &mut Self {
        self.intermediate_proof = maybe_intermediate_proof;
        self
    }

    pub fn with_snapshot(
        &mut self, maybe_snapshot_proof: Option<TrieProof>,
    ) -> &mut Self {
        self.snapshot_proof = maybe_snapshot_proof;
        self
    }

    // TODO
    pub fn merge(self, other: StateProof) -> Result<StateProof, Error> {
        let mut proof = StateProof::default();

        proof.with_delta(match (self.delta_proof, other.delta_proof) {
            (Some(p1), Some(p2)) => Some(p1.merge(p2)?),
            (Some(p1), None) => Some(p1),
            (None, Some(p2)) => Some(p2),
            (None, None) => None,
        });

        proof.with_intermediate(
            match (self.intermediate_proof, other.intermediate_proof) {
                (Some(p1), Some(p2)) => Some(p1.merge(p2)?),
                (Some(p1), None) => Some(p1),
                (None, Some(p2)) => Some(p2),
                (None, None) => None,
            },
        );

        proof.with_snapshot(
            match (self.snapshot_proof, other.snapshot_proof) {
                (Some(p1), Some(p2)) => Some(p1.merge(p2)?),
                (Some(p1), None) => Some(p1),
                (None, Some(p2)) => Some(p2),
                (None, None) => None,
            },
        );

        Ok(proof)
    }

    pub fn is_valid_kv(
        &self, key: &Vec<u8>, value: Option<&[u8]>, root: StateRoot,
        maybe_intermediate_padding: Option<DeltaMptKeyPadding>,
    ) -> bool
    {
        let delta_root = &root.delta_root;
        let intermediate_root = &root.intermediate_delta_root;
        let snapshot_root = &root.snapshot_root;

        let delta_mpt_padding =
            StorageKey::delta_mpt_padding(&snapshot_root, &intermediate_root);

        let storage_key = match StorageKey::from_key_bytes::<CheckInput>(&key) {
            Ok(k) => k,
            Err(e) => {
                warn!("Checking proof with invalid key: {:?}", e);
                return false;
            }
        };

        let delta_mpt_key =
            storage_key.to_delta_mpt_key_bytes(&delta_mpt_padding);
        let maybe_intermediate_mpt_key = maybe_intermediate_padding
            .as_ref()
            .map(|p| storage_key.to_delta_mpt_key_bytes(p));

        let tombstone_value = MptValue::<Box<[u8]>>::TombStone.unwrap();
        let delta_value = if value.is_some() {
            // Actual value.
            value.clone()
        } else {
            // Tombstone value.
            Some(&*tombstone_value)
        };

        // The delta proof must prove the key-value or key non-existence.
        match &self.delta_proof {
            Some(proof) => {
                // Existence proof.
                if proof.is_valid_kv(&delta_mpt_key, delta_value, delta_root) {
                    return true;
                }
                // Non-existence proof.
                if !proof.is_valid_kv(&delta_mpt_key, None, delta_root) {
                    return false;
                }
            }
            None => {
                // When delta trie exists, the proof can't be empty.
                if delta_root.ne(&MERKLE_NULL_NODE) {
                    return false;
                }
            }
        }

        // Now check intermediate_proof since it's required. Same logic applies.
        match &self.intermediate_proof {
            Some(proof) => {
                if maybe_intermediate_mpt_key.is_none() {
                    return false;
                }
                if proof.is_valid_kv(
                    maybe_intermediate_mpt_key.as_ref().unwrap(),
                    delta_value,
                    intermediate_root,
                ) {
                    return true;
                }
                if !proof.is_valid_kv(
                    maybe_intermediate_mpt_key.as_ref().unwrap(),
                    None,
                    intermediate_root,
                ) {
                    return false;
                }
            }
            None => {
                // When intermediate trie exists, the proof can't be empty.
                if intermediate_root.ne(&MERKLE_NULL_NODE) {
                    return false;
                }
            }
        }

        // At last, check snapshot
        match &self.snapshot_proof {
            None => false,
            Some(proof) => proof.is_valid_kv(key, value, snapshot_root),
        }
    }

    pub fn get_value(&self, key: StorageKey, state_root: &StateRoot, maybe_intermediate_padding: Option<DeltaMptKeyPadding>) -> (bool, Option<&[u8]>) {
        let delta_root = &state_root.delta_root;
        let intermediate_root = &state_root.intermediate_delta_root;
        let snapshot_root = &state_root.snapshot_root;

        // --------- delta ---------

        let padding = StorageKey::delta_mpt_padding(
            &snapshot_root,
            &intermediate_root,
        );

        let delta_key = key.to_delta_mpt_key_bytes(&padding);

        // TODO: what if none?
        match self.delta_proof.as_ref().unwrap().get_value(&delta_key[..], delta_root) {
            (false, _) => return (false, None),
            (true, Some(x)) => return (true, Some(x)),
            _ => {}
        }

        // --------- intermediate ---------

        let intermediate_key = match maybe_intermediate_padding {
            None => return (false, None),
            Some(p) => key.to_delta_mpt_key_bytes(&p),
        };

        match self.intermediate_proof.as_ref().unwrap().get_value(&intermediate_key[..], intermediate_root) {
            (false, _) => return (false, None),
            (true, Some(x)) => return (true, Some(x)),
            _ => {}
        }

        // --------- snapshot ---------

        let storage_key = key.to_key_bytes();

        match self.snapshot_proof.as_ref().unwrap().get_value(&storage_key[..], snapshot_root) {
            (false, _) => return (false, None),
            (true, Some(x)) => return (true, Some(x)),
            _ => return (true, None),
        }
    }
}

use crate::impls::{merkle_patricia_trie::TrieProof, errors::Error};
use primitives::{
    CheckInput, DeltaMptKeyPadding, MptValue, StateRoot, StorageKey,
    MERKLE_NULL_NODE, MerkleHash,
};
use rlp_derive::{RlpDecodable, RlpEncodable};
