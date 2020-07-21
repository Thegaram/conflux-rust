// Copyright 2020 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

fn min_repr_bytes(mut number_of_keys: usize) -> u8 {
    let mut min_repr_bytes = 0;
    while number_of_keys != 0 {
        min_repr_bytes += 1;
        number_of_keys >>= 8;
    }

    min_repr_bytes
}

fn to_index_bytes(mut index: usize, len: u8) -> Vec<u8> {
    let mut bytes = vec![0u8; len as usize];
    for i in 0..(len as usize) {
        bytes[i] = index as u8;
        index >>= 8;
    }

    bytes
}

/// Given an integer-indexed `SimpleMpt` with `num_keys` elements
/// stored in it, convert `key` into the corresponding key format.
pub fn into_simple_mpt_key(key: usize, num_keys: usize) -> Vec<u8> {
    let key_length = min_repr_bytes(num_keys);
    to_index_bytes(key, key_length)
}

pub fn make_simple_mpt(mut values: Vec<Box<[u8]>>) -> SimpleMpt {
    let mut mpt = SimpleMpt::default();
    let keys = values.len();
    let mut mpt_kvs = Vec::with_capacity(keys);

    let index_byte_len = min_repr_bytes(keys);
    for (index, value) in values.drain(..).enumerate() {
        mpt_kvs.push((to_index_bytes(index, index_byte_len), value));
    }

    MptMerger::new(None, &mut mpt)
        .merge(&DumpedMptKvIterator { kv: mpt_kvs })
        .expect("SimpleMpt does not fail.");

    mpt
}

pub fn simple_mpt_merkle_root(simple_mpt: &mut SimpleMpt) -> MerkleHash {
    let maybe_root_node = simple_mpt
        .load_node(&CompressedPathRaw::default())
        .expect("SimpleMpt does not fail.");
    match maybe_root_node {
        None => MERKLE_NULL_NODE,
        Some(root_node) => {
            // if all keys share the same prefix (e.g. 0x00, ..., 0x0f share
            // the first nibble) then they will all be under the first child of
            // the root. in this case, we will use this first child as the root.
            if root_node.get_children_count() == 1 {
                trace!(
                    "debug receipts calculation: root node {:?}",
                    simple_mpt.load_node(
                        &CompressedPathRaw::new_and_apply_mask(
                            &[0],
                            CompressedPathRaw::second_nibble_mask()
                        )
                    )
                );

                root_node.get_child(0).expect("Child 0 must exist").merkle
            } else {
                trace!("debug receipts calculation: root node {:?}", root_node);
                *root_node.get_merkle()
            }
        }
    }
}

pub fn simple_mpt_proof(
    simple_mpt: &mut SimpleMpt, access_key: &[u8],
) -> TrieProof {
    let mut cursor = MptCursor::<
        &mut dyn SnapshotMptTraitRead,
        BasicPathNode<&mut dyn SnapshotMptTraitRead>,
    >::new(simple_mpt);

    cursor.load_root().expect("load_root should succeed");

    // see comment in `simple_mpt_merkle_root`
    let remove_root = cursor.current_node_mut().get_children_count() == 1;

    cursor
        .open_path_for_key::<access_mode::Read>(access_key)
        .expect("open_path_for_key should succeed");

    let mut proof = cursor.to_proof();
    cursor.finish().expect("finish should succeed");

    if remove_root {
        proof = TrieProof::new(proof.get_proof_nodes()[1..].to_vec())
            .expect("Proof with root removed is still connected");
    }

    proof
}

// FIXME: add tests and verification code with Vec<TrieProofNode>.

use crate::storage::{
    impls::merkle_patricia_trie::{
        mpt_cursor::{BasicPathNode, MptCursor},
        trie_node::TrieNodeTrait,
        walk::{access_mode, GetChildTrait},
        CompressedPathRaw, MptMerger, TrieProof,
    },
    storage_db::SnapshotMptTraitRead,
    tests::DumpedMptKvIterator,
};
use primitives::{MerkleHash, MERKLE_NULL_NODE};

pub use crate::storage::tests::FakeSnapshotMptDb as SimpleMpt;

#[cfg(test)]
mod tests {
    use super::{
        into_simple_mpt_key, make_simple_mpt, min_repr_bytes,
        simple_mpt_merkle_root, simple_mpt_proof,
    };

    #[test]
    fn test_min_repr_bytes() {
        assert_eq!(min_repr_bytes(0x000000), 0); // 0
        assert_eq!(min_repr_bytes(0x000001), 1); // 1
        assert_eq!(min_repr_bytes(0x0000ff), 1); // 255
        assert_eq!(min_repr_bytes(0x000100), 2); // 256
        assert_eq!(min_repr_bytes(0x00ff00), 2); // 65535
        assert_eq!(min_repr_bytes(0x010000), 3); // 65536
    }

    #[test]
    fn test_into_simple_mpt_key() {
        assert_eq!(into_simple_mpt_key(0x01, 1), vec![0x01]);
        assert_eq!(into_simple_mpt_key(0x01, 255), vec![0x01]);
        assert_eq!(into_simple_mpt_key(0x01, 256), vec![0x01, 0x00]);
        assert_eq!(into_simple_mpt_key(0x01, 65535), vec![0x01, 0x00]);
        assert_eq!(into_simple_mpt_key(0x01, 65536), vec![0x01, 0x00, 0x00]);
    }

    #[test]
    fn test_simple_proof() {
        let num_items: usize = 100; // max 127

        // create k-v pairs:
        // (0x00, 0x00 + num_items)
        // (0x01, 0x01 + num_items)
        // (0x02, 0x02 + num_items)
        // ...

        let value_from_key = |key| key as u8 + num_items as u8;

        let values: Vec<Box<[u8]>> = (0..num_items)
            .map(|k| value_from_key(k))
            .map(|v| vec![v].into_boxed_slice())
            .collect();

        let mut mpt = make_simple_mpt(values);
        let root = simple_mpt_merkle_root(&mut mpt);

        for k in 0..num_items {
            let key = into_simple_mpt_key(k, num_items);
            let proof = simple_mpt_proof(&mut mpt, &key);

            // proof should be able to verify correct k-v
            assert!(proof.is_valid_kv(
                &into_simple_mpt_key(k, num_items),
                Some(&[value_from_key(k)]),
                &root
            ));

            // proof with incorrect value should fail
            assert!(!proof.is_valid_kv(
                &into_simple_mpt_key(k, num_items),
                Some(&[value_from_key(k) - 1]),
                &root
            ));

            // proof should not be able to verify other values in the trie
            for other in 0..num_items {
                if k == other {
                    continue;
                }

                assert!(!proof.is_valid_kv(
                    &into_simple_mpt_key(other, num_items),
                    Some(&[value_from_key(other)]),
                    &root
                ));
            }
        }
    }
}
