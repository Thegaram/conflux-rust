// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::RepeatVec;
use proptest::{
    collection::vec, prelude::*, sample::Index as PropIndex,
    test_runner::TestCaseResult,
};
use proptest_derive::Arbitrary;
use std::{
    collections::HashSet,
    fmt, iter, mem,
    sync::atomic::{AtomicUsize, Ordering},
};

trait RepeatVecMethods<T>: fmt::Debug {
    fn len(&self) -> usize;
    fn extend(&mut self, item: T, size: usize);
    fn remove(&mut self, logical_index: usize);
    fn remove_all(&mut self, logical_indexes: impl IntoIterator<Item = usize>);
    fn get(&self, at: usize) -> Option<(&T, usize)>;
}

impl<T> RepeatVecMethods<T> for RepeatVec<T>
where T: fmt::Debug
{
    fn len(&self) -> usize { self.len() }

    fn extend(&mut self, item: T, size: usize) { self.extend(item, size) }

    fn remove(&mut self, logical_index: usize) { self.remove(logical_index) }

    fn remove_all(&mut self, logical_indexes: impl IntoIterator<Item = usize>) {
        self.remove_all(logical_indexes)
    }

    fn get(&self, at: usize) -> Option<(&T, usize)> { self.get(at) }
}

/// A naive implementation of `RepeatVec` that actually repeats its elements.
#[derive(Clone, Debug, Default)]
struct NaiveRepeatVec<T> {
    items: Vec<(T, usize)>,
}

impl<T> NaiveRepeatVec<T> {
    pub fn new() -> Self { Self { items: vec![] } }
}

impl<T> RepeatVecMethods<T> for NaiveRepeatVec<T>
where T: Clone + fmt::Debug
{
    fn len(&self) -> usize { self.items.len() }

    fn extend(&mut self, item: T, size: usize) {
        self.items.extend(
            iter::repeat(item)
                .enumerate()
                .map(|(offset, item)| (item, offset))
                .take(size),
        );
    }

    fn remove(&mut self, logical_index: usize) {
        self.remove_all(iter::once(logical_index))
    }

    fn remove_all(&mut self, logical_indexes: impl IntoIterator<Item = usize>) {
        let mut logical_indexes: Vec<_> = logical_indexes.into_iter().collect();
        logical_indexes.sort();
        logical_indexes.dedup();

        let new_items = {
            let items = self.items.drain(0..);
            let mut decrease = 0;
            let mut current_index = 0;

            items
                .enumerate()
                .filter_map(move |(idx, (item, offset))| {
                    if offset == 0 {
                        // Reset the decrease counter since this is a new item.
                        decrease = 0;
                    }
                    if let Some(remove_idx) = logical_indexes.get(current_index)
                    {
                        if idx == *remove_idx {
                            decrease += 1;
                            current_index += 1;
                            None
                        } else {
                            Some((item, offset - decrease))
                        }
                    } else {
                        Some((item, offset - decrease))
                    }
                })
                .collect()
        };
        mem::replace(&mut self.items, new_items);
    }

    fn get(&self, at: usize) -> Option<(&T, usize)> {
        // Unlike `RepeatVec`, this actually could return &(T, usize) because
        // that's how data is stored internally. But keeping the
        // signature identical makes more sense.
        self.items.get(at).map(|(item, offset)| (item, *offset))
    }
}

/// A counter where no two values generated by `next()` or `strategy()` are
/// equal.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Counter(usize);

impl Counter {
    fn next() -> Self {
        static COUNTER_NEXT: AtomicUsize = AtomicUsize::new(0);

        Counter(COUNTER_NEXT.fetch_add(1, Ordering::AcqRel))
    }

    fn strategy() -> impl Strategy<Value = Self> {
        // Note that this isn't Just(Self::next()) because that will keep
        // generating a single value over and over again.
        Self::next as fn() -> Self
    }
}

/// An operation on a RepeatVec.
#[derive(Arbitrary, Clone, Debug)]
#[proptest(params = "usize")]
enum RepeatVecOp {
    #[proptest(weight = 3)]
    Get(PropIndex),
    #[proptest(weight = 1)]
    #[proptest(
        strategy = "(Counter::strategy(), 0..params).prop_map(RepeatVecOp::Extend)"
    )]
    Extend((Counter, usize)),
    #[proptest(weight = 1)]
    Remove(PropIndex),
    #[proptest(weight = 1)]
    #[proptest(
        strategy = "vec(any::<PropIndex>(), 0..params).prop_map(RepeatVecOp::RemoveAll)"
    )]
    RemoveAll(Vec<PropIndex>),
}

impl RepeatVecOp {
    /// Strategy that only produces get operations.
    fn get_strategy() -> impl Strategy<Value = Self> {
        any::<PropIndex>().prop_map(RepeatVecOp::Get)
    }
}

#[test]
fn basic_ops() {
    basic_ops_impl(RepeatVec::new());
    basic_ops_impl(NaiveRepeatVec::new());
}

fn basic_ops_impl(repeat_vec: impl RepeatVecMethods<&'static str>) {
    let mut repeat_vec = repeat_vec;

    repeat_vec.extend("foo", 3);
    repeat_vec.extend("bar", 4);
    repeat_vec.extend("baz", 0);
    assert_eq!(repeat_vec.len(), 7);

    // Basic queries work.
    assert_eq!(repeat_vec.get(0), Some((&"foo", 0)));
    assert_eq!(repeat_vec.get(1), Some((&"foo", 1)));
    assert_eq!(repeat_vec.get(2), Some((&"foo", 2)));
    assert_eq!(repeat_vec.get(3), Some((&"bar", 0)));
    assert_eq!(repeat_vec.get(4), Some((&"bar", 1)));
    assert_eq!(repeat_vec.get(5), Some((&"bar", 2)));
    assert_eq!(repeat_vec.get(6), Some((&"bar", 3)));
    assert_eq!(repeat_vec.get(7), None);

    // Removing an element shifts all further elements to the left.
    repeat_vec.remove(1);
    assert_eq!(repeat_vec.len(), 6);
    assert_eq!(repeat_vec.get(0), Some((&"foo", 0)));
    assert_eq!(repeat_vec.get(1), Some((&"foo", 1)));
    assert_eq!(repeat_vec.get(2), Some((&"bar", 0)));
    assert_eq!(repeat_vec.get(3), Some((&"bar", 1)));
    assert_eq!(repeat_vec.get(4), Some((&"bar", 2)));
    assert_eq!(repeat_vec.get(5), Some((&"bar", 3)));
    assert_eq!(repeat_vec.get(6), None);

    // Removing multiple elements shifts all other elements to the left. 6 and 7
    // are ignored as out of bounds. Removing 0 and 1 causes the entire
    // surface to be dropped.
    repeat_vec.remove_all(vec![0, 1, 3, 6, 7]);
    assert_eq!(repeat_vec.len(), 3);
    assert_eq!(repeat_vec.get(0), Some((&"bar", 0)));
    assert_eq!(repeat_vec.get(1), Some((&"bar", 1)));
    assert_eq!(repeat_vec.get(2), Some((&"bar", 2)));
    assert_eq!(repeat_vec.get(3), None);
}

proptest! {
    // Counter uniqueness is not strictly necessary for RepeatVec, but it makes the tests less
    // forgiving.
    #[test]
    fn counter_uniqueness(counters in vec(Counter::strategy(), 0..100usize)) {
        let counters_len = counters.len();
        let set: HashSet<_> = counters.into_iter().collect();
        prop_assert_eq!(counters_len, set.len());
    }

    #[test]
    fn repeat_vec_gets(
        item_sizes in vec((Counter::strategy(), 0..1000usize), 0..100),
        gets in vec(RepeatVecOp::get_strategy(), 1..5000),
    ) {
        repeat_vec_proptest_impl(item_sizes, gets)?;
    }

    // Run remove ops with smaller numbers as removing is very expensive with the naive RepeatVec.
    // The numbers here are tweaked to ensure a wide range of sizes from 0 to 256.
    #[test]
    fn repeat_vec_all_ops(
        item_sizes in vec((Counter::strategy(), 0..16usize), 0..32),
        ops in vec(any_with::<RepeatVecOp>(16), 1..64),
    ) {
        repeat_vec_proptest_impl(item_sizes, ops)?;
    }
}

fn repeat_vec_proptest_impl(
    item_sizes: Vec<(Counter, usize)>, ops: Vec<RepeatVecOp>,
) -> TestCaseResult {
    let mut test_vec = RepeatVec::new();
    let mut naive_vec = NaiveRepeatVec::new();

    for (item, size) in item_sizes {
        test_vec.extend(item.clone(), size);
        naive_vec.extend(item, size);
    }

    prop_assert_eq!(test_vec.len(), naive_vec.len());

    fn scaled_index(index: &PropIndex, len: usize) -> usize {
        // Go roughly 10% beyond the end of the list to also check negative
        // cases.
        let scaled_len = len + (len / 10);
        if scaled_len == 0 {
            // The vector is empty -- return 0, which is beyond the end of the
            // vector (but that's fine).
            0
        } else {
            index.index(scaled_len)
        }
    }

    for op in ops {
        match op {
            RepeatVecOp::Get(query) => {
                // Go beyond the end of the list to also check negative cases.
                let at = scaled_index(&query, test_vec.len());
                let test_get = test_vec.get(at);
                prop_assert_eq!(test_get, naive_vec.get(at));
                if at >= test_vec.len() {
                    prop_assert!(test_get.is_none());
                } else {
                    prop_assert!(test_get.is_some());
                }
            }
            RepeatVecOp::Extend((counter, size)) => {
                test_vec.extend(counter.clone(), size);
                naive_vec.extend(counter.clone(), size);
            }
            RepeatVecOp::Remove(prop_index) => {
                let logical_index = scaled_index(&prop_index, test_vec.len());
                test_vec.remove(logical_index);
                naive_vec.remove(logical_index);
            }
            RepeatVecOp::RemoveAll(prop_indexes) => {
                let logical_indexes: Vec<_> = prop_indexes
                    .into_iter()
                    .map(|prop_index| {
                        // Go beyond the end of the list to also check out of
                        // bounds cases.
                        scaled_index(&prop_index, test_vec.len())
                    })
                    .collect();

                test_vec.remove_all(logical_indexes.iter().copied());
                naive_vec.remove_all(logical_indexes);
            }
        }

        prop_assert_eq!(test_vec.len(), naive_vec.len());
        test_vec.assert_invariants();
    }

    Ok(())
}
