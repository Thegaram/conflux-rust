// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use super::builtin::Builtin;
use crate::{
    builtin::{builtin_factory, Linear},
    spec::CommonParams,
    vm::Spec,
};
use cfx_types::{Address, H256};
use primitives::BlockNumber;
use std::{collections::BTreeMap, sync::Arc};

pub type SpecCreationRules = dyn Fn(&mut Spec, BlockNumber) + Sync + Send;

pub struct Machine {
    params: CommonParams,
    builtins: Arc<BTreeMap<Address, Builtin>>,
    spec_rules: Option<Box<SpecCreationRules>>,
}

impl Machine {
    pub fn builtin(
        &self, address: &Address, block_number: BlockNumber,
    ) -> Option<&Builtin> {
        self.builtins.get(address).and_then(|b| {
            if b.is_active(block_number) {
                Some(b)
            } else {
                None
            }
        })
    }

    /// Attach special rules to the creation of spec.
    pub fn set_spec_creation_rules(&mut self, rules: Box<SpecCreationRules>) {
        self.spec_rules = Some(rules);
    }

    /// Get the general parameters of the chain.
    pub fn params(&self) -> &CommonParams { &self.params }

    pub fn spec(&self, number: BlockNumber) -> Spec {
        let mut spec = Spec::new_spec();
        if let Some(ref rules) = self.spec_rules {
            (rules)(&mut spec, number)
        }
        spec
    }

    /// Builtin-contracts for the chain..
    pub fn builtins(&self) -> &BTreeMap<Address, Builtin> { &*self.builtins }
}

pub fn new_machine(params: CommonParams) -> Machine {
    Machine {
        params,
        builtins: Arc::new(BTreeMap::new()),
        spec_rules: None,
    }
}

pub fn new_machine_with_builtin(params: CommonParams) -> Machine {
    let mut btree = BTreeMap::new();
    btree.insert(
        Address::from(H256::from_low_u64_be(1)),
        Builtin::new(
            Box::new(Linear::new(3000, 0)),
            builtin_factory("ecrecover"),
            0,
        ),
    );
    btree.insert(
        Address::from(H256::from_low_u64_be(2)),
        Builtin::new(
            Box::new(Linear::new(60, 12)),
            builtin_factory("sha256"),
            0,
        ),
    );
    btree.insert(
        Address::from(H256::from_low_u64_be(3)),
        Builtin::new(
            Box::new(Linear::new(600, 120)),
            builtin_factory("ripemd160"),
            0,
        ),
    );
    btree.insert(
        Address::from(H256::from_low_u64_be(4)),
        Builtin::new(
            Box::new(Linear::new(15, 3)),
            builtin_factory("identity"),
            0,
        ),
    );
    Machine {
        params,
        builtins: Arc::new(btree),
        spec_rules: None,
    }
}
