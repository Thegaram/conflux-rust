// Copyright 2020 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

mod admin;
mod context;
mod sponsor;
mod staking;

use super::{
    function::{
        ExecutionTrait, InterfaceTrait, PreExecCheckConfTrait,
        UpfrontPaymentTrait,
    },
    InternalContractTrait, SolidityFunctionTrait,
};
use std::collections::{BTreeMap, HashMap};

pub use self::{
    admin::AdminControl, context::Context, sponsor::SponsorWhitelistControl,
    staking::Staking,
};

use cfx_types::Address;
use std::sync::Arc;

pub(super) type SolFnTable = HashMap<[u8; 4], Box<dyn SolidityFunctionTrait>>;

/// A marco to implement an internal contract.
#[macro_export]
macro_rules! make_solidity_contract {
    ( $(#[$attr:meta])* $visibility:vis struct $name:ident ($addr:expr,$gen_table:ident); ) => {
        $(#[$attr])*
        #[derive(Copy, Clone)]
        $visibility struct $name {
        }

        impl $name {
            pub fn instance() -> Self {
                Self {}
            }
        }

        impl InternalContractTrait for $name {
            fn address(&self) -> &Address { &$addr }
            fn get_func_table(&self) -> SolFnTable { $gen_table() }
        }
    };
}

/// A marco to construct the functions table for an internal contract for a list
/// of types implements `SolidityFunctionTrait`.
#[macro_export]
macro_rules! make_function_table {
    ($($func:ty), *) => { {
        let mut table = SolFnTable::new();
        $({ let f = <$func>::instance(); table.insert(f.function_sig(), Box::new(f)); }) *
        table
    } }
}

#[macro_export]
macro_rules! check_signature {
    ($interface:ident, $signature:expr) => {
        let f = $interface::instance();
        assert_eq!(
            f.function_sig().to_vec(),
            $signature.from_hex::<Vec<u8>>().unwrap(),
            "Test solidity signature for {}",
            f.name()
        );
    };
}

pub struct InternalContractMap {
    builtin: Arc<BTreeMap<Address, Box<dyn InternalContractTrait>>>,
}

impl std::ops::Deref for InternalContractMap {
    type Target = Arc<BTreeMap<Address, Box<dyn InternalContractTrait>>>;

    fn deref(&self) -> &Self::Target { &self.builtin }
}

impl InternalContractMap {
    pub fn at_block_number(block_number: u64) -> Self {
        let mut builtin = BTreeMap::new();
        let admin = internal_contract_factory("admin");
        let sponsor = internal_contract_factory("sponsor");
        let staking = internal_contract_factory("staking");
        let context = internal_contract_factory("context");

        builtin.insert(*admin.address(), admin);
        builtin.insert(*sponsor.address(), sponsor);
        builtin.insert(*staking.address(), staking);

        let context_contract_activation_block_number: u64 = 10; // TODO: read from config

        if block_number > context_contract_activation_block_number {
            builtin.insert(*context.address(), context);
        }

        Self {
            builtin: Arc::new(builtin),
        }
    }

    pub fn contract(
        &self, address: &Address,
    ) -> Option<&Box<dyn InternalContractTrait>> {
        self.builtin.get(address)
    }
}

/// Built-in instruction factory.
pub fn internal_contract_factory(name: &str) -> Box<dyn InternalContractTrait> {
    match name {
        "admin" => Box::new(AdminControl::instance()),
        "staking" => Box::new(Staking::instance()),
        "sponsor" => Box::new(SponsorWhitelistControl::instance()),
        "context" => Box::new(Context::instance()),
        _ => panic!("invalid internal contract name: {}", name),
    }
}
