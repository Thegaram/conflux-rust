// Copyright 2021 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use cfx_parameters::internal_contract_addresses::CONTEXT_ADDRESS;

use super::{
    ExecutionTrait, InterfaceTrait, InternalContractTrait,
    PreExecCheckConfTrait, SolFnTable, SolidityFunctionTrait,
    UpfrontPaymentTrait,
};
#[cfg(test)]
use crate::check_signature;
use crate::{
    evm::{ActionParams, Spec},
    impl_function_type, make_function_table, make_solidity_contract,
    make_solidity_function,
    state::CallStackInfo,
    trace::{trace::ExecTrace, Tracer},
    vm::{self, Env},
};
use cfx_state::{state_trait::StateOpsTrait, SubstateTrait};
use cfx_types::{Address, U256};
#[cfg(test)]
use rustc_hex::FromHex;

fn generate_fn_table() -> SolFnTable { make_function_table!(EpochNumber) }

make_solidity_contract! {
    pub struct Context(CONTEXT_ADDRESS, generate_fn_table);
}

make_solidity_function! {
    struct EpochNumber((), "epochNumber()", U256); // TODO: U64
}
impl_function_type!(EpochNumber,  "query", gas: |_| U256::from(2)); // TODO

impl ExecutionTrait for EpochNumber {
    fn execute_inner(
        &self, _input: (), _: &ActionParams, env: &Env, _: &Spec,
        _state: &mut dyn StateOpsTrait,
        _: &mut dyn SubstateTrait<CallStackInfo = CallStackInfo, Spec = Spec>,
        _tracer: &mut dyn Tracer<Output = ExecTrace>,
    ) -> vm::Result<U256>
    {
        Ok(U256::from(env.epoch_height))
    }
}

#[test]
fn test_context_contract_sig() {
    check_signature!(EpochNumber, "64efb22b"); // TODO
}
