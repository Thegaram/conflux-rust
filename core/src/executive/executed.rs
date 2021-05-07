// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use crate::{bytes::Bytes, vm};
use cfx_types::{Address, U256, U512};
use primitives::{receipt::StorageChange, LogEntry, TransactionWithSignature};
use solidity_abi::{ABIDecodable, ABIDecodeError};

#[derive(Debug, PartialEq, Clone)]
pub struct Executed {
    /// Gas used during execution of transaction.
    pub gas_used: U256,

    /// Fee that need to be paid by execution of this transaction.
    pub fee: U256,

    /// Gas charged during execution of transaction.
    pub gas_charged: U256,

    /// If the gas fee is born by designated sponsor.
    pub gas_sponsor_paid: bool,

    /// Vector of logs generated by transaction.
    pub logs: Vec<LogEntry>,

    /// If the storage cost is born by designated sponsor.
    pub storage_sponsor_paid: bool,

    /// Any accounts that occupy some storage.
    pub storage_collateralized: Vec<StorageChange>,

    /// Any accounts that release some storage.
    pub storage_released: Vec<StorageChange>,

    /// Addresses of contracts created during execution of transaction.
    /// Ordered from earliest creation.
    ///
    /// eg. sender creates contract A and A in constructor creates contract B
    ///
    /// B creation ends first, and it will be the first element of the vector.
    pub contracts_created: Vec<Address>,
    /// Transaction output.
    pub output: Bytes,
    /// The trace of this transaction.
    pub trace: Vec<ExecTrace>,
}

#[derive(Debug)]
pub enum ToRepackError {
    /// Returned when transaction nonce does not match state nonce.
    InvalidNonce {
        /// Nonce expected.
        expected: U256,
        /// Nonce found.
        got: U256,
    },

    /// Epoch height out of bound.
    /// The transaction was correct in the block where it's packed, but
    /// falls into the error when in the epoch to execute.
    EpochHeightOutOfBound {
        block_height: u64,
        set: u64,
        transaction_epoch_bound: u64,
    },

    /// Returned when cost of transaction (value + gas_price * gas) exceeds
    /// current sponsor balance.
    NotEnoughCashFromSponsor {
        /// Minimum required gas cost.
        required_gas_cost: U512,
        /// Actual balance of gas sponsor.
        gas_sponsor_balance: U512,
        /// Minimum required storage collateral cost.
        required_storage_cost: U256,
        /// Actual balance of storage sponsor.
        storage_sponsor_balance: U256,
    },

    /// Returned when a non-sponsored transaction's sender does not exist yet.
    SenderDoesNotExist,
}

#[derive(Debug)]
pub enum TxDropError {
    /// The account nonce in world-state is larger than tx nonce
    OldNonce(U256, U256),

    /// The recipient of current tx is in invalid address field.
    /// Although it can be verified in tx packing,
    /// by spec doc, it is checked in execution.
    InvalidRecipientAddress(Address),
}

#[derive(Debug, PartialEq)]
pub enum ExecutionError {
    /// Returned when cost of transaction (value + gas_price * gas) exceeds
    /// current sender balance.
    NotEnoughCash {
        /// Minimum required balance.
        required: U512,
        /// Actual balance.
        got: U512,
        /// Actual gas cost. This should be min(gas_fee, balance).
        actual_gas_cost: U256,
        /// Maximum storage limit cost.
        max_storage_limit_cost: U256,
    },
    VmError(vm::Error),
}

#[derive(Debug)]
pub enum ExecutionOutcome {
    NotExecutedDrop(TxDropError),
    NotExecutedToReconsiderPacking(ToRepackError),
    ExecutionErrorBumpNonce(ExecutionError, Executed),
    Finished(Executed),
}

impl ExecutionOutcome {
    pub fn successfully_executed(self) -> Option<Executed> {
        match self {
            ExecutionOutcome::Finished(executed) => Some(executed),
            _ => None,
        }
    }
}

impl Executed {
    pub fn not_enough_balance_fee_charged(
        tx: &TransactionWithSignature, fee: &U256,
    ) -> Self {
        let gas_charged = if tx.gas_price == U256::zero() {
            U256::zero()
        } else {
            fee / tx.gas_price
        };
        Self {
            gas_used: tx.gas,
            gas_charged,
            fee: fee.clone(),
            gas_sponsor_paid: false,
            logs: vec![],
            contracts_created: vec![],
            storage_sponsor_paid: false,
            storage_collateralized: Vec::new(),
            storage_released: Vec::new(),
            output: Default::default(),
            trace: Default::default(),
        }
    }

    pub fn execution_error_fully_charged(
        tx: &TransactionWithSignature,
    ) -> Self {
        Self {
            gas_used: tx.gas,
            gas_charged: tx.gas,
            fee: tx.gas * tx.gas_price,
            gas_sponsor_paid: false,
            logs: vec![],
            contracts_created: vec![],
            storage_sponsor_paid: false,
            storage_collateralized: Vec::new(),
            storage_released: Vec::new(),
            output: Default::default(),
            trace: Default::default(),
        }
    }
}

pub fn revert_reason_decode(output: &Bytes) -> String {
    const MAX_LENGTH: usize = 50;
    let decode_result = if output.len() < 4 {
        Err(ABIDecodeError("Uncompleted Signature"))
    } else {
        let (sig, data) = output.split_at(4);
        if sig != [8, 195, 121, 160] {
            Err(ABIDecodeError("Unrecognized Signature"))
        } else {
            String::abi_decode(data)
        }
    };
    match decode_result {
        Ok(str) => {
            if str.len() < MAX_LENGTH {
                format!("Reason provided by the contract: '{}'", str)
            } else {
                // FIXME: do we need a more efficient way for string
                // concatenation.
                format!(
                    "Reason provided by the contract: '{}...'",
                    str[..MAX_LENGTH].to_string()
                )
            }
            .to_string()
        }
        Err(_) if output.len() == 0 => "".to_string(),
        Err(_) => "Unrecognized error message".to_string(),
    }
}

use crate::trace::trace::ExecTrace;
#[cfg(test)]
use rustc_hex::FromHex;

#[test]
fn test_decode_result() {
    let input_hex =
        "08c379a0\
         0000000000000000000000000000000000000000000000000000000000000020\
         0000000000000000000000000000000000000000000000000000000000000018\
         e699bae59586e4b88de8b6b3efbc8ce8afb7e58585e580bc0000000000000000";
    assert_eq!(
        "Reason provided by the contract: '智商不足，请充值'"
            .to_string(),
        revert_reason_decode(&input_hex.from_hex().unwrap())
    );
}
