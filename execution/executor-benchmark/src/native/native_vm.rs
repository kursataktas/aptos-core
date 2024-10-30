// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db_access::DbAccessUtil,
    native::{
        native_config::{NativeConfig, NATIVE_EXECUTOR_POOL},
        native_transaction::NativeTransaction,
    },
};
use aptos_block_executor::{
    code_cache_global::ImmutableModuleCache,
    errors::BlockExecutionError,
    task::{ExecutionStatus, ExecutorTask},
    txn_commit_hook::NoOpTransactionCommitHook,
};
use aptos_logger::error;
use aptos_mvhashmap::types::TxnIndex;
use aptos_types::{
    account_address::AccountAddress,
    account_config::{
        primary_apt_store, AccountResource, DepositFAEvent, FungibleStoreResource, WithdrawFAEvent,
    },
    block_executor::config::{
        BlockExecutorConfig, BlockExecutorConfigFromOnchain, BlockExecutorLocalConfig,
    },
    contract_event::ContractEvent,
    error::PanicError,
    executable::ExecutableTestType,
    fee_statement::FeeStatement,
    move_utils::move_event_v2::MoveEventV2Type,
    on_chain_config::FeatureFlag,
    state_store::{state_key::StateKey, state_value::StateValueMetadata, StateView},
    transaction::{
        signature_verified_transaction::SignatureVerifiedTransaction, BlockOutput, Transaction,
        TransactionAuxiliaryData, TransactionOutput, TransactionStatus, WriteSetPayload,
    },
    write_set::WriteOp,
};
use aptos_vm::{block_executor::AptosTransactionOutput, VMBlockExecutor};
use aptos_vm_environment::environment::AptosEnvironment;
use aptos_vm_types::{
    abstract_write_op::{AbstractResourceWriteOp, GroupWrite},
    change_set::VMChangeSet,
    module_write_set::ModuleWriteSet,
    output::VMOutput,
    resolver::{ExecutorView, ResourceGroupSize, ResourceGroupView},
    resource_group_adapter::group_tagged_resource_size,
};
use bytes::Bytes;
use move_core_types::{
    language_storage::StructTag,
    move_resource::MoveStructType,
    value::MoveTypeLayout,
    vm_status::{StatusCode, VMStatus},
};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

pub struct NativeVMBlockExecutor;

// Executor external API
impl VMBlockExecutor for NativeVMBlockExecutor {
    fn new() -> Self {
        Self
    }

    /// Execute a block of `transactions`. The output vector will have the exact same length as the
    /// input vector. The discarded transactions will be marked as `TransactionStatus::Discard` and
    /// have an empty `WriteSet`. Also `state_view` is immutable, and does not have interior
    /// mutability. Writes to be applied to the data view are encoded in the write set part of a
    /// transaction output.
    fn execute_block(
        &self,
        transactions: &[SignatureVerifiedTransaction],
        state_view: &(impl StateView + Sync),
        onchain_config: BlockExecutorConfigFromOnchain,
    ) -> Result<BlockOutput<TransactionOutput>, VMStatus> {
        Self::execute_block_impl(transactions, state_view, BlockExecutorConfig {
            local: BlockExecutorLocalConfig {
                concurrency_level: NativeConfig::get_concurrency_level(),
                allow_fallback: true,
                discard_failed_blocks: false,
            },
            onchain: onchain_config,
        })
    }
}

impl NativeVMBlockExecutor {
    pub fn execute_block_impl<S: StateView + Sync>(
        signature_verified_block: &[SignatureVerifiedTransaction],
        state_view: &S,
        config: BlockExecutorConfig,
    ) -> Result<BlockOutput<TransactionOutput>, VMStatus> {
        let executor = aptos_block_executor::executor::BlockExecutor::<
            SignatureVerifiedTransaction,
            NativeVMExecutorTask,
            S,
            NoOpTransactionCommitHook<AptosTransactionOutput, VMStatus>,
            ExecutableTestType,
        >::new(
            config,
            Arc::clone(&NATIVE_EXECUTOR_POOL),
            Arc::new(ImmutableModuleCache::empty()),
            None,
        );

        let environment = AptosEnvironment::new_with_delayed_field_optimization_enabled(state_view);
        let ret = executor.execute_block(environment, signature_verified_block, state_view);
        match ret {
            Ok(block_output) => {
                let (transaction_outputs, block_end_info) = block_output.into_inner();
                let output_vec: Vec<_> = transaction_outputs
                    .into_iter()
                    .map(|output| output.take_output())
                    .collect();

                // Flush the speculative logs of the committed transactions.
                // let pos = output_vec.partition_point(|o| !o.status().is_retry());

                Ok(BlockOutput::new(output_vec, block_end_info))
            },
            Err(BlockExecutionError::FatalBlockExecutorError(PanicError::CodeInvariantError(
                err_msg,
            ))) => Err(VMStatus::Error {
                status_code: StatusCode::DELAYED_FIELD_OR_BLOCKSTM_CODE_INVARIANT_ERROR,
                sub_status: None,
                message: Some(err_msg),
            }),
            Err(BlockExecutionError::FatalVMError(err)) => Err(err),
        }
    }
}

pub(crate) struct NativeVMExecutorTask {
    fa_migration_complete: bool,
    db_util: DbAccessUtil,
}

impl ExecutorTask for NativeVMExecutorTask {
    type Environment = AptosEnvironment;
    type Error = VMStatus;
    type Output = AptosTransactionOutput;
    type Txn = SignatureVerifiedTransaction;

    fn init(env: Self::Environment, _state_view: &impl StateView) -> Self {
        Self {
            fa_migration_complete: env
                .features()
                .is_enabled(FeatureFlag::OPERATIONS_DEFAULT_TO_FA_APT_STORE),
            db_util: DbAccessUtil::new(),
        }
    }

    // This function is called by the BlockExecutor for each transaction it intends
    // to execute (via the ExecutorTask trait). It can be as a part of sequential
    // execution, or speculatively as a part of a parallel execution.
    fn execute_transaction(
        &self,
        executor_with_group_view: &(impl ExecutorView + ResourceGroupView),
        txn: &SignatureVerifiedTransaction,
        _txn_idx: TxnIndex,
    ) -> ExecutionStatus<AptosTransactionOutput, VMStatus> {
        let gas_units = 4;
        let gas = gas_units * 100;

        match self.execute_transaction_impl(
            executor_with_group_view,
            txn,
            gas,
            self.fa_migration_complete,
        ) {
            Ok(change_set) => ExecutionStatus::Success(AptosTransactionOutput::new(VMOutput::new(
                change_set,
                ModuleWriteSet::empty(),
                FeeStatement::new(gas_units, gas_units, 0, 0, 0),
                TransactionStatus::Keep(aptos_types::transaction::ExecutionStatus::Success),
                TransactionAuxiliaryData::default(),
            ))),
            Err(_) => ExecutionStatus::SpeculativeExecutionAbortError("something".to_string()),
        }
    }

    fn is_transaction_dynamic_change_set_capable(txn: &Self::Txn) -> bool {
        if txn.is_valid() {
            if let Transaction::GenesisTransaction(WriteSetPayload::Direct(_)) = txn.expect_valid()
            {
                // WriteSetPayload::Direct cannot be handled in mode where delayed_field_optimization or
                // resource_groups_split_in_change_set is enabled.
                return false;
            }
        }
        true
    }
}

impl NativeVMExecutorTask {
    fn execute_transaction_impl(
        &self,
        view: &(impl ExecutorView + ResourceGroupView),
        txn: &SignatureVerifiedTransaction,
        gas: u64,
        fa_migration_complete: bool,
    ) -> Result<VMChangeSet, ()> {
        let mut resource_write_set = BTreeMap::new();
        let mut events = Vec::new();
        let delayed_field_change_set = BTreeMap::new();
        let aggregator_v1_write_set = BTreeMap::new();
        let aggregator_v1_delta_set = BTreeMap::new();

        match NativeTransaction::parse(txn) {
            NativeTransaction::Nop {
                sender,
                sequence_number,
            } => {
                self.check_and_set_sequence_number(
                    sender,
                    sequence_number,
                    view,
                    &mut resource_write_set,
                )?;
                self.withdraw_fa_apt_from_signer(
                    sender,
                    0,
                    view,
                    gas,
                    &mut resource_write_set,
                    &mut events,
                )?;
            },
            NativeTransaction::FaTransfer {
                sender,
                sequence_number,
                recipient,
                amount,
            } => {
                self.check_and_set_sequence_number(
                    sender,
                    sequence_number,
                    view,
                    &mut resource_write_set,
                )?;
                self.withdraw_fa_apt_from_signer(
                    sender,
                    amount,
                    view,
                    gas,
                    &mut resource_write_set,
                    &mut events,
                )?;
                self.deposit_fa_apt(
                    recipient,
                    amount,
                    view,
                    gas,
                    &mut resource_write_set,
                    &mut events,
                )?;
            },
            NativeTransaction::Transfer {
                sender,
                sequence_number,
                recipient,
                amount,
                fail_on_account_existing,
                fail_on_account_missing,
            } => {
                if !fa_migration_complete {
                    panic!("!fa_migration_complete");
                    // return Err(());
                }
                self.check_and_set_sequence_number(
                    sender,
                    sequence_number,
                    view,
                    &mut resource_write_set,
                )?;
                self.withdraw_fa_apt_from_signer(
                    sender,
                    amount,
                    view,
                    gas,
                    &mut resource_write_set,
                    &mut events,
                )?;

                if !self.deposit_fa_apt(
                    recipient,
                    amount,
                    view,
                    gas,
                    &mut resource_write_set,
                    &mut events,
                )? {
                    self.check_or_create_account(
                        recipient,
                        fail_on_account_existing,
                        fail_on_account_missing,
                        view,
                        &mut resource_write_set,
                    )?;
                }
            },
            NativeTransaction::BatchTransfer { .. } => {
                todo!("to implement");
            },
        };

        Ok(VMChangeSet::new(
            resource_write_set,
            events,
            delayed_field_change_set,
            aggregator_v1_write_set,
            aggregator_v1_delta_set,
        ))
    }

    pub fn get_value<T: DeserializeOwned>(
        state_key: &StateKey,
        view: &(impl ExecutorView + ResourceGroupView),
    ) -> Result<Option<(T, StateValueMetadata)>, ()> {
        view.get_resource_state_value(state_key, None)
            .map_err(hide_error)?
            .map(|value| {
                bcs::from_bytes::<T>(value.bytes()).map(|bytes| (bytes, value.into_metadata()))
            })
            .transpose()
            .map_err(hide_error)
    }

    pub fn get_value_from_group<T: DeserializeOwned>(
        group_key: &StateKey,
        resource_tag: &StructTag,
        view: &(impl ExecutorView + ResourceGroupView),
    ) -> Result<Option<T>, ()> {
        view.get_resource_from_group(group_key, resource_tag, None)
            .map_err(hide_error)?
            .map(|value| bcs::from_bytes::<T>(&value))
            .transpose()
            .map_err(hide_error)
    }

    fn check_and_set_sequence_number(
        &self,
        sender_address: AccountAddress,
        sequence_number: u64,
        view: &(impl ExecutorView + ResourceGroupView),
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
    ) -> Result<(), ()> {
        let sender_account_key = self.db_util.new_state_key_account(&sender_address);

        let value = Self::get_value::<AccountResource>(&sender_account_key, view)?;

        match value {
            Some((mut account, metadata)) => {
                if sequence_number == account.sequence_number {
                    account.sequence_number += 1;
                    resource_write_set.insert(
                        sender_account_key,
                        AbstractResourceWriteOp::Write(WriteOp::Modification {
                            data: Bytes::from(bcs::to_bytes(&account).map_err(hide_error)?),
                            metadata,
                        }),
                    );
                    Ok(())
                } else {
                    error!(
                        "Invalid sequence number: txn: {} vs account: {}",
                        sequence_number, account.sequence_number
                    );
                    Err(())
                }
            },
            None => {
                error!("Account doesn't exist");
                Err(())
            },
        }
    }

    fn check_or_create_account(
        &self,
        address: AccountAddress,
        fail_on_account_existing: bool,
        fail_on_account_missing: bool,
        view: &(impl ExecutorView + ResourceGroupView),
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
    ) -> Result<(), ()> {
        let account_key = self.db_util.new_state_key_account(&address);

        let value = Self::get_value::<AccountResource>(&account_key, view)?;
        match value {
            Some(_) => {
                if fail_on_account_existing {
                    return Err(());
                }
            },
            None => {
                if fail_on_account_missing {
                    return Err(());
                } else {
                    let account = DbAccessUtil::new_account_resource(address);

                    resource_write_set.insert(
                        account_key,
                        AbstractResourceWriteOp::Write(WriteOp::legacy_creation(Bytes::from(
                            bcs::to_bytes(&account).map_err(hide_error)?,
                        ))),
                    );
                }
            },
        }

        Ok(())
    }

    fn withdraw_fa_apt_from_signer(
        &self,
        sender_address: AccountAddress,
        transfer_amount: u64,
        view: &(impl ExecutorView + ResourceGroupView),
        gas: u64,
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
        events: &mut Vec<(ContractEvent, Option<MoveTypeLayout>)>,
    ) -> Result<(), ()> {
        let sender_store_address = primary_apt_store(sender_address);
        let sender_fa_store_object_key = self
            .db_util
            .new_state_key_object_resource_group(&sender_store_address);
        let fungible_store_rg_tag = FungibleStoreResource::struct_tag();

        match Self::get_value_from_group::<FungibleStoreResource>(
            &sender_fa_store_object_key,
            &fungible_store_rg_tag,
            view,
        )? {
            Some(mut fa_store) => {
                if fa_store.balance >= transfer_amount + gas {
                    fa_store.balance -= transfer_amount + gas;
                    let fa_store_write = Self::create_single_resource_in_group_modification(
                        &fa_store,
                        &sender_fa_store_object_key,
                        fungible_store_rg_tag,
                        view,
                    )?;
                    resource_write_set.insert(sender_fa_store_object_key, fa_store_write);

                    if transfer_amount > 0 {
                        events.push((
                            WithdrawFAEvent {
                                store: sender_store_address,
                                amount: transfer_amount,
                            }
                            .create_event_v2(),
                            None,
                        ));
                    }
                    events.push((
                        WithdrawFAEvent {
                            store: sender_store_address,
                            amount: gas,
                        }
                        .create_event_v2(),
                        None,
                    ));
                    Ok(())
                } else {
                    Err(())
                }
            },
            None => Err(()),
        }
    }

    fn deposit_fa_apt(
        &self,
        recipient_address: AccountAddress,
        transfer_amount: u64,
        view: &(impl ExecutorView + ResourceGroupView),
        gas: u64,
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
        events: &mut Vec<(ContractEvent, Option<MoveTypeLayout>)>,
    ) -> Result<bool, ()> {
        let recipient_store_address = primary_apt_store(recipient_address);
        let recipient_fa_store_object_key = self
            .db_util
            .new_state_key_object_resource_group(&recipient_store_address);
        let fungible_store_rg_tag = FungibleStoreResource::struct_tag();

        match Self::get_value_from_group::<FungibleStoreResource>(
            &recipient_fa_store_object_key,
            &fungible_store_rg_tag,
            view,
        )? {
            Some(mut fa_store) => {
                fa_store.balance += transfer_amount + gas;
                let fa_store_write = Self::create_single_resource_in_group_modification(
                    &fa_store,
                    &recipient_fa_store_object_key,
                    fungible_store_rg_tag,
                    view,
                )?;
                resource_write_set.insert(recipient_fa_store_object_key, fa_store_write);

                events.push((
                    DepositFAEvent {
                        store: recipient_store_address,
                        amount: transfer_amount,
                    }
                    .create_event_v2(),
                    None,
                ));
                Ok(true)
            },
            None => {
                let receipeint_fa_store =
                    FungibleStoreResource::new(AccountAddress::TEN, transfer_amount, false);
                let fa_store_write = Self::create_single_resource_in_group_creation(
                    &receipeint_fa_store,
                    &recipient_fa_store_object_key,
                    fungible_store_rg_tag,
                    view,
                )?;
                resource_write_set.insert(recipient_fa_store_object_key, fa_store_write);

                events.push((
                    DepositFAEvent {
                        store: recipient_store_address,
                        amount: transfer_amount,
                    }
                    .create_event_v2(),
                    None,
                ));
                Ok(false)
            },
        }
    }

    fn create_single_resource_in_group_modification<T: Serialize>(
        value: &T,
        group_key: &StateKey,
        resource_tag: StructTag,
        view: &(impl ExecutorView + ResourceGroupView),
    ) -> Result<AbstractResourceWriteOp, ()> {
        let metadata = view
            .get_resource_state_value_metadata(group_key)
            .map_err(hide_error)?
            .unwrap();
        let size = view.resource_group_size(group_key).map_err(hide_error)?;
        let value_bytes = Bytes::from(bcs::to_bytes(value).map_err(hide_error)?);
        let group_write = AbstractResourceWriteOp::WriteResourceGroup(GroupWrite::new(
            WriteOp::Modification {
                data: Bytes::new(),
                metadata,
            },
            BTreeMap::from([(
                resource_tag,
                (WriteOp::legacy_modification(value_bytes), None),
            )]),
            size,
            size.get(),
        ));
        Ok(group_write)
    }

    fn create_single_resource_in_group_creation<T: Serialize>(
        value: &T,
        group_key: &StateKey,
        resource_tag: StructTag,
        view: &(impl ExecutorView + ResourceGroupView),
    ) -> Result<AbstractResourceWriteOp, ()> {
        let size = view.resource_group_size(group_key).map_err(hide_error)?;
        assert_eq!(size.get(), 0);
        let value_bytes = Bytes::from(bcs::to_bytes(value).map_err(hide_error)?);
        let new_size = ResourceGroupSize::Combined {
            num_tagged_resources: 1,
            all_tagged_resources_size: group_tagged_resource_size(&resource_tag, value_bytes.len())
                .map_err(hide_error)?,
        };
        let group_write = AbstractResourceWriteOp::WriteResourceGroup(GroupWrite::new(
            WriteOp::legacy_creation(Bytes::new()),
            BTreeMap::from([(resource_tag, (WriteOp::legacy_creation(value_bytes), None))]),
            new_size,
            size.get(),
        ));
        Ok(group_write)
    }
}

fn hide_error<E: Debug>(e: E) {
    error!("encountered error {:?}, hiding", e);
}
