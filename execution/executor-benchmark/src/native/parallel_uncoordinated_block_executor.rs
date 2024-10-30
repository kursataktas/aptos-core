// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db_access::DbAccessUtil,
    metrics::TIMER,
    native::{native_config::NATIVE_EXECUTOR_POOL, native_transaction::NativeTransaction},
};
use anyhow::Result;
use aptos_block_executor::counters::BLOCK_EXECUTOR_INNER_EXECUTE_BLOCK;
use aptos_types::{
    account_address::AccountAddress,
    account_config::{
        deposit::DepositEvent, primary_apt_store, withdraw::WithdrawEvent, AccountResource,
        DepositFAEvent, FungibleStoreResource, WithdrawFAEvent,
    },
    block_executor::config::BlockExecutorConfigFromOnchain,
    contract_event::ContractEvent,
    event::EventKey,
    fee_statement::FeeStatement,
    move_utils::move_event_v2::MoveEventV2Type,
    on_chain_config::{FeatureFlag, Features, OnChainConfig},
    state_store::{state_key::StateKey, StateView},
    transaction::{
        signature_verified_transaction::SignatureVerifiedTransaction, BlockOutput, ExecutionStatus,
        TransactionAuxiliaryData, TransactionOutput, TransactionStatus,
    },
    vm_status::{AbortLocation, StatusCode, VMStatus},
    write_set::{WriteOp, WriteSetMut},
};
use aptos_vm::VMBlockExecutor;
use dashmap::DashMap;
use move_core_types::{
    ident_str,
    language_storage::{ModuleId, TypeTag},
    move_resource::MoveStructType,
};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::collections::{BTreeMap, HashMap};

/// Executes transactions fully, and produces TransactionOutput (with final WriteSet)
/// (unlike execution within BlockSTM that produces non-materialized VMChangeSet)
pub trait RawTransactionExecutor: Sync {
    type BlockState: Sync;

    fn new() -> Self;

    fn init_block_state(&self, state_view: &(impl StateView + Sync)) -> Self::BlockState;

    fn execute_transaction(
        &self,
        txn: NativeTransaction,
        state_view: &(impl StateView + Sync),
        block_state: &Self::BlockState,
    ) -> Result<TransactionOutput>;
}

pub struct NativeParallelUncoordinatedBlockExecutor<E: RawTransactionExecutor + Sync + Send> {
    executor: E,
}

impl<E: RawTransactionExecutor + Sync + Send> VMBlockExecutor
    for NativeParallelUncoordinatedBlockExecutor<E>
{
    fn new() -> Self {
        Self { executor: E::new() }
    }

    fn execute_block(
        &self,
        transactions: &[SignatureVerifiedTransaction],
        state_view: &(impl StateView + Sync),
        _onchain_config: BlockExecutorConfigFromOnchain,
    ) -> Result<BlockOutput<TransactionOutput>, VMStatus> {
        let native_transactions = NATIVE_EXECUTOR_POOL.install(|| {
            transactions
                .par_iter()
                .map(NativeTransaction::parse)
                .collect::<Vec<_>>()
        });

        let _timer = BLOCK_EXECUTOR_INNER_EXECUTE_BLOCK.start_timer();

        let state = self.executor.init_block_state(state_view);

        let transaction_outputs = NATIVE_EXECUTOR_POOL
            .install(|| {
                native_transactions
                    .into_par_iter()
                    .map(|txn| self.executor.execute_transaction(txn, state_view, &state))
                    .collect::<Result<Vec<_>>>()
            })
            .map_err(|e| {
                VMStatus::error(
                    StatusCode::DELAYED_FIELD_OR_BLOCKSTM_CODE_INVARIANT_ERROR,
                    Some(format!("{:?}", e).to_string()),
                )
            })?;

        Ok(BlockOutput::new(transaction_outputs, None))
    }
}

struct IncrementalOutput {
    write_set: Vec<(StateKey, WriteOp)>,
    events: Vec<ContractEvent>,
}

impl IncrementalOutput {
    fn new() -> Self {
        IncrementalOutput {
            write_set: vec![],
            events: vec![],
        }
    }

    fn into_success_output(mut self, gas: u64) -> Result<TransactionOutput> {
        self.events
            .push(FeeStatement::new(gas, gas, 0, 0, 0).create_event_v2());

        Ok(TransactionOutput::new(
            WriteSetMut::new(self.write_set).freeze()?,
            self.events,
            /*gas_used=*/ gas,
            TransactionStatus::Keep(ExecutionStatus::Success),
            TransactionAuxiliaryData::default(),
        ))
    }

    fn append(&mut self, mut other: IncrementalOutput) {
        self.write_set.append(&mut other.write_set);
        self.events.append(&mut other.events);
    }

    fn to_abort(status: TransactionStatus) -> TransactionOutput {
        TransactionOutput::new(
            Default::default(),
            vec![],
            0,
            status,
            TransactionAuxiliaryData::default(),
        )
    }
}

#[macro_export]
macro_rules! merge_output {
    ($output:ident, $new_output:expr) => {
        match $new_output {
            Ok(new_output) => {
                $output.append(new_output);
            },
            Err(status) => return Ok(IncrementalOutput::to_abort(status)),
        }
    };
}

#[macro_export]
macro_rules! merge_output_in_partial {
    ($output:ident, $new_output:expr) => {
        match $new_output {
            Ok(new_output) => {
                $output.append(new_output);
            },
            Err(status) => return Ok(Err(status)),
        }
    };
}

pub struct NativeRawTransactionExecutor {
    db_util: DbAccessUtil,
}

impl RawTransactionExecutor for NativeRawTransactionExecutor {
    type BlockState = (bool, bool);

    fn new() -> Self {
        Self {
            db_util: DbAccessUtil::new(),
        }
    }

    fn init_block_state(&self, state_view: &(impl StateView + Sync)) -> Self::BlockState {
        let features = Features::fetch_config(&state_view).unwrap_or_default();
        let fa_migration_complete =
            features.is_enabled(FeatureFlag::OPERATIONS_DEFAULT_TO_FA_APT_STORE);
        let new_accounts_default_to_fa =
            features.is_enabled(FeatureFlag::NEW_ACCOUNTS_DEFAULT_TO_FA_APT_STORE);

        (fa_migration_complete, new_accounts_default_to_fa)
    }

    fn execute_transaction(
        &self,
        txn: NativeTransaction,
        state_view: &(impl StateView + Sync),
        block_state: &(bool, bool),
    ) -> Result<TransactionOutput> {
        let (fa_migration_complete, new_accounts_default_to_fa) = *block_state;

        match txn {
            NativeTransaction::Nop {
                sender,
                sequence_number: _,
            } => self.handle_nop(sender, fa_migration_complete, state_view),
            NativeTransaction::FaTransfer {
                sender,
                sequence_number: _,
                recipient,
                amount,
            } => self.handle_fa_transfer(sender, recipient, amount, state_view),
            NativeTransaction::Transfer {
                sender,
                sequence_number: _,
                recipient,
                amount,
                fail_on_account_existing,
                fail_on_account_missing,
            } => self.handle_account_creation_and_transfer(
                sender,
                recipient,
                amount,
                fail_on_account_existing,
                fail_on_account_missing,
                state_view,
                new_accounts_default_to_fa,
            ),
            NativeTransaction::BatchTransfer {
                sender,
                sequence_number: _,
                recipients,
                amounts,
                fail_on_account_existing,
                fail_on_account_missing,
            } => self.handle_batch_account_creation_and_transfer(
                sender,
                recipients,
                amounts,
                fail_on_account_existing,
                fail_on_account_missing,
                state_view,
                new_accounts_default_to_fa,
            ),
        }
    }
}

impl NativeRawTransactionExecutor {
    fn increment_sequence_number(
        &self,
        sender_address: AccountAddress,
        state_view: &(impl StateView + Sync),
    ) -> Result<Result<IncrementalOutput, TransactionStatus>> {
        let sender_account_key = self.db_util.new_state_key_account(&sender_address);
        let mut sender_account = {
            let _timer = TIMER
                .with_label_values(&["read_sender_account"])
                .start_timer();
            DbAccessUtil::get_account(&sender_account_key, state_view)?.unwrap()
        };

        sender_account.sequence_number += 1;

        let write_set = vec![(
            sender_account_key,
            WriteOp::legacy_modification(bcs::to_bytes(&sender_account)?.into()),
        )];
        let events = vec![];
        Ok(Ok(IncrementalOutput { write_set, events }))
    }

    // add total supply via aggregators?
    // let mut total_supply: u128 =
    //     DbAccessUtil::get_value(&TOTAL_SUPPLY_STATE_KEY, state_view)?.unwrap();
    // total_supply -= gas as u128;
    // (
    //     TOTAL_SUPPLY_STATE_KEY.clone(),
    //     WriteOp::legacy_modification(bcs::to_bytes(&total_supply)?),
    // ),

    fn withdraw_fa_apt_from_signer(
        &self,
        sender_address: AccountAddress,
        transfer_amount: u64,
        state_view: &(impl StateView + Sync),
        gas: u64,
    ) -> Result<Result<IncrementalOutput, TransactionStatus>> {
        let sender_store_address = primary_apt_store(sender_address);

        let sender_fa_store_object_key = self
            .db_util
            .new_state_key_object_resource_group(&sender_store_address);
        let mut sender_fa_store_object = {
            let _timer = TIMER
                .with_label_values(&["read_sender_fa_store"])
                .start_timer();
            match DbAccessUtil::get_resource_group(&sender_fa_store_object_key, state_view)? {
                Some(sender_fa_store_object) => sender_fa_store_object,
                None => {
                    return Ok(Err(TransactionStatus::Keep(ExecutionStatus::MoveAbort {
                        location: AbortLocation::Module(ModuleId::new(
                            AccountAddress::ONE,
                            ident_str!("fungible_asset").into(),
                        )),
                        code: 7,
                        info: None,
                    })))
                },
            }
        };

        let fungible_store_rg_tag = &self.db_util.common.fungible_store;
        let mut sender_fa_store = bcs::from_bytes::<FungibleStoreResource>(
            &sender_fa_store_object
                .remove(fungible_store_rg_tag)
                .unwrap(),
        )?;

        sender_fa_store.balance -= transfer_amount + gas;

        sender_fa_store_object.insert(
            fungible_store_rg_tag.clone(),
            bcs::to_bytes(&sender_fa_store)?,
        );

        let write_set = vec![(
            sender_fa_store_object_key,
            WriteOp::legacy_modification(bcs::to_bytes(&sender_fa_store_object)?.into()),
        )];

        let mut events = Vec::new();
        if transfer_amount > 0 {
            events.push(
                WithdrawFAEvent {
                    store: sender_store_address,
                    amount: transfer_amount,
                }
                .create_event_v2(),
            );
        }

        events.push(
            WithdrawFAEvent {
                store: sender_store_address,
                amount: gas,
            }
            .create_event_v2(),
        );
        Ok(Ok(IncrementalOutput { write_set, events }))
    }

    fn deposit_fa_apt(
        &self,
        recipient_address: AccountAddress,
        transfer_amount: u64,
        state_view: &(impl StateView + Sync),
    ) -> Result<Result<IncrementalOutput, TransactionStatus>> {
        let recipient_store_address = primary_apt_store(recipient_address);
        let recipient_fa_store_object_key = self
            .db_util
            .new_state_key_object_resource_group(&recipient_store_address);
        let fungible_store_rg_tag = &self.db_util.common.fungible_store;

        let (recipient_fa_store, mut recipient_fa_store_object, recipient_fa_store_existed) = {
            let _timer = TIMER
                .with_label_values(&["read_recipient_fa_store"])
                .start_timer();
            match DbAccessUtil::get_resource_group(&recipient_fa_store_object_key, state_view)? {
                Some(mut recipient_fa_store_object) => {
                    let mut recipient_fa_store = bcs::from_bytes::<FungibleStoreResource>(
                        &recipient_fa_store_object
                            .remove(fungible_store_rg_tag)
                            .unwrap(),
                    )?;
                    recipient_fa_store.balance += transfer_amount;
                    (recipient_fa_store, recipient_fa_store_object, true)
                },
                None => {
                    let receipeint_fa_store =
                        FungibleStoreResource::new(AccountAddress::TEN, transfer_amount, false);
                    let receipeint_fa_store_object = BTreeMap::new();
                    (receipeint_fa_store, receipeint_fa_store_object, false)
                },
            }
        };

        recipient_fa_store_object.insert(
            fungible_store_rg_tag.clone(),
            bcs::to_bytes(&recipient_fa_store)?,
        );

        let write_set = vec![(
            recipient_fa_store_object_key,
            if recipient_fa_store_existed {
                WriteOp::legacy_modification(bcs::to_bytes(&recipient_fa_store_object)?.into())
            } else {
                WriteOp::legacy_creation(bcs::to_bytes(&recipient_fa_store_object)?.into())
            },
        )];

        let event = DepositFAEvent {
            store: recipient_store_address,
            amount: transfer_amount,
        };

        let events = vec![event.create_event_v2()];
        Ok(Ok(IncrementalOutput { write_set, events }))
    }

    fn withdraw_coin_apt_from_signer(
        &self,
        sender_address: AccountAddress,
        transfer_amount: u64,
        state_view: &(impl StateView + Sync),
        gas: u64,
    ) -> Result<Result<IncrementalOutput, TransactionStatus>> {
        let sender_coin_store_key = self.db_util.new_state_key_aptos_coin(&sender_address);
        let sender_coin_store_opt = {
            let _timer = TIMER
                .with_label_values(&["read_sender_coin_store"])
                .start_timer();
            DbAccessUtil::get_apt_coin_store(&sender_coin_store_key, state_view)?
        };
        let mut sender_coin_store = match sender_coin_store_opt {
            None => {
                return self.withdraw_fa_apt_from_signer(
                    sender_address,
                    transfer_amount,
                    state_view,
                    gas,
                )
            },
            Some(sender_coin_store) => sender_coin_store,
        };

        sender_coin_store.set_coin(sender_coin_store.coin() - transfer_amount - gas);

        let write_set = vec![(
            sender_coin_store_key,
            WriteOp::legacy_modification(bcs::to_bytes(&sender_coin_store)?.into()),
        )];

        // TODO(grao): Some values are fake, because I'm lazy.
        let events = vec![ContractEvent::new_v1(
            EventKey::new(0, sender_address),
            0,
            TypeTag::Struct(Box::new(WithdrawEvent::struct_tag())),
            sender_address.to_vec(),
        )];
        Ok(Ok(IncrementalOutput { write_set, events }))
    }

    fn create_non_existing_account(
        recipient_address: AccountAddress,
        recipient_account_key: StateKey,
    ) -> Result<Result<IncrementalOutput, TransactionStatus>> {
        let mut output = IncrementalOutput::new();

        let recipient_account = DbAccessUtil::new_account_resource(recipient_address);
        output.write_set.push((
            recipient_account_key,
            WriteOp::legacy_creation(bcs::to_bytes(&recipient_account)?.into()),
        ));

        Ok(Ok(output))
    }

    fn deposit_coin_apt(
        &self,
        recipient_address: AccountAddress,
        transfer_amount: u64,
        fail_on_existing: bool,
        fail_on_missing: bool,
        state_view: &(impl StateView + Sync),
        new_accounts_default_to_fa: bool,
    ) -> Result<Result<IncrementalOutput, TransactionStatus>> {
        let recipient_account_key = self.db_util.new_state_key_account(&recipient_address);
        let recipient_coin_store_key = self.db_util.new_state_key_aptos_coin(&recipient_address);

        let recipient_account = {
            let _timer = TIMER.with_label_values(&["read_new_account"]).start_timer();
            DbAccessUtil::get_account(&recipient_account_key, state_view)?
        };

        let mut output = IncrementalOutput::new();
        if recipient_account.is_some() {
            if fail_on_existing {
                return Ok(Err(TransactionStatus::Keep(ExecutionStatus::MoveAbort {
                    location: AbortLocation::Module(ModuleId::new(
                        AccountAddress::ONE,
                        ident_str!("account").into(),
                    )),
                    code: 7,
                    info: None,
                })));
            }

            let mut recipient_coin_store = {
                let _timer = TIMER
                    .with_label_values(&["read_new_coin_store"])
                    .start_timer();
                DbAccessUtil::get_apt_coin_store(&recipient_coin_store_key, state_view)?.unwrap()
            };

            if transfer_amount != 0 {
                recipient_coin_store.set_coin(recipient_coin_store.coin() + transfer_amount);

                output.write_set.push((
                    recipient_coin_store_key,
                    WriteOp::legacy_modification(bcs::to_bytes(&recipient_coin_store)?.into()),
                ));
            }
        } else {
            if fail_on_missing {
                return Ok(Err(TransactionStatus::Keep(ExecutionStatus::MoveAbort {
                    location: AbortLocation::Module(ModuleId::new(
                        AccountAddress::ONE,
                        ident_str!("account").into(),
                    )),
                    code: 8,
                    info: None,
                })));
            }

            merge_output_in_partial!(
                output,
                Self::create_non_existing_account(recipient_address, recipient_account_key)?
            );

            if new_accounts_default_to_fa {
                merge_output_in_partial!(
                    output,
                    self.deposit_fa_apt(recipient_address, transfer_amount, state_view)?
                );
                return Ok(Ok(output));
            }

            {
                let _timer = TIMER
                    .with_label_values(&["read_new_coin_store"])
                    .start_timer();
                assert!(
                    DbAccessUtil::get_apt_coin_store(&recipient_coin_store_key, state_view)?
                        .is_none()
                );
            }

            let recipient_coin_store =
                DbAccessUtil::new_apt_coin_store(transfer_amount, recipient_address);

            output.write_set.push((
                recipient_coin_store_key,
                WriteOp::legacy_creation(bcs::to_bytes(&recipient_coin_store)?.into()),
            ));
        }

        output.events.push(
            ContractEvent::new_v1(
                EventKey::new(0, recipient_address),
                0,
                TypeTag::Struct(Box::new(DepositEvent::struct_tag())),
                recipient_address.to_vec(),
            ), // TODO(grao): CoinRegisterEvent
        );
        Ok(Ok(output))
    }

    fn handle_fa_transfer(
        &self,
        sender_address: AccountAddress,
        recipient_address: AccountAddress,
        transfer_amount: u64,
        state_view: &(impl StateView + Sync),
    ) -> Result<TransactionOutput> {
        let _timer = TIMER.with_label_values(&["fa_transfer"]).start_timer();

        let gas = 500; // hardcode gas consumed.

        let mut output = IncrementalOutput::new();

        merge_output!(
            output,
            self.increment_sequence_number(sender_address, state_view)?
        );
        merge_output!(
            output,
            self.withdraw_fa_apt_from_signer(sender_address, transfer_amount, state_view, gas)?
        );

        merge_output!(
            output,
            self.deposit_fa_apt(recipient_address, transfer_amount, state_view,)?
        );

        output.into_success_output(gas)
    }

    fn handle_account_creation_and_transfer(
        &self,
        sender_address: AccountAddress,
        recipient_address: AccountAddress,
        transfer_amount: u64,
        fail_on_existing: bool,
        fail_on_missing: bool,
        state_view: &(impl StateView + Sync),
        new_accounts_default_to_fa: bool,
    ) -> Result<TransactionOutput> {
        let _timer = TIMER.with_label_values(&["account_creation"]).start_timer();

        let gas = 500; // hardcode gas consumed.

        let mut output = IncrementalOutput::new();
        merge_output!(
            output,
            self.increment_sequence_number(sender_address, state_view)?
        );
        merge_output!(
            output,
            self.withdraw_coin_apt_from_signer(sender_address, transfer_amount, state_view, gas)?
        );
        merge_output!(
            output,
            self.deposit_coin_apt(
                recipient_address,
                transfer_amount,
                fail_on_existing,
                fail_on_missing,
                state_view,
                new_accounts_default_to_fa,
            )?
        );

        output.into_success_output(gas)
    }

    fn handle_batch_account_creation_and_transfer(
        &self,
        sender_address: AccountAddress,
        recipient_addresses: Vec<AccountAddress>,
        transfer_amounts: Vec<u64>,
        fail_on_existing: bool,
        fail_on_missing: bool,
        state_view: &(impl StateView + Sync),
        new_accounts_default_to_fa: bool,
    ) -> Result<TransactionOutput> {
        let gas = 5000; // hardcode gas consumed.

        let mut deltas =
            compute_deltas_for_batch(recipient_addresses, transfer_amounts, sender_address);

        let amount_to_sender = -deltas.remove(&sender_address).unwrap_or(0);
        assert!(amount_to_sender >= 0);

        let mut output = IncrementalOutput::new();
        merge_output!(
            output,
            self.increment_sequence_number(sender_address, state_view)?
        );
        merge_output!(
            output,
            self.withdraw_coin_apt_from_signer(
                sender_address,
                amount_to_sender as u64,
                state_view,
                gas
            )?
        );

        for (recipient_address, transfer_amount) in deltas.into_iter() {
            merge_output!(
                output,
                self.deposit_coin_apt(
                    recipient_address,
                    transfer_amount as u64,
                    fail_on_existing,
                    fail_on_missing,
                    state_view,
                    new_accounts_default_to_fa,
                )?
            );
        }

        output.into_success_output(gas)
    }

    fn handle_nop(
        &self,
        sender_address: AccountAddress,
        fa_migration_complete: bool,
        state_view: &(impl StateView + Sync),
    ) -> Result<TransactionOutput> {
        let _timer = TIMER.with_label_values(&["nop"]).start_timer();

        let gas = 4; // hardcode gas consumed.

        let mut output = IncrementalOutput::new();

        merge_output!(
            output,
            self.increment_sequence_number(sender_address, state_view)?
        );
        if fa_migration_complete {
            merge_output!(
                output,
                self.withdraw_fa_apt_from_signer(sender_address, 0, state_view, gas)?
            );
        } else {
            merge_output!(
                output,
                self.withdraw_coin_apt_from_signer(sender_address, 0, state_view, gas)?
            );
        }
        output.into_success_output(gas)
    }
}

fn compute_deltas_for_batch(
    recipient_addresses: Vec<AccountAddress>,
    transfer_amounts: Vec<u64>,
    sender_address: AccountAddress,
) -> HashMap<AccountAddress, i64> {
    let mut deltas = HashMap::new();
    for (recipient, amount) in recipient_addresses
        .into_iter()
        .zip(transfer_amounts.into_iter())
    {
        let amount = amount as i64;
        deltas
            .entry(recipient)
            .and_modify(|counter| *counter += amount)
            .or_insert(amount);
        deltas
            .entry(sender_address)
            .and_modify(|counter| *counter -= amount)
            .or_insert(-amount);
    }
    deltas
}
enum CachedResource {
    Account(AccountResource),
    FungibleStore(FungibleStoreResource),
}

pub struct NativeValueCacheRawTransactionExecutor {
    db_util: DbAccessUtil,
    cache: DashMap<StateKey, CachedResource>,
}

impl RawTransactionExecutor for NativeValueCacheRawTransactionExecutor {
    type BlockState = ();

    fn new() -> Self {
        Self {
            db_util: DbAccessUtil::new(),
            cache: DashMap::new(),
        }
    }

    fn init_block_state(&self, _state_view: &(impl StateView + Sync)) {}

    fn execute_transaction(
        &self,
        txn: NativeTransaction,
        state_view: &(impl StateView + Sync),
        _block_state: &(),
    ) -> Result<TransactionOutput> {
        let gas_units = 4;
        let gas = gas_units * 100;

        match txn {
            NativeTransaction::Nop {
                sender,
                sequence_number,
            } => {
                self.update_sequence_number(sender, state_view, sequence_number);
                self.update_fa_balance(sender, state_view, 0, gas, true);
            },
            NativeTransaction::FaTransfer {
                sender,
                sequence_number,
                recipient,
                amount,
            } => {
                self.update_sequence_number(sender, state_view, sequence_number);
                self.update_fa_balance(sender, state_view, 0, gas + amount, true);
                self.update_fa_balance(recipient, state_view, amount, 0, false);
            },
            NativeTransaction::Transfer {
                sender,
                sequence_number,
                recipient,
                amount,
                fail_on_account_existing,
                fail_on_account_missing,
            } => {
                self.update_sequence_number(sender, state_view, sequence_number);
                self.update_fa_balance(sender, state_view, 0, gas + amount, true);

                if !self.update_fa_balance(
                    recipient,
                    state_view,
                    amount,
                    0,
                    fail_on_account_missing,
                ) {
                    self.check_or_create_account(
                        recipient,
                        state_view,
                        fail_on_account_existing,
                        fail_on_account_missing,
                    );
                }
            },
            NativeTransaction::BatchTransfer { .. } => {
                todo!("")
            },
        }
        Ok(TransactionOutput::new(
            Default::default(),
            vec![],
            0,
            TransactionStatus::Keep(ExecutionStatus::Success),
            TransactionAuxiliaryData::default(),
        ))
    }
}

impl NativeValueCacheRawTransactionExecutor {
    fn update_sequence_number(
        &self,
        sender: AccountAddress,
        state_view: &(impl StateView + Sync),
        sequence_number: u64,
    ) {
        let sender_account_key = self.db_util.new_state_key_account(&sender);
        match self
            .cache
            .entry(sender_account_key.clone())
            .or_insert_with(|| {
                CachedResource::Account(
                    DbAccessUtil::get_account(&sender_account_key, state_view)
                        .unwrap()
                        .unwrap(),
                )
            })
            .value_mut()
        {
            CachedResource::Account(account) => account.sequence_number = sequence_number,
            CachedResource::FungibleStore(_) => panic!("wrong type"),
        };
    }

    fn check_or_create_account(
        &self,
        sender: AccountAddress,
        state_view: &(impl StateView + Sync),
        fail_on_existing: bool,
        fail_on_missing: bool,
    ) {
        let sender_account_key = self.db_util.new_state_key_account(&sender);
        let mut missing = false;
        self.cache
            .entry(sender_account_key.clone())
            .or_insert_with(|| {
                CachedResource::Account(
                    match DbAccessUtil::get_account(&sender_account_key, state_view).unwrap() {
                        Some(account) => account,
                        None => {
                            missing = true;
                            assert!(!fail_on_missing);
                            DbAccessUtil::new_account_resource(sender)
                        },
                    },
                )
            });
        if fail_on_existing {
            assert!(missing);
        }
    }

    fn update_fa_balance(
        &self,
        sender: AccountAddress,
        state_view: &(impl StateView + Sync),
        increment: u64,
        decrement: u64,
        fail_on_missing: bool,
    ) -> bool {
        let sender_store_address = primary_apt_store(sender);
        let fungible_store_rg_tag = &self.db_util.common.fungible_store;
        let cache_key = StateKey::resource(&sender_store_address, fungible_store_rg_tag).unwrap();

        let mut exists = false;
        let mut entry = self.cache.entry(cache_key).or_insert_with(|| {
            let sender_fa_store_object_key = self
                .db_util
                .new_state_key_object_resource_group(&sender_store_address);
            let rg_opt =
                DbAccessUtil::get_resource_group(&sender_fa_store_object_key, state_view).unwrap();
            CachedResource::FungibleStore(match rg_opt {
                Some(mut rg) => {
                    exists = true;
                    bcs::from_bytes(&rg.remove(fungible_store_rg_tag).unwrap()).unwrap()
                },
                None => {
                    assert!(!fail_on_missing);
                    FungibleStoreResource::new(AccountAddress::TEN, 0, false)
                },
            })
        });
        match entry.value_mut() {
            CachedResource::FungibleStore(fungible_store_resource) => {
                fungible_store_resource.balance += increment;
                fungible_store_resource.balance -= decrement;
            },
            CachedResource::Account(_) => panic!("wrong type"),
        };
        exists
    }
}

pub struct NativeNoStorageRawTransactionExecutor {
    seq_nums: DashMap<AccountAddress, u64>,
    balances: DashMap<AccountAddress, u64>,
}

impl RawTransactionExecutor for NativeNoStorageRawTransactionExecutor {
    type BlockState = ();

    fn new() -> Self {
        Self {
            seq_nums: DashMap::new(),
            balances: DashMap::new(),
        }
    }

    fn init_block_state(&self, _state_view: &(impl StateView + Sync)) {}

    fn execute_transaction(
        &self,
        txn: NativeTransaction,
        _state_view: &(impl StateView + Sync),
        _block_state: &(),
    ) -> Result<TransactionOutput> {
        let gas_units = 4;
        let gas = gas_units * 100;
        match txn {
            NativeTransaction::Nop {
                sender,
                sequence_number,
            } => {
                self.seq_nums.insert(sender, sequence_number);
                *self
                    .balances
                    .entry(sender)
                    .or_insert(100_000_000_000_000_000) -= gas;
            },
            NativeTransaction::FaTransfer {
                sender,
                sequence_number,
                recipient,
                amount,
            }
            | NativeTransaction::Transfer {
                sender,
                sequence_number,
                recipient,
                amount,
                ..
            } => {
                self.seq_nums.insert(sender, sequence_number);
                *self
                    .balances
                    .entry(sender)
                    .or_insert(100_000_000_000_000_000) -= amount + gas;
                *self
                    .balances
                    .entry(recipient)
                    .or_insert(100_000_000_000_000_000) += amount;
            },
            NativeTransaction::BatchTransfer {
                sender,
                sequence_number,
                recipients,
                amounts,
                ..
            } => {
                self.seq_nums.insert(sender, sequence_number);

                let mut deltas = compute_deltas_for_batch(recipients, amounts, sender);

                let amount_from_sender = -deltas.remove(&sender).unwrap_or(0);
                assert!(amount_from_sender >= 0);

                *self
                    .balances
                    .entry(sender)
                    .or_insert(100_000_000_000_000_000) -= amount_from_sender as u64;

                for (recipient, amount) in deltas.into_iter() {
                    *self
                        .balances
                        .entry(recipient)
                        .or_insert(100_000_000_000_000_000) += amount as u64;
                }
            },
        }
        Ok(TransactionOutput::new(
            Default::default(),
            vec![],
            0,
            TransactionStatus::Keep(ExecutionStatus::Success),
            TransactionAuxiliaryData::default(),
        ))
    }
}
