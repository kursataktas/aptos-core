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
        primary_apt_store, AccountResource, CoinDeposit, CoinInfoResource, CoinRegister,
        CoinStoreResource, CoinWithdraw, ConcurrentSupplyResource, DepositFAEvent,
        FungibleStoreResource, TypeInfoResource, WithdrawFAEvent,
    },
    block_executor::config::BlockExecutorConfigFromOnchain,
    contract_event::ContractEvent,
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
    AptosCoinType,
};
use aptos_vm::VMBlockExecutor;
use dashmap::DashMap;
use move_core_types::{ident_str, language_storage::ModuleId};
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

pub struct NativeRawTransactionExecutor {
    db_util: DbAccessUtil,
}

impl RawTransactionExecutor for NativeRawTransactionExecutor {
    type BlockState = bool;

    fn new() -> Self {
        Self {
            db_util: DbAccessUtil::new(),
        }
    }

    fn init_block_state(&self, state_view: &(impl StateView + Sync)) -> bool {
        let features = Features::fetch_config(&state_view).unwrap_or_default();
        let fa_migration_complete =
            features.is_enabled(FeatureFlag::OPERATIONS_DEFAULT_TO_FA_APT_STORE);
        let new_accounts_default_to_fa =
            features.is_enabled(FeatureFlag::NEW_ACCOUNTS_DEFAULT_TO_FA_APT_STORE);
        assert_eq!(
            fa_migration_complete, new_accounts_default_to_fa,
            "native code only works with both flags either enabled or disabled"
        );

        fa_migration_complete
    }

    fn execute_transaction(
        &self,
        txn: NativeTransaction,
        state_view: &(impl StateView + Sync),
        block_state: &bool,
    ) -> Result<TransactionOutput> {
        let fa_migration_complete = *block_state;

        let gas_unit = 4; // hardcode gas consumed.
        let gas = gas_unit * 100;

        let mut output = match txn {
            NativeTransaction::Nop {
                sender,
                sequence_number: _,
            } => {
                let mut output = self.increment_sequence_number(sender, state_view)?;

                merge_output!(
                    output,
                    self.withdraw_apt_from_signer(
                        fa_migration_complete,
                        sender,
                        0,
                        state_view,
                        gas
                    )?
                );

                output
            },
            NativeTransaction::FaTransfer {
                sender,
                sequence_number: _,
                recipient,
                amount,
            } => {
                let mut output = self.increment_sequence_number(sender, state_view)?;

                merge_output!(
                    output,
                    self.withdraw_fa_apt_from_signer(sender, amount, state_view, gas)?
                );

                let (deposit_output, _existed) =
                    self.deposit_fa_apt(recipient, amount, state_view)?;
                output.append(deposit_output);
                output
            },
            NativeTransaction::Transfer {
                sender,
                sequence_number: _,
                recipient,
                amount,
                fail_on_recipient_account_existing,
                fail_on_recipient_account_missing,
            } => {
                let mut output = self.increment_sequence_number(sender, state_view)?;
                merge_output!(
                    output,
                    self.withdraw_apt_from_signer(
                        fa_migration_complete,
                        sender,
                        amount,
                        state_view,
                        gas
                    )?
                );

                let (deposit_output, existed) =
                    self.deposit_apt(fa_migration_complete, recipient, amount, state_view)?;
                output.append(deposit_output);

                if !existed || fail_on_recipient_account_existing {
                    merge_output!(
                        output,
                        self.check_or_create_account(
                            recipient,
                            fail_on_recipient_account_existing,
                            fail_on_recipient_account_missing,
                            state_view,
                        )?
                    );
                }
                output
            },
            NativeTransaction::BatchTransfer {
                sender,
                sequence_number: _,
                recipients,
                amounts,
                fail_on_recipient_account_existing: fail_on_account_existing,
                fail_on_recipient_account_missing: fail_on_account_missing,
            } => {
                let mut deltas = compute_deltas_for_batch(recipients, amounts, sender);

                let amount_to_sender = -deltas.remove(&sender).unwrap_or(0);
                assert!(amount_to_sender >= 0);

                let mut output = self.increment_sequence_number(sender, state_view)?;
                merge_output!(
                    output,
                    self.withdraw_apt_from_signer(
                        fa_migration_complete,
                        sender,
                        amount_to_sender as u64,
                        state_view,
                        gas
                    )?
                );

                for (recipient_address, transfer_amount) in deltas.into_iter() {
                    let (deposit_output, existed) = self.deposit_apt(
                        fa_migration_complete,
                        recipient_address,
                        transfer_amount as u64,
                        state_view,
                    )?;
                    output.append(deposit_output);

                    if !existed || fail_on_account_existing {
                        merge_output!(
                            output,
                            self.check_or_create_account(
                                recipient_address,
                                fail_on_account_existing,
                                fail_on_account_missing,
                                state_view,
                            )?
                        );
                    }
                }
                output
            },
        };

        output.append(self.reduce_apt_supply(fa_migration_complete, gas, state_view)?);

        output.into_success_output(gas)
    }
}

impl NativeRawTransactionExecutor {
    fn increment_sequence_number(
        &self,
        sender_address: AccountAddress,
        state_view: &(impl StateView + Sync),
    ) -> Result<IncrementalOutput> {
        let sender_account_key = self.db_util.new_state_key_account(&sender_address);
        let mut sender_account =
            DbAccessUtil::get_account(&sender_account_key, state_view)?.unwrap();

        sender_account.sequence_number += 1;

        let write_set = vec![(
            sender_account_key,
            WriteOp::legacy_modification(bcs::to_bytes(&sender_account)?.into()),
        )];
        let events = vec![];
        Ok(IncrementalOutput { write_set, events })
    }

    fn reduce_apt_supply(
        &self,
        fa_migration_complete: bool,
        gas: u64,
        state_view: &(impl StateView + Sync),
    ) -> Result<IncrementalOutput> {
        if fa_migration_complete {
            self.reduce_fa_apt_supply(gas, state_view)
        } else {
            self.reduce_coin_apt_supply(gas, state_view)
        }
    }

    fn reduce_fa_apt_supply(
        &self,
        gas: u64,
        state_view: &(impl StateView + Sync),
    ) -> Result<IncrementalOutput> {
        let apt_metadata_object_state_key = self
            .db_util
            .new_state_key_object_resource_group(&AccountAddress::TEN);

        let concurrent_supply_rg_tag = &self.db_util.common.concurrent_supply;

        let mut apt_metadata_object =
            DbAccessUtil::get_resource_group(&apt_metadata_object_state_key, state_view)?.unwrap();
        let mut supply = bcs::from_bytes::<ConcurrentSupplyResource>(
            &apt_metadata_object
                .remove(concurrent_supply_rg_tag)
                .unwrap(),
        )?;

        supply.current.set(supply.current.get() - gas as u128);

        apt_metadata_object.insert(concurrent_supply_rg_tag.clone(), bcs::to_bytes(&supply)?);

        let write_set = vec![(
            apt_metadata_object_state_key,
            WriteOp::legacy_modification(bcs::to_bytes(&apt_metadata_object)?.into()),
        )];

        Ok(IncrementalOutput {
            write_set,
            events: Vec::new(),
        })
    }

    fn reduce_coin_apt_supply(
        &self,
        gas: u64,
        state_view: &(impl StateView + Sync),
    ) -> Result<IncrementalOutput> {
        let sender_coin_store = DbAccessUtil::get_value::<CoinInfoResource<AptosCoinType>>(
            &self.db_util.common.apt_coin_info_resource,
            state_view,
        )?
        .ok_or_else(|| anyhow::anyhow!("no coin info"))?;

        let total_supply_state_key = sender_coin_store.supply_aggregator_state_key();
        let total_supply = DbAccessUtil::get_value::<u128>(&total_supply_state_key, state_view)?
            .ok_or_else(|| anyhow::anyhow!("no total supply"))?;

        let write_set = vec![(
            total_supply_state_key,
            WriteOp::legacy_modification(bcs::to_bytes(&(total_supply - gas as u128))?.into()),
        )];

        Ok(IncrementalOutput {
            write_set,
            events: vec![],
        })
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

    fn withdraw_apt_from_signer(
        &self,
        fa_migration_complete: bool,
        sender_address: AccountAddress,
        transfer_amount: u64,
        state_view: &(impl StateView + Sync),
        gas: u64,
    ) -> Result<Result<IncrementalOutput, TransactionStatus>> {
        if fa_migration_complete {
            self.withdraw_fa_apt_from_signer(sender_address, transfer_amount, state_view, gas)
        } else {
            self.withdraw_coin_apt_from_signer(sender_address, transfer_amount, state_view, gas)
        }
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

        let mut events = Vec::new();
        if transfer_amount != 0 {
            events.push(
                CoinWithdraw {
                    coin_type: self.db_util.common.apt_coin_type_name.clone(),
                    account: sender_address,
                    amount: transfer_amount,
                }
                .create_event_v2(),
            );
            // Coin doesn't emit Withdraw event for gas
        }

        let write_set = vec![(
            sender_coin_store_key,
            WriteOp::legacy_modification(bcs::to_bytes(&sender_coin_store)?.into()),
        )];

        Ok(Ok(IncrementalOutput { write_set, events }))
    }

    fn create_non_existing_account(
        recipient_address: AccountAddress,
        recipient_account_key: StateKey,
    ) -> Result<IncrementalOutput> {
        let mut output = IncrementalOutput::new();

        let recipient_account = DbAccessUtil::new_account_resource(recipient_address);
        output.write_set.push((
            recipient_account_key,
            WriteOp::legacy_creation(bcs::to_bytes(&recipient_account)?.into()),
        ));

        Ok(output)
    }

    fn deposit_apt(
        &self,
        fa_migration_complete: bool,
        recipient_address: AccountAddress,
        transfer_amount: u64,
        state_view: &(impl StateView + Sync),
    ) -> Result<(IncrementalOutput, bool)> {
        if fa_migration_complete {
            self.deposit_fa_apt(recipient_address, transfer_amount, state_view)
        } else {
            self.deposit_coin_apt(recipient_address, transfer_amount, state_view)
        }
    }

    fn deposit_fa_apt(
        &self,
        recipient_address: AccountAddress,
        transfer_amount: u64,
        state_view: &(impl StateView + Sync),
    ) -> Result<(IncrementalOutput, bool)> {
        let recipient_store_address = primary_apt_store(recipient_address);
        let recipient_fa_store_object_key = self
            .db_util
            .new_state_key_object_resource_group(&recipient_store_address);
        let fungible_store_rg_tag = &self.db_util.common.fungible_store;

        let (mut recipient_fa_store, mut recipient_fa_store_object, recipient_fa_store_existed) =
            match DbAccessUtil::get_resource_group(&recipient_fa_store_object_key, state_view)? {
                Some(mut recipient_fa_store_object) => {
                    let recipient_fa_store = bcs::from_bytes::<FungibleStoreResource>(
                        &recipient_fa_store_object
                            .remove(fungible_store_rg_tag)
                            .unwrap(),
                    )?;
                    (recipient_fa_store, recipient_fa_store_object, true)
                },
                None => {
                    let receipeint_fa_store =
                        FungibleStoreResource::new(AccountAddress::TEN, 0, false);
                    let receipeint_fa_store_object = BTreeMap::from([(
                        self.db_util.common.object_core.clone(),
                        bcs::to_bytes(&DbAccessUtil::new_object_core(
                            recipient_store_address,
                            recipient_address,
                        ))?,
                    )]);
                    (receipeint_fa_store, receipeint_fa_store_object, false)
                },
            };

        recipient_fa_store.balance += transfer_amount;

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

        let mut events = Vec::new();
        if transfer_amount != 0 {
            events.push(
                DepositFAEvent {
                    store: recipient_store_address,
                    amount: transfer_amount,
                }
                .create_event_v2(),
            )
        }

        Ok((
            IncrementalOutput { write_set, events },
            recipient_fa_store_existed,
        ))
    }

    fn deposit_coin_apt(
        &self,
        recipient_address: AccountAddress,
        transfer_amount: u64,
        state_view: &(impl StateView + Sync),
    ) -> Result<(IncrementalOutput, bool)> {
        let recipient_coin_store_key = self.db_util.new_state_key_aptos_coin(&recipient_address);

        let mut events = Vec::new();
        let (mut recipient_coin_store, recipient_coin_store_existed) =
            match DbAccessUtil::get_apt_coin_store(&recipient_coin_store_key, state_view)? {
                Some(recipient_coin_store) => (recipient_coin_store, true),
                None => {
                    events.push(
                        CoinRegister {
                            account: AccountAddress::ONE,
                            type_info: TypeInfoResource::new::<AptosCoinType>()?,
                        }
                        .create_event_v2(),
                    );
                    (
                        DbAccessUtil::new_apt_coin_store(0, recipient_address),
                        false,
                    )
                },
            };

        recipient_coin_store.set_coin(recipient_coin_store.coin() + transfer_amount);

        // first need to create events, to update the handle, and then serialize sender_coin_store
        if transfer_amount != 0 {
            events.push(
                CoinDeposit {
                    coin_type: self.db_util.common.apt_coin_type_name.clone(),
                    account: recipient_address,
                    amount: transfer_amount,
                }
                .create_event_v2(),
            );
        }

        let write_set = vec![(
            recipient_coin_store_key,
            if recipient_coin_store_existed {
                WriteOp::legacy_modification(bcs::to_bytes(&recipient_coin_store)?.into())
            } else {
                WriteOp::legacy_creation(bcs::to_bytes(&recipient_coin_store)?.into())
            },
        )];

        Ok((
            IncrementalOutput { write_set, events },
            recipient_coin_store_existed,
        ))
    }

    fn check_or_create_account(
        &self,
        address: AccountAddress,
        fail_on_account_existing: bool,
        fail_on_account_missing: bool,
        state_view: &(impl StateView + Sync),
    ) -> Result<Result<IncrementalOutput, TransactionStatus>> {
        let account_key = self.db_util.new_state_key_account(&address);
        match DbAccessUtil::get_account(&account_key, state_view)? {
            Some(_) => {
                if fail_on_account_existing {
                    return Ok(Err(TransactionStatus::Keep(ExecutionStatus::MoveAbort {
                        location: AbortLocation::Module(ModuleId::new(
                            AccountAddress::ONE,
                            ident_str!("account").into(),
                        )),
                        code: 7,
                        info: None,
                    })));
                }
            },
            None => {
                if fail_on_account_missing {
                    return Ok(Err(TransactionStatus::Keep(ExecutionStatus::MoveAbort {
                        location: AbortLocation::Module(ModuleId::new(
                            AccountAddress::ONE,
                            ident_str!("account").into(),
                        )),
                        code: 7,
                        info: None,
                    })));
                } else {
                    return Ok(Ok(Self::create_non_existing_account(address, account_key)?));
                }
            },
        }

        Ok(Ok(IncrementalOutput::new()))
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
    #[allow(dead_code)]
    AptCoinStore(CoinStoreResource<AptosCoinType>),
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
                fail_on_recipient_account_existing: fail_on_account_existing,
                fail_on_recipient_account_missing: fail_on_account_missing,
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
            CachedResource::FungibleStore(_) | CachedResource::AptCoinStore(_) => {
                panic!("wrong type")
            },
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
            CachedResource::Account(_) | CachedResource::AptCoinStore(_) => panic!("wrong type"),
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
