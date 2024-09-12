// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::db_indexer::DBIndexer;
use aptos_storage_interface::Result;
use aptos_types::{
    account_config::{
        AccountResource, CoinDeposit, CoinRegister, CoinRegisterEvent, CoinStoreResource,
        CoinWithdraw, DepositEvent, KeyRotation, KeyRotationEvent, WithdrawEvent,
        COIN_DEPOSIT_TYPE, COIN_REGISTER_EVENT_TYPE, COIN_REGISTER_TYPE, COIN_WITHDRAW_TYPE,
        DEPOSIT_EVENT_TYPE, KEY_ROTATION_EVENT_TYPE, KEY_ROTATION_TYPE, WITHDRAW_EVENT_TYPE,
    },
    contract_event::{ContractEventV1, ContractEventV2},
    event::EventKey,
    DummyCoinType,
};
use move_core_types::language_storage::{StructTag, TypeTag};
use std::{collections::HashMap, str::FromStr};

pub trait EventV2Translator: Send + Sync {
    fn translate_event_v2_to_v1(
        &self,
        v2: &ContractEventV2,
        db_indexer: &DBIndexer,
    ) -> Result<ContractEventV1>;
}

pub struct EventV2TranslationEngine {
    // Map from event type to translator
    pub translators: HashMap<TypeTag, Box<dyn EventV2Translator + Send + Sync>>,
}

impl EventV2TranslationEngine {
    pub fn new() -> Self {
        let mut translators: HashMap<TypeTag, Box<dyn EventV2Translator + Send + Sync>> =
            HashMap::new();
        translators.insert(COIN_DEPOSIT_TYPE.clone(), Box::new(CoinDepositTranslator));
        translators.insert(COIN_WITHDRAW_TYPE.clone(), Box::new(CoinWithdrawTranslator));
        translators.insert(COIN_REGISTER_TYPE.clone(), Box::new(CoinRegisterTranslator));
        translators.insert(KEY_ROTATION_TYPE.clone(), Box::new(KeyRotationTranslator));
        Self { translators }
    }
}

struct CoinDepositTranslator;
impl EventV2Translator for CoinDepositTranslator {
    fn translate_event_v2_to_v1(
        &self,
        v2: &ContractEventV2,
        db_indexer: &DBIndexer,
    ) -> Result<ContractEventV1> {
        let coin_deposit = CoinDeposit::try_from_bytes(v2.event_data())?;
        let struct_tag_str = format!("0x1::coin::CoinStore<{}>", coin_deposit.coin_type());
        let struct_tag = StructTag::from_str(&struct_tag_str)?;
        let (key, sequence_number) = if let Some(state_value) =
            db_indexer.get_state_value_for_resource(coin_deposit.account(), &struct_tag)?
        {
            // We can use `DummyCoinType` as it does not affect the correctness of deserialization.
            let coin_store_resource: CoinStoreResource<DummyCoinType> =
                bcs::from_bytes(state_value.bytes())?;
            let key = *coin_store_resource.deposit_events().key();
            let sequence_number = db_indexer
                .get_next_sequence_number(&key, coin_store_resource.deposit_events().count())?;
            (key, sequence_number)
        } else {
            (EventKey::new(2, coin_deposit.account().clone()), 0)
        };
        let deposit_event = DepositEvent::new(coin_deposit.amount());
        Ok(ContractEventV1::new(
            key,
            sequence_number,
            DEPOSIT_EVENT_TYPE.clone(),
            bcs::to_bytes(&deposit_event)?,
        ))
    }
}

struct CoinWithdrawTranslator;
impl EventV2Translator for CoinWithdrawTranslator {
    fn translate_event_v2_to_v1(
        &self,
        v2: &ContractEventV2,
        db_indexer: &DBIndexer,
    ) -> Result<ContractEventV1> {
        let coin_withdraw = CoinWithdraw::try_from_bytes(v2.event_data())?;
        let struct_tag_str = format!("0x1::coin::CoinStore<{}>", coin_withdraw.coin_type());
        let struct_tag = StructTag::from_str(&struct_tag_str)?;
        let (key, sequence_number) = if let Some(state_value) =
            db_indexer.get_state_value_for_resource(coin_withdraw.account(), &struct_tag)?
        {
            // We can use `DummyCoinType` as it does not affect the correctness of deserialization.
            let coin_store_resource: CoinStoreResource<DummyCoinType> =
                bcs::from_bytes(state_value.bytes())?;
            let key = *coin_store_resource.withdraw_events().key();
            let sequence_number = db_indexer
                .get_next_sequence_number(&key, coin_store_resource.withdraw_events().count())?;
            (key, sequence_number)
        } else {
            (EventKey::new(2, coin_withdraw.account().clone()), 0)
        };
        let withdraw_event = WithdrawEvent::new(coin_withdraw.amount());
        Ok(ContractEventV1::new(
            key,
            sequence_number,
            WITHDRAW_EVENT_TYPE.clone(),
            bcs::to_bytes(&withdraw_event)?,
        ))
    }
}

struct CoinRegisterTranslator;
impl EventV2Translator for CoinRegisterTranslator {
    fn translate_event_v2_to_v1(
        &self,
        v2: &ContractEventV2,
        db_indexer: &DBIndexer,
    ) -> Result<ContractEventV1> {
        let coin_register = CoinRegister::try_from_bytes(v2.event_data())?;
        let struct_tag_str = format!("0x1::account::Account");
        let struct_tag = StructTag::from_str(&struct_tag_str)?;
        let (key, sequence_number) = if let Some(state_value) =
            db_indexer.get_state_value_for_resource(coin_register.account(), &struct_tag)?
        {
            let account_resource: AccountResource = bcs::from_bytes(state_value.bytes())?;
            let key = *account_resource.coin_register_events().key();
            let sequence_number = db_indexer
                .get_next_sequence_number(&key, account_resource.coin_register_events().count())?;
            (key, sequence_number)
        } else {
            (EventKey::new(0, coin_register.account().clone()), 0)
        };
        let coin_register_event = CoinRegisterEvent::new(coin_register.type_info().clone());
        Ok(ContractEventV1::new(
            key,
            sequence_number,
            COIN_REGISTER_EVENT_TYPE.clone(),
            bcs::to_bytes(&coin_register_event)?,
        ))
    }
}

struct KeyRotationTranslator;
impl EventV2Translator for KeyRotationTranslator {
    fn translate_event_v2_to_v1(
        &self,
        v2: &ContractEventV2,
        db_indexer: &DBIndexer,
    ) -> Result<ContractEventV1> {
        let key_rotation = KeyRotation::try_from_bytes(v2.event_data())?;
        let struct_tag_str = format!("0x1::account::Account");
        let struct_tag = StructTag::from_str(&struct_tag_str)?;
        let (key, sequence_number) = if let Some(state_value) =
            db_indexer.get_state_value_for_resource(key_rotation.account(), &struct_tag)?
        {
            let account_resource: AccountResource = bcs::from_bytes(state_value.bytes())?;
            let key = *account_resource.key_rotation_events().key();
            let sequence_number = db_indexer
                .get_next_sequence_number(&key, account_resource.key_rotation_events().count())?;
            (key, sequence_number)
        } else {
            (EventKey::new(1, key_rotation.account().clone()), 0)
        };
        let key_rotation_event = KeyRotationEvent::new(
            key_rotation.old_authentication_key().clone(),
            key_rotation.new_authentication_key().clone(),
        );
        Ok(ContractEventV1::new(
            key,
            sequence_number,
            KEY_ROTATION_EVENT_TYPE.clone(),
            bcs::to_bytes(&key_rotation_event)?,
        ))
    }
}
