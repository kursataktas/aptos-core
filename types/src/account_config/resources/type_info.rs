// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use itertools::Itertools;
use move_core_types::{
    account_address::AccountAddress,
    ident_str,
    identifier::IdentStr,
    move_resource::{MoveResource, MoveStructType},
};
use serde::{Deserialize, Serialize};

/// A Rust representation of TypeInfo.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TypeInfoResource {
    pub account_address: AccountAddress,
    pub module_name: Vec<u8>,
    pub struct_name: Vec<u8>,
}

impl TypeInfoResource {
    pub fn new<T: MoveStructType>() -> anyhow::Result<Self> {
        let struct_tag = T::struct_tag();
        Ok(Self {
            account_address: struct_tag.address,
            module_name: bcs::to_bytes(&struct_tag.module.to_string())?,
            struct_name: bcs::to_bytes(
                &if struct_tag.type_args.is_empty() {
                    struct_tag.name.to_string()
                } else {
                    format!(
                        "{}<{}>",
                        struct_tag.name,
                        struct_tag
                            .type_args
                            .iter()
                            .map(|v| v.to_string())
                            .join(", ")
                    )
                    .to_string()
                },
            )?,
        })
    }
}

impl MoveStructType for TypeInfoResource {
    const MODULE_NAME: &'static IdentStr = ident_str!("type_info");
    const STRUCT_NAME: &'static IdentStr = ident_str!("TypeInfo");
}

impl MoveResource for TypeInfoResource {}
