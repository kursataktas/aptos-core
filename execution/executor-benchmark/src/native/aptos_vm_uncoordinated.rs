// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::native_config::NATIVE_EXECUTOR_POOL;
use aptos_block_executor::counters::BLOCK_EXECUTOR_INNER_EXECUTE_BLOCK;
use aptos_types::{
    block_executor::config::BlockExecutorConfigFromOnchain,
    state_store::StateView,
    transaction::{
        signature_verified_transaction::SignatureVerifiedTransaction, BlockOutput,
        TransactionOutput,
    },
    vm_status::VMStatus,
};
use aptos_vm::{AptosVM, VMBlockExecutor};
use aptos_vm_environment::environment::AptosEnvironment;
use aptos_vm_logging::log_schema::AdapterLogSchema;
use aptos_vm_types::module_and_script_storage::AsAptosCodeStorage;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

pub struct AptosVMParallelUncoordinatedBlockExecutor;

impl VMBlockExecutor for AptosVMParallelUncoordinatedBlockExecutor {
    fn new() -> Self {
        Self
    }

    fn execute_block(
        &self,
        transactions: &[SignatureVerifiedTransaction],
        state_view: &(impl StateView + Sync),
        _onchain_config: BlockExecutorConfigFromOnchain,
    ) -> Result<BlockOutput<TransactionOutput>, VMStatus> {
        let _timer = BLOCK_EXECUTOR_INNER_EXECUTE_BLOCK.start_timer();

        // let features = Features::fetch_config(&state_view).unwrap_or_default();

        let env = AptosEnvironment::new(state_view);
        let vm = AptosVM::new(env.clone(), state_view);

        let transaction_outputs = NATIVE_EXECUTOR_POOL.install(|| {
            transactions
                .par_iter()
                .enumerate()
                .map(|(txn_idx, txn)| {
                    let log_context = AdapterLogSchema::new(state_view.id(), txn_idx);
                    let code_storage = state_view.as_aptos_code_storage(env.clone());

                    vm.execute_single_transaction(
                        txn,
                        &vm.as_move_resolver(state_view),
                        &code_storage,
                        &log_context,
                    )
                    .map(|(_vm_status, vm_output)| {
                        vm_output
                            .try_materialize_into_transaction_output(state_view)
                            .unwrap()

                        // if vm_status == VMStatus::Executed {

                        // } else {
                        //     TransactionOutput::new(
                        //         Default::default(),
                        //         vec![],
                        //         0,
                        //         TransactionStatus::from_vm_status(vm_status, false, &features),
                        //         TransactionAuxiliaryData::default(),
                        //     )
                        // }
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })?;

        Ok(BlockOutput::new(transaction_outputs, None))
    }
}
