// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::sharded_block_executor::{
    local_executor_shard::GlobalCrossShardClient, sharded_executor_service::ShardedExecutorService,
};
use aptos_logger::trace;
use aptos_types::{
    block_executor::{
        config::{BlockExecutorConfig, BlockExecutorConfigFromOnchain, BlockExecutorLocalConfig},
        partitioner::{TransactionWithDependencies, GLOBAL_ROUND_ID},
    },
    state_store::StateView,
    transaction::{analyzed_transaction::AnalyzedTransaction, TransactionOutput},
};
use move_core_types::vm_status::VMStatus;
use std::sync::Arc;
use aptos_types::transaction::signature_verified_transaction::SignatureVerifiedTransaction;
use crate::sharded_block_executor::streamed_transactions_provider::StreamedTransactionsProvider;

pub struct GlobalExecutor<S: StateView + Sync + Send + 'static> {
    global_cross_shard_client: Arc<GlobalCrossShardClient>,
    executor_thread_pool: Arc<rayon::ThreadPool>,
    concurrency_level: usize,
    phantom: std::marker::PhantomData<S>,
}

impl<S: StateView + Sync + Send + 'static> GlobalExecutor<S> {
    pub fn new(cross_shard_client: Arc<GlobalCrossShardClient>, num_threads: usize) -> Self {
        let executor_thread_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                // We need two extra threads for the cross-shard commit receiver and the thread
                // that is blocked on waiting for execute block to finish.
                .num_threads(num_threads + 2)
                .build()
                .unwrap(),
        );
        Self {
            global_cross_shard_client: cross_shard_client,
            executor_thread_pool,
            phantom: std::marker::PhantomData,
            concurrency_level: num_threads,
        }
    }

    pub fn execute_global_txns(
        &self,
        transactions: Vec<TransactionWithDependencies<AnalyzedTransaction>>,
        state_view: &S,
        onchain_config: BlockExecutorConfigFromOnchain,
    ) -> Result<Vec<TransactionOutput>, VMStatus> {
        trace!("executing the last round in global executor",);
        return Ok(vec![]);
        /*if transactions.is_empty() {
            return Ok(vec![]);
        }
        let signature_verified_transactions: Vec<SignatureVerifiedTransaction> = transactions
            .into_iter()
            .map(|txn| txn.into_txn().into_txn())
            .collect();
        let streamed_transactions_provider = Arc::new(StreamedTransactionsProvider::new(signature_verified_transactions));
        ShardedExecutorService::execute_transactions_with_dependencies(
            None,
            self.executor_thread_pool.clone(),
            streamed_transactions_provider,
            self.global_cross_shard_client.clone(),
            None,
            GLOBAL_ROUND_ID,
            state_view,
            BlockExecutorConfig {
                local: BlockExecutorLocalConfig {
                    concurrency_level: self.concurrency_level,
                    allow_fallback: true,
                    discard_failed_blocks: false,
                },
                onchain: onchain_config,
            },
        )*/
    }

    pub fn get_executor_thread_pool(&self) -> Arc<rayon::ThreadPool> {
        self.executor_thread_pool.clone()
    }
}
