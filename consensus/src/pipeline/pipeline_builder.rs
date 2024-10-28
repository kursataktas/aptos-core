// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    block_preparer::BlockPreparer, counters, execution_pipeline::SIG_VERIFY_POOL, monitor,
    payload_manager::TPayloadManager, txn_notifier::TxnNotifier,
};
use anyhow::anyhow;
use aptos_consensus_notifications::ConsensusNotificationSender;
use aptos_consensus_types::{
    block::Block,
    common::Round,
    pipeline::commit_vote::CommitVote,
    pipelined_block::{
        PipelineFutures, PipelineRx, PipelineTx, PipelinedBlock, TaskError, TaskFuture, TaskResult,
    },
};
use aptos_crypto::HashValue;
use aptos_executor_types::{state_compute_result::StateComputeResult, BlockExecutorTrait};
use aptos_experimental_runtimes::thread_manager::optimal_min_len;
use aptos_logger::{error, warn};
use aptos_types::{
    block_executor::config::BlockExecutorConfigFromOnchain,
    ledger_info::{LedgerInfo, LedgerInfoWithSignatures},
    randomness::Randomness,
    transaction::{
        signature_verified_transaction::{SignatureVerifiedTransaction, TransactionProvider},
        SignedTransaction, Transaction,
    },
    validator_signer::ValidatorSigner,
};
use futures::FutureExt;
use move_core_types::account_address::AccountAddress;
use rayon::prelude::*;
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{select, sync::oneshot};

struct PipelineBuilder {
    block_preparer: Arc<BlockPreparer>,
    executor: Arc<dyn BlockExecutorTrait>,
    validators: Arc<[AccountAddress]>,
    block_executor_onchain_config: BlockExecutorConfigFromOnchain,
    is_randomness_enabled: bool,
    signer: Arc<ValidatorSigner>,
    state_sync_notifier: Arc<dyn ConsensusNotificationSender>,
    payload_manager: Arc<dyn TPayloadManager>,
    txn_notifier: Arc<dyn TxnNotifier>,
}

fn spawn_shared_fut<
    T: Send + Clone + 'static,
    F: Future<Output = TaskResult<T>> + Send + 'static,
>(
    f: F,
) -> TaskFuture<T> {
    let join_handle = tokio::spawn(f);
    async move {
        match join_handle.await {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(TaskError::JoinError(Arc::new(e))),
        }
    }
    .boxed()
    .shared()
}

async fn wait_and_log_error<T, F: Future<Output = TaskResult<T>>>(f: F, msg: String) {
    if let Err(TaskError::InternalError(e)) = f.await {
        warn!("{} failed: {}", msg, e);
    }
}

// TODO: add counters for each phase
impl PipelineBuilder {
    fn channel() -> (PipelineTx, PipelineRx) {
        let (rand_tx, rand_rx) = oneshot::channel();
        let (order_vote_tx, order_vote_rx) = oneshot::channel();
        let (order_proof_tx, order_proof_rx) = tokio::sync::broadcast::channel(1);
        let (commit_proof_tx, commit_proof_rx) = tokio::sync::broadcast::channel(1);
        (
            PipelineTx {
                rand_tx,
                order_vote_tx,
                order_proof_tx,
                commit_proof_tx,
            },
            PipelineRx {
                rand_rx,
                order_vote_rx,
                order_proof_rx,
                commit_proof_rx,
            },
        )
    }

    fn build(
        &self,
        parent: &PipelineFutures,
        block: Arc<Block>,
        block_store_callback: Box<
            dyn FnOnce(HashValue, Round, LedgerInfoWithSignatures) + Send + Sync,
        >,
    ) -> (PipelineFutures, PipelineTx) {
        let (tx, rx) = Self::channel();
        let PipelineRx {
            rand_rx,
            order_vote_rx,
            order_proof_rx,
            commit_proof_rx,
        } = rx;

        let prepare_fut =
            spawn_shared_fut(Self::prepare(self.block_preparer.clone(), block.clone()));
        let execute_fut = spawn_shared_fut(Self::execute(
            prepare_fut.clone(),
            parent.execute_fut.clone(),
            rand_rx,
            self.executor.clone(),
            block.clone(),
            self.is_randomness_enabled,
            self.validators.clone(),
            self.block_executor_onchain_config.clone(),
        ));
        let ledger_update_fut = spawn_shared_fut(Self::ledger_update(
            execute_fut.clone(),
            parent.ledger_update_fut.clone(),
            self.executor.clone(),
            block.clone(),
        ));
        let commit_vote_fut = spawn_shared_fut(Self::sign_commit_vote(
            ledger_update_fut.clone(),
            order_vote_rx,
            order_proof_rx.resubscribe(),
            commit_proof_rx.resubscribe(),
            self.signer.clone(),
            block.clone(),
        ));
        let pre_commit_fut = spawn_shared_fut(Self::pre_commit(
            ledger_update_fut.clone(),
            parent.pre_commit_fut.clone(),
            order_proof_rx,
            self.executor.clone(),
            block.id(),
        ));
        let commit_ledger_fut = spawn_shared_fut(Self::commit_ledger(
            commit_proof_rx,
            parent.commit_ledger_fut.clone(),
            self.executor.clone(),
        ));

        let post_ledger_update_fut = spawn_shared_fut(Self::post_ledger_update(
            prepare_fut.clone(),
            ledger_update_fut.clone(),
            self.txn_notifier.clone(),
        ));
        let post_pre_commit_fut = spawn_shared_fut(Self::post_pre_commit(
            pre_commit_fut.clone(),
            parent.post_pre_commit_fut.clone(),
            self.state_sync_notifier.clone(),
            self.payload_manager.clone(),
            block.clone(),
        ));
        let post_commit_fut = spawn_shared_fut(Self::post_commit_ledger(
            commit_ledger_fut.clone(),
            parent.post_commit_fut.clone(),
            block_store_callback,
            block,
        ));
        (
            PipelineFutures {
                prepare_fut,
                execute_fut,
                ledger_update_fut,
                post_ledger_update_fut,
                commit_vote_fut,
                pre_commit_fut,
                post_pre_commit_fut,
                commit_ledger_fut,
                post_commit_fut,
            },
            tx,
        )
    }

    async fn prepare(
        preparer: Arc<BlockPreparer>,
        block: Arc<Block>,
    ) -> TaskResult<Vec<SignatureVerifiedTransaction>> {
        let input_txns = loop {
            match preparer.prepare_block(&block).await {
                Ok(input_txns) => break input_txns,
                Err(e) => {
                    warn!(
                        "[BlockPreparer] failed to prepare block {}, retrying: {}",
                        block.id(),
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                },
            }
        };
        let sig_verification_start = Instant::now();
        let sig_verified_txns: Vec<SignatureVerifiedTransaction> = SIG_VERIFY_POOL.install(|| {
            let num_txns = input_txns.len();
            input_txns
                .into_par_iter()
                .with_min_len(optimal_min_len(num_txns, 32))
                .map(|t| Transaction::UserTransaction(t).into())
                .collect::<Vec<_>>()
        });
        counters::PREPARE_BLOCK_SIG_VERIFICATION_TIME
            .observe_duration(sig_verification_start.elapsed());
        Ok(sig_verified_txns)
    }

    async fn execute(
        prepare_phase: TaskFuture<Vec<SignatureVerifiedTransaction>>,
        parent_block_execute_phase: TaskFuture<()>,
        randomness_rx: oneshot::Receiver<Option<Randomness>>,
        executor: Arc<dyn BlockExecutorTrait>,
        block: Arc<Block>,
        is_randomness_enabled: bool,
        validator: Arc<[AccountAddress]>,
        onchain_execution_config: BlockExecutorConfigFromOnchain,
    ) -> TaskResult<()> {
        parent_block_execute_phase.await?;
        let user_txns = prepare_phase.await?;
        let maybe_rand = randomness_rx
            .await
            .map_err(|_| anyhow!("randomness tx cancelled"))?;
        let metadata_txn = if is_randomness_enabled {
            block.new_metadata_with_randomness(&validator, maybe_rand)
        } else {
            block.new_block_metadata(&validator).into()
        };
        let txns = [
            vec![SignatureVerifiedTransaction::from(Transaction::from(
                metadata_txn,
            ))],
            block
                .validator_txns()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(Transaction::ValidatorTransaction)
                .map(SignatureVerifiedTransaction::from)
                .collect(),
            user_txns,
        ]
        .concat();
        tokio::task::spawn_blocking(move || {
            executor
                .execute_and_state_checkpoint(
                    (block.id(), txns).into(),
                    block.parent_id(),
                    onchain_execution_config,
                )
                .map_err(anyhow::Error::from)
        })
        .await
        .expect("spawn blocking failed")?;
        Ok(())
    }

    async fn ledger_update(
        execute_phase: TaskFuture<()>,
        parent_block_ledger_update_phase: TaskFuture<StateComputeResult>,
        executor: Arc<dyn BlockExecutorTrait>,
        block: Arc<Block>,
    ) -> TaskResult<StateComputeResult> {
        parent_block_ledger_update_phase.await?;
        execute_phase.await?;
        let result = tokio::task::spawn_blocking(move || {
            executor
                .ledger_update(block.id(), block.parent_id())
                .map_err(anyhow::Error::from)
        })
        .await
        .expect("spawn blocking failed")?;
        Ok(result)
    }

    async fn post_ledger_update(
        prepare_fut: TaskFuture<Vec<SignatureVerifiedTransaction>>,
        ledger_update: TaskFuture<StateComputeResult>,
        mempool_notifier: Arc<dyn TxnNotifier>,
    ) -> TaskResult<()> {
        let user_txns = prepare_fut.await?;
        let compute_result = ledger_update.await?;
        let compute_status = compute_result.compute_status_for_input_txns();
        // the length of compute_status is user_txns.len() + num_vtxns + 1 due to having blockmetadata
        if user_txns.len() >= compute_status.len() {
            // reconfiguration suffix blocks don't have any transactions
            // otherwise, this is an error
            if !compute_status.is_empty() {
                error!(
                        "Expected compute_status length and actual compute_status length mismatch! user_txns len: {}, compute_status len: {}, has_reconfiguration: {}",
                        user_txns.len(),
                        compute_status.len(),
                        compute_result.has_reconfiguration(),
                    );
            }
        } else {
            let user_txn_status = &compute_status[compute_status.len() - user_txns.len()..];
            // todo: avoid clone
            let txns: Vec<SignedTransaction> = user_txns
                .iter()
                .flat_map(|txn| txn.get_transaction().map(|t| t.try_as_signed_user_txn()))
                .flatten()
                .cloned()
                .collect();

            // notify mempool about failed transaction
            if let Err(e) = mempool_notifier
                .notify_failed_txn(&txns, user_txn_status)
                .await
            {
                error!(
                    error = ?e, "Failed to notify mempool of rejected txns",
                );
            }
        }
        Ok(())
    }

    async fn sign_commit_vote(
        ledger_update_phase: TaskFuture<StateComputeResult>,
        order_vote_rx: oneshot::Receiver<()>,
        mut order_proof_rx: tokio::sync::broadcast::Receiver<()>,
        mut commit_proof_rx: tokio::sync::broadcast::Receiver<LedgerInfoWithSignatures>,
        signer: Arc<ValidatorSigner>,
        block: Arc<Block>,
    ) -> TaskResult<CommitVote> {
        let compute_result = ledger_update_phase.await?;
        let block_info = block.gen_block_info(
            compute_result.root_hash(),
            compute_result.last_version_or_0(),
            compute_result.epoch_state().clone(),
        );
        let ledger_info = LedgerInfo::new(block_info, HashValue::zero());
        // either order_vote_rx or order_proof_rx can trigger the next phase
        select! {
            Ok(_) = order_vote_rx => {
            }
            Ok(_) = order_proof_rx.recv() => {
            }
            Ok(_) = commit_proof_rx.recv() => {
            }
        }
        let signature = signer.sign(&ledger_info).unwrap();
        Ok(CommitVote::new_with_signature(
            signer.author(),
            ledger_info,
            signature,
        ))
    }

    async fn pre_commit(
        ledger_update_phase: TaskFuture<StateComputeResult>,
        // TODO bound parent_commit_ledger too
        parent_block_pre_commit_phase: TaskFuture<StateComputeResult>,
        mut order_proof_rx: tokio::sync::broadcast::Receiver<()>,
        executor: Arc<dyn BlockExecutorTrait>,
        block_id: HashValue,
    ) -> TaskResult<StateComputeResult> {
        let compute_result = ledger_update_phase.await?;
        parent_block_pre_commit_phase.await?;
        order_proof_rx
            .recv()
            .await
            .map_err(|_| anyhow!("order proof tx cancelled"))?;
        tokio::task::spawn_blocking(move || {
            executor
                .pre_commit_block(block_id)
                .map_err(anyhow::Error::from)
        })
        .await
        .expect("spawn blocking failed")?;
        Ok(compute_result)
    }

    async fn post_pre_commit(
        pre_commit: TaskFuture<StateComputeResult>,
        parent_post_pre_commit: TaskFuture<()>,
        state_sync_notifier: Arc<dyn ConsensusNotificationSender>,
        payload_manager: Arc<dyn TPayloadManager>,
        block: Arc<Block>,
    ) -> TaskResult<()> {
        let compute_result = pre_commit.await?;
        parent_post_pre_commit.await?;
        let payload = block.payload().cloned();
        let timestamp = block.timestamp_usecs();
        let _timer = counters::OP_COUNTERS.timer("pre_commit_notify");

        let txns = compute_result.transactions_to_commit().to_vec();
        let subscribable_events = compute_result.subscribable_events().to_vec();
        if let Err(e) = monitor!(
            "notify_state_sync",
            state_sync_notifier
                .notify_new_commit(txns, subscribable_events)
                .await
        ) {
            error!(error = ?e, "Failed to notify state synchronizer");
        }

        let payload_vec = payload.into_iter().collect();
        payload_manager.notify_commit(timestamp, payload_vec);
        Ok(())
    }

    async fn commit_ledger(
        mut commit_proof_rx: tokio::sync::broadcast::Receiver<LedgerInfoWithSignatures>,
        parent_block_commit_phase: TaskFuture<LedgerInfoWithSignatures>,
        executor: Arc<dyn BlockExecutorTrait>,
    ) -> TaskResult<LedgerInfoWithSignatures> {
        parent_block_commit_phase.await?;
        let ledger_info_with_sigs = commit_proof_rx
            .recv()
            .await
            .map_err(|_| anyhow!("commit rx cancelled"))?;
        let ledger_info_with_sigs_clone = ledger_info_with_sigs.clone();
        tokio::task::spawn_blocking(move || {
            executor
                .commit_ledger(ledger_info_with_sigs_clone)
                .map_err(anyhow::Error::from)
        })
        .await
        .expect("spawn blocking failed")?;
        Ok(ledger_info_with_sigs)
    }

    async fn post_commit_ledger(
        commit_ledger: TaskFuture<LedgerInfoWithSignatures>,
        parent_post_commit: TaskFuture<()>,
        block_store_callback: Box<
            dyn FnOnce(HashValue, Round, LedgerInfoWithSignatures) + Send + Sync,
        >,
        block: Arc<Block>,
    ) -> TaskResult<()> {
        let ledger_info_with_sigs = commit_ledger.await?;
        parent_post_commit.await?;
        block_store_callback(block.id(), block.round(), ledger_info_with_sigs);
        Ok(())
    }

    async fn monitor(epoch: u64, round: Round, block_id: HashValue, all_futs: PipelineFutures) {
        let PipelineFutures {
            prepare_fut,
            execute_fut,
            ledger_update_fut,
            post_ledger_update_fut: _,
            commit_vote_fut: _,
            pre_commit_fut,
            post_pre_commit_fut: _,
            commit_ledger_fut,
            post_commit_fut: _,
        } = all_futs;
        wait_and_log_error(prepare_fut, format!("{epoch} {round} {block_id} prepare")).await;
        wait_and_log_error(execute_fut, format!("{epoch} {round} {block_id} execute")).await;
        wait_and_log_error(
            ledger_update_fut,
            format!("{epoch} {round} {block_id} ledger update"),
        )
        .await;
        wait_and_log_error(
            pre_commit_fut,
            format!("{epoch} {round} {block_id} pre commit"),
        )
        .await;
        wait_and_log_error(
            commit_ledger_fut,
            format!("{epoch} {round} {block_id} commit ledger"),
        )
        .await;
    }
}
