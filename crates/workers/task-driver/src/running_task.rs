//! Encapsulates the running task's bookkeeping structure to simplify the driver
//! logic

use state::{State, error::StateError};
use tracing::{error, info};
use types_core::AccountId;
use types_tasks::TaskIdentifier;

use crate::{
    error::TaskDriverError,
    task_state::TaskStateWrapper,
    traits::{Task, TaskContext, TaskError},
};

// ----------------
// | Running Task |
// ----------------

/// The container type for a task running in the driver
///
/// Used to simplify driver logic
pub struct RunnableTask<T: Task> {
    /// The id of the underlying task
    task_id: TaskIdentifier,
    /// The underlying task
    task: T,
    /// A handle to the relayer-global state
    state: State,
}

impl<T: Task> RunnableTask<T> {
    /// Creates a new running task from the given task and state
    pub fn new(task_id: TaskIdentifier, task: T, state: State) -> Self {
        Self { task_id, task, state }
    }

    /// Get the inner task
    pub fn inner(&self) -> &T {
        &self.task
    }

    /// Create a runnable from the given descriptor and context
    pub async fn from_descriptor(
        id: TaskIdentifier,
        descriptor: T::Descriptor,
        ctx: TaskContext,
    ) -> Result<Self, TaskDriverError> {
        Self::restore(id, descriptor, None, ctx).await
    }

    /// Create a runnable from the given descriptor, restoring persisted task state
    pub async fn restore(
        id: TaskIdentifier,
        descriptor: T::Descriptor,
        restored_state: Option<T::State>,
        ctx: TaskContext,
    ) -> Result<Self, TaskDriverError> {
        let state = ctx.state.clone();
        let task = T::restore(descriptor, restored_state, ctx).await?;

        Ok(Self::new(id, task, state))
    }

    /// The ID of the underlying task
    pub fn id(&self) -> TaskIdentifier {
        self.task_id
    }

    /// Whether the underlying task completed
    pub fn completed(&self) -> bool {
        self.task.completed()
    }

    /// Returns the state of the underlying task
    pub fn state(&self) -> TaskStateWrapper {
        self.task.task_state().into()
    }

    /// `true` if the task does not need to update the task queue during state
    /// transitions or cleanup
    pub fn bypass_task_queue(&self) -> bool {
        self.task.bypass_task_queue()
    }

    /// Step the underlying task, returns whether the driver should continue or
    /// abort. `Ok(true)` means successful step, `Ok(false)` means that the task
    /// step failed and should be retried, an error should be aborted
    ///
    /// This includes a state transition in the consensus engine, if this method
    /// returns an error the driver should abort the task
    pub async fn step(&mut self) -> Result<bool, TaskDriverError> {
        // Handle a failed step
        if let Err(e) = self.task.step().await {
            error!("error executing task step: {e}");
            let retryable = e.retryable() && self.is_task_running().await?;
            return if retryable { Ok(false) } else { Err(e.into()) };
        };

        // Successful step, attempt to transition the state
        self.transition_state().await?;
        Ok(true)
    }

    /// Attempts to transition the state of the underlying task in the consensus
    /// engine. If this method fails the driver should abort the task
    pub async fn transition_state(&self) -> Result<(), StateError> {
        let task_id = self.task_id;
        let name = self.task.name();
        let new_state = self.state();
        info!("task {name}({task_id:?}) transitioning to state {new_state}");

        // Preemptive tasks need not update state in the consensus engine
        if self.bypass_task_queue() {
            return Ok(());
        }

        // If this state commits the task (first state past the commit point),
        // or if the task is completed, then await consensus before continuing
        let is_commit = new_state.is_committing();
        let is_completed = new_state.completed();
        let waiter = self.state.transition_task(task_id, new_state.into()).await?;
        if is_commit || is_completed {
            waiter.await?;
        }

        Ok(())
    }

    /// Cleanup the underlying task
    pub async fn cleanup(
        &mut self,
        success: bool,
        affected_accounts: Vec<AccountId>,
    ) -> Result<(), TaskDriverError> {
        // Do not propagate errors from cleanup, continue to cleanup
        if let Err(e) = self.task.cleanup().await {
            error!("error cleaning up task: {e:?}");
        }

        // Pop the task from the state, unless this task bypasses the task queue
        // Do not propagate this error; otherwise we may skip the account refresh step
        if !self.bypass_task_queue()
            && let Err(e) = self.pop_task_or_clear_queue(success, &affected_accounts).await
        {
            error!("error popping task: {e}");
        }

        Ok(())
    }

    /// Pop a task from a queue
    ///
    /// This method will clear the task queues of the affected accounts if
    /// popping the task fails
    async fn pop_task_or_clear_queue(
        &self,
        success: bool,
        affected_accounts: &[AccountId],
    ) -> Result<(), TaskDriverError> {
        let waiter = self.state.pop_task(self.task_id, success).await?;
        let mut res = waiter.await;
        if res.is_ok() {
            return Ok(());
        }

        // Failing through the above code path implies we should clear the task queues
        for account_id in affected_accounts {
            let clear_res = self.state.clear_task_queue(account_id).await?.await;
            res = clear_res.and(res);
        }

        res.map(|_| ()).map_err(Into::into)
    }

    /// Returns whether the task is marked as running in the state
    async fn is_task_running(&self) -> Result<bool, TaskDriverError> {
        let task = self.state.get_task(&self.task_id).await?;
        let is_running = task.is_some_and(|t| t.state.is_running());

        if !is_running {
            error!("task {} is not running", self.task_id);
        }

        Ok(is_running)
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use alloy::{primitives::Address, signers::local::PrivateKeySigner};
    use async_trait::async_trait;
    use darkpool_client::{DarkpoolClient, client::DarkpoolClientConfig};
    use job_types::{
        event_manager::new_event_manager_queue, matching_engine::new_matching_engine_worker_queue,
        network_manager::new_network_manager_queue, proof_manager::new_proof_manager_queue,
        task_driver::new_task_driver_queue,
    };
    use state::test_helpers::mock_state;
    use system_bus::SystemBus;
    use types_core::{Chain, HmacKey};
    use url::Url;
    use uuid::Uuid;

    use crate::{
        task_state::TaskStateWrapper,
        traits::{Descriptor, TaskContext, TaskState},
        utils::indexer_client::IndexerClient,
    };

    use super::*;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct DummyDescriptor;

    impl Descriptor for DummyDescriptor {}

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    enum DummyState {
        Pending,
        Running,
        Completed,
    }

    impl std::fmt::Display for DummyState {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Pending => write!(f, "Pending"),
                Self::Running => write!(f, "Running"),
                Self::Completed => write!(f, "Completed"),
            }
        }
    }

    impl TaskState for DummyState {
        fn completed(&self) -> bool {
            matches!(self, Self::Completed)
        }

        fn commit_point() -> Self {
            Self::Running
        }
    }

    impl From<DummyState> for TaskStateWrapper {
        fn from(value: DummyState) -> Self {
            TaskStateWrapper::NodeStartup(match value {
                DummyState::Pending => crate::tasks::node_startup::NodeStartupTaskState::Pending,
                DummyState::Running => {
                    crate::tasks::node_startup::NodeStartupTaskState::RunningStateMigrations
                },
                DummyState::Completed => crate::tasks::node_startup::NodeStartupTaskState::Completed,
            })
        }
    }

    struct DummyTask {
        state: DummyState,
    }

    #[async_trait]
    impl Task for DummyTask {
        type Descriptor = DummyDescriptor;
        type State = DummyState;
        type Error = DummyError;

        async fn new(_descriptor: Self::Descriptor, _ctx: TaskContext) -> Result<Self, Self::Error> {
            Ok(Self { state: DummyState::Pending })
        }

        fn task_state(&self) -> Self::State {
            self.state.clone()
        }

        fn name(&self) -> String {
            "dummy-task".to_string()
        }

        fn restore_state(&mut self, state: Self::State) {
            self.state = state;
        }

        async fn step(&mut self) -> Result<(), Self::Error> {
            self.state = DummyState::Completed;
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct DummyError;

    impl std::fmt::Display for DummyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "dummy error")
        }
    }

    impl TaskError for DummyError {
        fn retryable(&self) -> bool {
            false
        }
    }

    async fn mock_task_context() -> TaskContext {
        let state = mock_state().await;
        let darkpool_client = DarkpoolClient::new(DarkpoolClientConfig {
            darkpool_addr: Address::ZERO,
            permit2_addr: Address::ZERO,
            chain: Chain::ArbitrumSepolia,
            rpc_url: "http://localhost:8545".to_string(),
            private_key: PrivateKeySigner::random(),
            block_polling_interval: std::time::Duration::from_secs(1),
        })
        .expect("darkpool client config should be constructable");
        let (network_queue, network_recv) = new_network_manager_queue();
        let (proof_queue, proof_recv) = new_proof_manager_queue();
        let (event_queue, event_recv) = new_event_manager_queue();
        let (matching_engine_queue, matching_engine_recv) = new_matching_engine_worker_queue();
        let (task_queue, task_recv) = new_task_driver_queue();
        mem::forget(network_recv);
        mem::forget(proof_recv);
        mem::forget(event_recv);
        mem::forget(matching_engine_recv);
        mem::forget(task_recv);

        TaskContext {
            darkpool_client,
            state,
            network_queue,
            proof_queue,
            event_queue,
            matching_engine_queue,
            task_queue,
            bus: SystemBus::new(),
            indexer_client: IndexerClient::new(
                Url::parse("http://localhost:3000").unwrap(),
                HmacKey([0u8; 32]),
            ),
        }
    }

    #[tokio::test]
    async fn test_restore_uses_persisted_task_state() {
        let ctx = mock_task_context().await;
        let runnable = RunnableTask::<DummyTask>::restore(
            Uuid::new_v4(),
            DummyDescriptor,
            Some(DummyState::Running),
            ctx,
        )
        .await
        .expect("task restore should succeed");

        assert!(matches!(
            runnable.state(),
            TaskStateWrapper::NodeStartup(
                crate::tasks::node_startup::NodeStartupTaskState::RunningStateMigrations
            )
        ));
    }
}
