//! Task state management

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use types_tasks::QueuedTaskState;

use crate::{
    tasks::{
        cancel_order::CancelOrderTaskState, create_balance::CreateBalanceTaskState,
        create_new_account::CreateNewAccountTaskState, create_order::CreateOrderTaskState,
        deposit::DepositTaskState, node_startup::NodeStartupTaskState,
        refresh_account::RefreshAccountTaskState,
        settlement::settle_external_match::SettleExternalMatchTaskState,
        settlement::settle_internal_match::SettleInternalMatchTaskState,
        settlement::settle_private_match::SettlePrivateMatchTaskState, withdraw::WithdrawTaskState,
    },
    traits::TaskState,
};

// --------------------
// | State Management |
// --------------------

/// Defines a wrapper that allows state objects to be stored generically
#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
#[serde(tag = "task_type", content = "state")]
pub enum TaskStateWrapper {
    /// The state of a create new account task
    CreateNewAccount(CreateNewAccountTaskState),
    /// The state of a node startup task
    NodeStartup(NodeStartupTaskState),
    /// The state of a deposit task
    Deposit(DepositTaskState),
    /// The state of a create balance task
    CreateBalance(CreateBalanceTaskState),
    /// The state of a create order task
    CreateOrder(CreateOrderTaskState),
    /// The state of a cancel order task
    CancelOrder(CancelOrderTaskState),
    /// The state of a refresh account task
    RefreshAccount(RefreshAccountTaskState),
    /// The state of a settle internal match task
    SettleInternalMatch(SettleInternalMatchTaskState),
    /// The state of a settle external match task
    SettleExternalMatch(SettleExternalMatchTaskState),
    /// The state of a settle private match task
    SettlePrivateMatch(SettlePrivateMatchTaskState),
    /// The state of a withdraw task
    Withdraw(WithdrawTaskState),
}

impl TaskStateWrapper {
    /// Deserialize a typed task-state payload from a queued task state
    pub fn from_queued_task_state(state: &QueuedTaskState) -> Result<Option<Self>, serde_json::Error> {
        match state.execution_state() {
            Some(serialized) => serde_json::from_str(serialized).map(Some),
            None => Ok(None),
        }
    }

    /// Whether the underlying state is committed or not
    pub fn committed(&self) -> bool {
        match self {
            TaskStateWrapper::CreateNewAccount(state) => {
                <CreateNewAccountTaskState as TaskState>::committed(state)
            },
            TaskStateWrapper::NodeStartup(state) => {
                <NodeStartupTaskState as TaskState>::committed(state)
            },
            TaskStateWrapper::Deposit(state) => <DepositTaskState as TaskState>::committed(state),
            TaskStateWrapper::CreateBalance(state) => {
                <CreateBalanceTaskState as TaskState>::committed(state)
            },
            TaskStateWrapper::CreateOrder(state) => {
                <CreateOrderTaskState as TaskState>::committed(state)
            },
            TaskStateWrapper::CancelOrder(state) => {
                <CancelOrderTaskState as TaskState>::committed(state)
            },
            TaskStateWrapper::RefreshAccount(state) => {
                <RefreshAccountTaskState as TaskState>::committed(state)
            },
            TaskStateWrapper::SettleInternalMatch(state) => {
                <SettleInternalMatchTaskState as TaskState>::committed(state)
            },
            TaskStateWrapper::SettleExternalMatch(state) => {
                <SettleExternalMatchTaskState as TaskState>::committed(state)
            },
            TaskStateWrapper::SettlePrivateMatch(state) => {
                <SettlePrivateMatchTaskState as TaskState>::committed(state)
            },
            TaskStateWrapper::Withdraw(state) => <WithdrawTaskState as TaskState>::committed(state),
        }
    }

    /// Whether or not this state commits the task, i.e. is the first state that
    /// for which `committed` is true
    pub fn is_committing(&self) -> bool {
        match self {
            TaskStateWrapper::CreateNewAccount(state) => {
                *state == CreateNewAccountTaskState::commit_point()
            },
            TaskStateWrapper::NodeStartup(state) => *state == NodeStartupTaskState::commit_point(),
            TaskStateWrapper::Deposit(state) => *state == DepositTaskState::commit_point(),
            TaskStateWrapper::CreateBalance(state) => {
                *state == CreateBalanceTaskState::commit_point()
            },
            TaskStateWrapper::CreateOrder(state) => *state == CreateOrderTaskState::commit_point(),
            TaskStateWrapper::CancelOrder(state) => *state == CancelOrderTaskState::commit_point(),
            TaskStateWrapper::RefreshAccount(state) => {
                *state == RefreshAccountTaskState::commit_point()
            },
            TaskStateWrapper::SettleInternalMatch(state) => {
                *state == SettleInternalMatchTaskState::commit_point()
            },
            TaskStateWrapper::SettleExternalMatch(state) => {
                *state == SettleExternalMatchTaskState::commit_point()
            },
            TaskStateWrapper::SettlePrivateMatch(state) => {
                *state == SettlePrivateMatchTaskState::commit_point()
            },
            TaskStateWrapper::Withdraw(state) => *state == WithdrawTaskState::commit_point(),
        }
    }

    /// Whether the underlying state is completed or not
    pub fn completed(&self) -> bool {
        match self {
            TaskStateWrapper::CreateNewAccount(state) => {
                <CreateNewAccountTaskState as TaskState>::completed(state)
            },
            TaskStateWrapper::NodeStartup(state) => {
                <NodeStartupTaskState as TaskState>::completed(state)
            },
            TaskStateWrapper::Deposit(state) => <DepositTaskState as TaskState>::completed(state),
            TaskStateWrapper::CreateBalance(state) => {
                <CreateBalanceTaskState as TaskState>::completed(state)
            },
            TaskStateWrapper::CreateOrder(state) => {
                <CreateOrderTaskState as TaskState>::completed(state)
            },
            TaskStateWrapper::CancelOrder(state) => {
                <CancelOrderTaskState as TaskState>::completed(state)
            },
            TaskStateWrapper::RefreshAccount(state) => {
                <RefreshAccountTaskState as TaskState>::completed(state)
            },
            TaskStateWrapper::SettleInternalMatch(state) => {
                <SettleInternalMatchTaskState as TaskState>::completed(state)
            },
            TaskStateWrapper::SettleExternalMatch(state) => {
                <SettleExternalMatchTaskState as TaskState>::completed(state)
            },
            TaskStateWrapper::SettlePrivateMatch(state) => {
                <SettlePrivateMatchTaskState as TaskState>::completed(state)
            },
            TaskStateWrapper::Withdraw(state) => <WithdrawTaskState as TaskState>::completed(state),
        }
    }
}

/// Deserialize a concrete task-state payload from a queued task state
pub fn decode_task_state<T>(state: &QueuedTaskState) -> Result<Option<T>, serde_json::Error>
where
    T: for<'de> Deserialize<'de>,
{
    match state.execution_state() {
        Some(serialized) => {
            let wrapper: serde_json::Value = serde_json::from_str(serialized)?;
            match wrapper.get("state") {
                Some(inner) => serde_json::from_value(inner.clone()).map(Some),
                None => Ok(None),
            }
        },
        None => Ok(None),
    }
}

impl Display for TaskStateWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStateWrapper::CreateNewAccount(state) => write!(f, "{state}"),
            TaskStateWrapper::NodeStartup(state) => write!(f, "{state}"),
            TaskStateWrapper::Deposit(state) => write!(f, "{state}"),
            TaskStateWrapper::CreateBalance(state) => write!(f, "{state}"),
            TaskStateWrapper::CreateOrder(state) => write!(f, "{state}"),
            TaskStateWrapper::CancelOrder(state) => write!(f, "{state}"),
            TaskStateWrapper::RefreshAccount(state) => write!(f, "{state}"),
            TaskStateWrapper::SettleInternalMatch(state) => write!(f, "{state}"),
            TaskStateWrapper::SettleExternalMatch(state) => write!(f, "{state}"),
            TaskStateWrapper::SettlePrivateMatch(state) => write!(f, "{state}"),
            TaskStateWrapper::Withdraw(state) => write!(f, "{state}"),
        }
    }
}

impl From<TaskStateWrapper> for QueuedTaskState {
    fn from(value: TaskStateWrapper) -> Self {
        // Serialize the state into a string
        let description = value.to_string();
        let committed = value.committed();
        let execution_state =
            Some(serde_json::to_string(&value).expect("task state serialization should not fail"));
        QueuedTaskState::Running { state: description, committed, execution_state }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tasks::create_order::CreateOrderTaskState;

    #[test]
    fn test_running_state_roundtrips_execution_payload() {
        let wrapper = TaskStateWrapper::CreateOrder(CreateOrderTaskState::Creating);
        let queued_state: QueuedTaskState = wrapper.clone().into();

        match &queued_state {
            QueuedTaskState::Running { state, committed, execution_state } => {
                assert_eq!(state, "Creating");
                assert!(*committed);
                assert!(execution_state.is_some());
            },
            other => panic!("expected running state, got {other:?}"),
        }

        let decoded_wrapper = TaskStateWrapper::from_queued_task_state(&queued_state)
            .expect("queued task state should decode")
            .expect("execution payload should be present");
        assert!(matches!(decoded_wrapper, TaskStateWrapper::CreateOrder(CreateOrderTaskState::Creating)));

        let decoded_concrete = decode_task_state::<CreateOrderTaskState>(&queued_state)
            .expect("concrete task state should decode")
            .expect("execution payload should be present");
        assert_eq!(decoded_concrete, CreateOrderTaskState::Creating);
    }

    #[test]
    fn test_missing_execution_payload_decodes_to_none() {
        let queued_state = QueuedTaskState::Running {
            state: "Pending".to_string(),
            committed: false,
            execution_state: None,
        };

        assert!(TaskStateWrapper::from_queued_task_state(&queued_state)
            .expect("missing payload should not error")
            .is_none());
        assert!(decode_task_state::<CreateOrderTaskState>(&queued_state)
            .expect("missing payload should not error")
            .is_none());
    }
}
