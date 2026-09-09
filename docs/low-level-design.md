# DurableFlow Low-Level Design

The diagrams below follow the current Go code. Boxes represent structs and interfaces; sequence arrows name the methods that coordinate each feature. Transaction notes identify which writes commit together.

## Domain classes

A definition describes reusable steps. An execution is one run of that definition; a task instance is one step in that run, and attempts record individual tries. Fields below are selected from [domain/models.go](../internal/domain/models.go); timestamps and error fields are omitted for space.

```mermaid
classDiagram
    direction TB
    class WorkflowDefinition {
        +string ID
        +string Name
        +int Version
        +RawMessage DefinitionJSON
    }
    class WorkflowDefinitionSpec {
        +string EntryTask
        +WorkflowTaskSpec[] Tasks
    }
    class WorkflowTaskSpec {
        +string Name
        +string HandlerKey
        +string NextTask
        +int MaxAttempts
        +int BackoffSeconds
    }
    class WorkflowExecution {
        +string ID
        +string WorkflowDefinitionID
        +string Status
        +RawMessage InputJSON
        +RawMessage OutputJSON
    }
    class TaskInstance {
        +string ID
        +string WorkflowExecutionID
        +string TaskName
        +string HandlerKey
        +string Status
        +int AttemptsTotal
        +string IdempotencyKey
        +Time NextRunAt
    }
    class TaskAttempt {
        +string ID
        +string TaskInstanceID
        +int AttemptNumber
        +string Status
    }
    class ExecutionStartResult
    class ExecutionSnapshot
    class TaskSnapshot
    WorkflowDefinition ..> WorkflowDefinitionSpec : JSON parsed into
    WorkflowDefinitionSpec "1" *-- "1..*" WorkflowTaskSpec : Tasks
    WorkflowDefinition "1" <-- "0..*" WorkflowExecution : WorkflowDefinitionID
    WorkflowExecution "1" <-- "0..*" TaskInstance : WorkflowExecutionID
    TaskInstance "1" <-- "0..*" TaskAttempt : TaskInstanceID
    ExecutionStartResult *-- WorkflowExecution : Execution
    ExecutionStartResult *-- TaskInstance : Task
    ExecutionSnapshot *-- WorkflowExecution : Execution
    ExecutionSnapshot *-- "0..*" TaskSnapshot : Tasks
    TaskSnapshot *-- TaskInstance : Task
    TaskSnapshot *-- "0..*" TaskAttempt : Attempts
```

The ID arrows represent references, not in-memory parent objects. `NextRunAt` is nullable in Go. `NextTask` names another step in the same definition, and that step receives the preceding handler's output as input. The persisted task key is `executionID:taskName`; replay preserves it.
