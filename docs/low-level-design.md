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

## Dispatch models

The publisher decodes an outbox payload into `domain.DispatchTaskPayload`, then copies its identifiers into `queue.TaskMessage`. The worker loads the task's input and state from Postgres.

```mermaid
classDiagram
    direction TB
    class OutboxEvent {
        +string ID
        +string AggregateType
        +string AggregateID
        +string EventType
        +RawMessage PayloadJSON
        +Time AvailableAt
        +Time DispatchedAt
        +int AttemptCount
    }
    class DispatchTaskPayload {
        +string TaskID
        +string ExecutionID
        +string HandlerKey
    }
    class TaskMessage {
        +string TaskID
        +string ExecutionID
        +string HandlerKey
    }
    OutboxEvent ..> DispatchTaskPayload : PayloadJSON decodes into
    DispatchTaskPayload ..> TaskMessage : Publisher maps identifiers
```

`DispatchedAt` is nullable. `AggregateID` refers to the task; Redis serializes `TaskMessage` as JSON in the stream entry's `payload` field. Source: [domain models](../internal/domain/models.go), [publisher](../internal/outbox/publisher.go), and [queue models](../internal/queue/redis_streams.go).

## Handler strategy and persistence contract

`Worker` selects a handler using the persisted task's `HandlerKey`. Both built-in implementations satisfy the same contract. `Registry` holds existing handler objects in a map; it does not construct a new handler for every delivery.

```mermaid
classDiagram
    direction TB
    class Worker {
        +HandleDispatchedTask(ctx, message) error
    }
    class Registry {
        -map handlers
        +Get(key) Handler, bool
    }
    class Handler {
        <<interface>>
        +Key() string
        +Handle(ctx, task) RawMessage, error
    }
    class SampleEchoHandler {
        +Key() string
        +Handle(ctx, task) RawMessage, error
    }
    class NotificationSendHandler {
        +Key() string
        +Handle(ctx, task) RawMessage, error
    }
    class idempotencyStore {
        <<interface>>
        +BeginIdempotentTask(ctx, handlerKey, key, ownerTaskID)
        +CompleteIdempotentTask(ctx, handlerKey, key, ownerTaskID, response) error
        +ReleaseIdempotentTask(ctx, handlerKey, key, ownerTaskID) error
    }
    class Store
    Worker --> Registry : Get task.HandlerKey
    Worker ..> Handler : Handle
    Registry "1" o-- "0..*" Handler : handlers
    SampleEchoHandler ..|> Handler
    NotificationSendHandler ..|> Handler
    SampleEchoHandler --> idempotencyStore
    NotificationSendHandler --> idempotencyStore
    Store ..|> idempotencyStore
```

Source: [registry](../internal/handlers/registry.go), [echo handler and persistence interface](../internal/handlers/sample_handler.go), [notification handler](../internal/handlers/notification_handler.go), and [idempotency store](../internal/db/idempotency.go).

## Dependency injection

Each `main` function creates the dependencies and passes them to constructors. The API and worker are separate processes with separate store and queue objects, connected to the same infrastructure.

### API process

```mermaid
flowchart TB
    AP["Postgres pool"] --> AS["db.NewStore(pool)"]
    AS --> SV["NewService(store, logger)"]
    SV --> RT["NewRouter(logger, service, healthFn)"]
    AS --> PU["NewPublisher(store, streams, interval, logger)"]
    AQ["NewRedisStreams(...)"] --> PU
    PU --> LOOP["Publisher.Run goroutine"]
```
