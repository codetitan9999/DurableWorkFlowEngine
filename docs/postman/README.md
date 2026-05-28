# Postman setup

This folder contains a ready-to-import Postman collection and a local environment for DurableFlow.

## Files

- `DurableFlow.postman_collection.json`
- `DurableFlow.local.postman_environment.json`

## Import order

1. Import the environment file.
2. Import the collection file.
3. Select the `DurableFlow Local` environment in Postman.

## Default local endpoints

- API: `http://localhost:8080`
- Worker health: `http://localhost:8081`

## Suggested run order

### Basic health

1. `API health`
2. `Worker health`

### Successful linear workflow

1. `Create workflow definition`
2. `Trigger execution`
3. `Get execution snapshot`

The collection stores:

- `workflowDefinitionId`
- `executionId`

from the earlier responses automatically.

### Dead-letter and replay

1. `Create dead-letter workflow`
2. `Trigger dead-letter execution`
3. `List dead-letter tasks`
4. `Replay dead-letter task`

The collection stores:

- `deadLetterWorkflowDefinitionId`
- `deadLetterExecutionId`
- `deadLetterTaskId`

automatically where possible.

## Notes

- The `Create workflow definition` requests use timestamp-based names so repeated runs do not fail on duplicate names.
- Replay only works for tasks that are already `dead_lettered`.
