# Postman setup

This folder contains:

- `DurableFlow.postman_collection.json`
- `DurableFlow.local.postman_environment.json`

## Import order

1. Import the environment.
2. Import the collection.
3. Select `DurableFlow Local`.

## Default endpoints

- API: `http://localhost:8080`
- Worker health: `http://localhost:8081`

## Suggested run order

### Health

1. `API health`
2. `Worker health`
3. `API metrics`
4. `Worker metrics`

### Happy path

1. `Create workflow definition`
2. `Trigger execution`
3. `Get execution snapshot`

Saved automatically:

- `workflowDefinitionId`
- `executionId`

### Dead-letter and replay

1. `Create dead-letter workflow`
2. `Trigger dead-letter execution`
3. `List dead-letter tasks`
4. `Replay dead-letter task`

Saved automatically where possible:

- `deadLetterWorkflowDefinitionId`
- `deadLetterExecutionId`
- `deadLetterTaskId`

## Notes

- Workflow names use timestamps to avoid duplicate-name failures.
- Replay only works for tasks already in `dead_lettered`.
- The collection includes both `/api/dead-letter-tasks` and `/api/dead-lettered-tasks`.
