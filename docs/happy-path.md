# Happy Path Notes

The simplest flow in DurableFlow is:

1. Create a workflow definition.
2. Trigger an execution.
3. Persist the execution, first task, and outbox row.
4. Publish the task through Redis Streams.
5. Consume it in the worker.
6. Run the handler.
7. Mark task and execution successful.

Useful code entry points:

- [apps/api/main.go](../apps/api/main.go)
- [apps/worker/main.go](../apps/worker/main.go)
- [internal/orchestrator/service.go](../internal/orchestrator/service.go)
- [internal/outbox/publisher.go](../internal/outbox/publisher.go)
- [internal/queue/redis_streams.go](../internal/queue/redis_streams.go)
- [internal/orchestrator/worker.go](../internal/orchestrator/worker.go)
