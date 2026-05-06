# Happy Path Validation Notes

The current starter validates one intentionally thin path:

1. Create a workflow definition
2. Trigger an execution
3. Persist the execution and one task instance
4. Persist an outbox dispatch event
5. Publish that event to Redis Streams
6. Consume it in the worker
7. Run the `sample.echo` handler
8. Persist task and execution success

Useful places to inspect while learning:

- API bootstrapping: [apps/api/main.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/apps/api/main.go)
- Worker bootstrapping: [apps/worker/main.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/apps/worker/main.go)
- Execution creation: [internal/orchestrator/service.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/service.go)
- Outbox publishing: [internal/outbox/publisher.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/outbox/publisher.go)
- Task consumption: [internal/queue/redis_streams.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/queue/redis_streams.go)
- Task processing: [internal/orchestrator/worker.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/worker.go)

