import { useEffect, useState } from "react";

type WorkflowDefinition = {
  id: string;
  name: string;
  description: string;
  definition: unknown;
};

type TaskInstance = {
  id: string;
  workflow_execution_id: string;
  task_name: string;
  handler_key: string;
  status: string;
  attempts_total: number;
  output?: unknown;
  last_error?: string;
  next_run_at?: string;
  completed_at?: string;
  created_at?: string;
};

type TaskAttempt = {
  id: string;
  attempt_number: number;
  status: string;
  started_at: string;
  finished_at?: string;
  error?: string;
};

type TaskSnapshot = {
  task: TaskInstance;
  attempts: TaskAttempt[];
};

type ExecutionSnapshot = {
  execution: {
    id: string;
    workflow_definition_id: string;
    status: string;
    output?: unknown;
    error?: string;
    started_at?: string;
    completed_at?: string;
  };
  tasks: TaskSnapshot[];
};

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";

const defaultDefinition = `{
  "entry_task": "validate-order",
  "tasks": [
    {
      "name": "validate-order",
      "handler_key": "sample.echo",
      "next_task": "send-confirmation"
    },
    {
      "name": "send-confirmation",
      "handler_key": "notifications.send"
    }
  ]
}`;

const defaultExecutionInput = `{
  "customer_id": "demo-customer-123",
  "order_id": "demo-order-456"
}`;

function formatTimestamp(value?: string) {
  if (!value) {
    return "Not available";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString();
}

function getExecutionStateLabel(snapshot: ExecutionSnapshot["execution"]) {
  if (snapshot.status === "failed") {
    return "Failed permanently";
  }
  if (snapshot.status === "succeeded") {
    return "Completed";
  }
  return "In progress";
}

function getTaskStateLabel(task: TaskInstance) {
  if (task.status === "dead_lettered") {
    return "Needs attention";
  }
  if (task.status === "failed") {
    return "Failed permanently";
  }
  if (task.status === "succeeded") {
    return "Completed";
  }
  if (task.status === "running") {
    return "Running now";
  }
  if (task.status === "pending" && task.next_run_at) {
    return "Pending retry";
  }
  return "Queued";
}

function tryParseJSON(value: string): { value?: unknown; error?: string } {
  try {
    return { value: JSON.parse(value) };
  } catch (error) {
    return {
      error: error instanceof Error ? error.message : "Invalid JSON"
    };
  }
}

export default function App() {
  const [workflowName, setWorkflowName] = useState("demo-order-approval");
  const [workflowDescription, setWorkflowDescription] = useState(
    "Linear two-step workflow used to validate chaining, retries, dispatch flow, and handler-level idempotency."
  );
  const [definitionText, setDefinitionText] = useState(defaultDefinition);
  const [inputText, setInputText] = useState(defaultExecutionInput);
  const [workflow, setWorkflow] = useState<WorkflowDefinition | null>(null);
  const [executionId, setExecutionId] = useState("");
  const [snapshot, setSnapshot] = useState<ExecutionSnapshot | null>(null);
  const [deadLetterTasks, setDeadLetterTasks] = useState<TaskInstance[]>([]);
  const [deadLetterError, setDeadLetterError] = useState("");
  const [responseText, setResponseText] = useState("");
  const [errorText, setErrorText] = useState("");
  const [loading, setLoading] = useState<string | null>(null);
  const trimmedWorkflowName = workflowName.trim();
  const workflowNameError = trimmedWorkflowName ? "" : "Workflow name is required.";
  const definitionParseResult = tryParseJSON(definitionText);
  const inputParseResult = tryParseJSON(inputText);
  const definitionError = definitionParseResult.error
    ? `Definition JSON is invalid: ${definitionParseResult.error}`
    : "";
  const inputError = inputParseResult.error
    ? `Execution input JSON is invalid: ${inputParseResult.error}`
    : "";
  const createWorkflowDisabled =
    loading !== null || workflowNameError !== "" || definitionError !== "";
  const triggerExecutionDisabled =
    loading !== null || workflow === null || inputError !== "";

  async function loadDeadLetterTasks() {
    try {
      const response = await fetch(`${apiBaseUrl}/api/dead-letter-tasks?limit=10`);
      const data = (await response.json()) as TaskInstance[] | { error?: string };
      if (!response.ok) {
        throw new Error(
          "error" in data && data.error ? data.error : "Failed to load dead-lettered tasks"
        );
      }

      setDeadLetterTasks(data as TaskInstance[]);
      setDeadLetterError("");
    } catch (error) {
      setDeadLetterError(
        error instanceof Error ? error.message : "Failed to load dead-lettered tasks"
      );
    }
  }

  useEffect(() => {
    if (!executionId) {
      return;
    }

    let cancelled = false;
    const loadSnapshot = async () => {
      try {
        const response = await fetch(`${apiBaseUrl}/api/executions/${executionId}`);
        if (!response.ok) {
          throw new Error(`Failed to load execution ${executionId}`);
        }

        const data = (await response.json()) as ExecutionSnapshot;
        if (!cancelled) {
          setSnapshot(data);
        }
      } catch (error) {
        if (!cancelled) {
          setErrorText(error instanceof Error ? error.message : "Unknown error");
        }
      }
    };

    void loadSnapshot();
    const intervalId = window.setInterval(() => {
      void loadSnapshot();
    }, 2000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [executionId]);

  useEffect(() => {
    let cancelled = false;

    const refreshDeadLetterTasks = async () => {
      await loadDeadLetterTasks();
    };

    void refreshDeadLetterTasks();
    const intervalId = window.setInterval(() => {
      if (!cancelled) {
        void refreshDeadLetterTasks();
      }
    }, 5000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, []);

  async function replayDeadLetteredTask(task: TaskInstance) {
    setLoading(`replay-${task.id}`);
    setErrorText("");

    try {
      const response = await fetch(`${apiBaseUrl}/api/tasks/${task.id}/replay`, {
        method: "POST"
      });
      const data = (await response.json()) as TaskInstance | { error?: string };
      if (!response.ok) {
        throw new Error("error" in data && data.error ? data.error : "Failed to replay task");
      }

      const replayedTask = data as TaskInstance;
      setExecutionId(replayedTask.workflow_execution_id);
      setSnapshot(null);
      setResponseText(JSON.stringify(replayedTask, null, 2));
      await loadDeadLetterTasks();
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : "Unknown error");
    } finally {
      setLoading(null);
    }
  }

  async function createWorkflow() {
    if (workflowNameError) {
      setErrorText(workflowNameError);
      return;
    }

    if (definitionParseResult.error) {
      setErrorText(definitionError);
      return;
    }

    setLoading("create-workflow");
    setErrorText("");

    try {
      const response = await fetch(`${apiBaseUrl}/api/workflows`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: trimmedWorkflowName,
          description: workflowDescription,
          definition: definitionParseResult.value
        })
      });

      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error ?? "Failed to create workflow");
      }

      setWorkflow(data as WorkflowDefinition);
      setResponseText(JSON.stringify(data, null, 2));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : "Unknown error");
    } finally {
      setLoading(null);
    }
  }

  async function triggerExecution() {
    if (!workflow?.id) {
      setErrorText("Create a workflow first.");
      return;
    }

    if (inputParseResult.error) {
      setErrorText(inputError);
      return;
    }

    setLoading("trigger-execution");
    setErrorText("");

    try {
      const response = await fetch(`${apiBaseUrl}/api/executions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workflow_definition_id: workflow.id,
          input: inputParseResult.value
        })
      });

      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error ?? "Failed to trigger execution");
      }

      setExecutionId(data.execution.id as string);
      setSnapshot(null);
      setResponseText(JSON.stringify(data, null, 2));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : "Unknown error");
    } finally {
      setLoading(null);
    }
  }

  return (
    <div className="page-shell">
      <header className="hero">
        <div>
          <p className="eyebrow">Durable workflow engine starter</p>
          <h1>DurableFlow</h1>
          <p className="lede">
            This shell is intentionally small. It exists to validate the architecture, not
            to finish the product for you.
          </p>
        </div>
        <div className="hero-card">
          <h2>Current vertical slice</h2>
          <ul>
            <li>Create one workflow definition</li>
            <li>Trigger one execution</li>
            <li>Persist task instances in Postgres</li>
            <li>Publish each step through the outbox to Redis Streams</li>
            <li>Chain one successful task into the next task</li>
            <li>Retry failures with durable scheduling</li>
            <li>Finish the execution when the final task completes</li>
          </ul>
        </div>
      </header>

      <main className="grid">
        <section className="panel">
          <h2>1. Workflow definition</h2>
          <label>
            Name
            <input value={workflowName} onChange={(event) => setWorkflowName(event.target.value)} />
          </label>
          {workflowNameError ? <p className="validation-text">{workflowNameError}</p> : null}
          <label>
            Description
            <input
              value={workflowDescription}
              onChange={(event) => setWorkflowDescription(event.target.value)}
            />
          </label>
          <label>
            Definition JSON
            <textarea
              value={definitionText}
              onChange={(event) => setDefinitionText(event.target.value)}
              rows={10}
            />
          </label>
          {definitionError ? <p className="validation-text">{definitionError}</p> : null}
          <button onClick={() => void createWorkflow()} disabled={createWorkflowDisabled}>
            {loading === "create-workflow" ? "Creating..." : "Create Workflow"}
          </button>
        </section>

        <section className="panel">
          <h2>2. Trigger execution</h2>
          <p className="muted">
            This demo now supports a simple linear workflow: one task can enqueue the next
            task after it succeeds.
          </p>
          <label>
            Execution input JSON
            <textarea
              value={inputText}
              onChange={(event) => setInputText(event.target.value)}
              rows={8}
            />
          </label>
          {inputError ? <p className="validation-text">{inputError}</p> : null}
          <button
            onClick={() => void triggerExecution()}
            disabled={triggerExecutionDisabled}
          >
            {loading === "trigger-execution" ? "Triggering..." : "Trigger Execution"}
          </button>
          {workflow && (
            <div className="status-chip">
              Active workflow: <strong>{workflow.name}</strong>
            </div>
          )}
        </section>

        <section className="panel">
          <h2>3. Execution snapshot</h2>
          <p className="muted">Polled from the API every 2 seconds after you trigger.</p>
          {snapshot ? (
            <div className="snapshot">
              <div className="snapshot-header">
                <span>Execution</span>
                <strong>{snapshot.execution.status}</strong>
              </div>
              <code>{snapshot.execution.id}</code>
              <div className={`state-pill state-${snapshot.execution.status}`}>
                {getExecutionStateLabel(snapshot.execution)}
              </div>
              <p className="attempt-meta">
                Started: {formatTimestamp(snapshot.execution.started_at)}
              </p>
              <p className="attempt-meta">
                Completed: {formatTimestamp(snapshot.execution.completed_at)}
              </p>
              {snapshot.execution.error ? (
                <div className="attempt-error">{snapshot.execution.error}</div>
              ) : null}
              <div className="task-list">
                {snapshot.tasks.map((taskSnapshot, index) => {
                  const attempts = taskSnapshot.attempts ?? [];

                  return (
                    <div className="task-card" key={taskSnapshot.task.id}>
                      <div className="snapshot-header">
                        <span>
                          Step {index + 1}: {taskSnapshot.task.task_name}
                        </span>
                        <strong>{taskSnapshot.task.status}</strong>
                      </div>
                      <div className={`state-pill state-${taskSnapshot.task.status}`}>
                        {getTaskStateLabel(taskSnapshot.task)}
                      </div>
                      <code>{taskSnapshot.task.handler_key}</code>
                      <p>Attempts: {taskSnapshot.task.attempts_total}</p>
                      <p className="attempt-meta">
                        Created: {formatTimestamp(taskSnapshot.task.created_at)}
                      </p>
                      <p className="attempt-meta">
                        Completed: {formatTimestamp(taskSnapshot.task.completed_at)}
                      </p>
                      {taskSnapshot.task.next_run_at ? (
                        <p className="attempt-meta">
                          Retry due: {formatTimestamp(taskSnapshot.task.next_run_at)}
                        </p>
                      ) : null}
                      {taskSnapshot.task.last_error ? (
                        <div className="attempt-error">{taskSnapshot.task.last_error}</div>
                      ) : null}
                      {attempts.length > 0 && (
                        <div className="attempt-list">
                          {attempts.map((attempt) => (
                            <div className="attempt-card" key={attempt.id}>
                              <div className="snapshot-header">
                                <span>Attempt {attempt.attempt_number}</span>
                                <strong>{attempt.status}</strong>
                              </div>
                              <p className="attempt-meta">
                                Started: {formatTimestamp(attempt.started_at)}
                              </p>
                              <p className="attempt-meta">
                                Finished: {formatTimestamp(attempt.finished_at)}
                              </p>
                              {attempt.error ? (
                                <div className="attempt-error">{attempt.error}</div>
                              ) : null}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          ) : (
            <p className="muted">No execution loaded yet.</p>
          )}
        </section>

        <section className="panel">
          <h2>Latest response</h2>
          {errorText ? <div className="error-box">{errorText}</div> : null}
          <pre>{responseText || "Responses will appear here."}</pre>
        </section>

        <section className="panel">
          <h2>4. Needs attention</h2>
          <p className="muted">
            Dead-lettered tasks are listed here so you can inspect permanently failed
            work without digging through raw tables.
          </p>
          {deadLetterError ? <div className="error-box">{deadLetterError}</div> : null}
          {deadLetterTasks.length > 0 ? (
            <div className="task-list">
              {deadLetterTasks.map((task) => (
                <div className="task-card attention-card" key={task.id}>
                  <div className="snapshot-header">
                    <span>{task.task_name}</span>
                    <strong>{task.status}</strong>
                  </div>
                  <div className={`state-pill state-${task.status}`}>
                    {getTaskStateLabel(task)}
                  </div>
                  <code>{task.handler_key}</code>
                  <p className="attempt-meta">Execution: {task.workflow_execution_id}</p>
                  <p className="attempt-meta">Attempts: {task.attempts_total}</p>
                  <p className="attempt-meta">Created: {formatTimestamp(task.created_at)}</p>
                  <p className="attempt-meta">
                    Failed at: {formatTimestamp(task.completed_at)}
                  </p>
                  {task.last_error ? <div className="attempt-error">{task.last_error}</div> : null}
                  <button
                    className="secondary-button"
                    onClick={() => void replayDeadLetteredTask(task)}
                    disabled={loading !== null}
                  >
                    {loading === `replay-${task.id}` ? "Replaying..." : "Replay task"}
                  </button>
                </div>
              ))}
            </div>
          ) : (
            <p className="empty-state">No dead-lettered tasks right now.</p>
          )}
        </section>
      </main>
    </div>
  );
}
