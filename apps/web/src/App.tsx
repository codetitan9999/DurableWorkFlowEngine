import { useEffect, useState } from "react";

type WorkflowDefinition = {
  id: string;
  name: string;
  description: string;
  definition: unknown;
};

type TaskInstance = {
  id: string;
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
      "handler_key": "sample.echo"
    }
  ]
}`;

const defaultExecutionInput = `{
  "customer_id": "demo-customer-123",
  "order_id": "demo-order-456"
}`;

function formatTimestamp(value?: string) {
  if (!value) {
    return "Not finished yet";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString();
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
    "Linear two-step workflow used to validate chaining, retries, and dispatch flow."
  );
  const [definitionText, setDefinitionText] = useState(defaultDefinition);
  const [inputText, setInputText] = useState(defaultExecutionInput);
  const [workflow, setWorkflow] = useState<WorkflowDefinition | null>(null);
  const [executionId, setExecutionId] = useState("");
  const [snapshot, setSnapshot] = useState<ExecutionSnapshot | null>(null);
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
              <p className="attempt-meta">
                Started: {formatTimestamp(snapshot.execution.started_at)}
              </p>
              <p className="attempt-meta">
                Completed: {formatTimestamp(snapshot.execution.completed_at)}
              </p>
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
      </main>
    </div>
  );
}
