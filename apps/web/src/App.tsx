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
};

type ExecutionSnapshot = {
  execution: {
    id: string;
    workflow_definition_id: string;
    status: string;
    output?: unknown;
    error?: string;
  };
  tasks: TaskInstance[];
};

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";

const defaultDefinition = `{
  "entry_task": "sample-task",
  "tasks": [
    {
      "name": "sample-task",
      "handler_key": "sample.echo"
    }
  ]
}`;

const defaultExecutionInput = `{
  "customer_id": "demo-customer-123",
  "order_id": "demo-order-456"
}`;

export default function App() {
  const [workflowName, setWorkflowName] = useState("demo-order-approval");
  const [workflowDescription, setWorkflowDescription] = useState(
    "Thin starter workflow used to validate the DurableFlow vertical slice."
  );
  const [definitionText, setDefinitionText] = useState(defaultDefinition);
  const [inputText, setInputText] = useState(defaultExecutionInput);
  const [workflow, setWorkflow] = useState<WorkflowDefinition | null>(null);
  const [executionId, setExecutionId] = useState("");
  const [snapshot, setSnapshot] = useState<ExecutionSnapshot | null>(null);
  const [responseText, setResponseText] = useState("");
  const [errorText, setErrorText] = useState("");
  const [loading, setLoading] = useState<string | null>(null);

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
    setLoading("create-workflow");
    setErrorText("");

    try {
      const response = await fetch(`${apiBaseUrl}/api/workflows`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: workflowName,
          description: workflowDescription,
          definition: JSON.parse(definitionText)
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

    setLoading("trigger-execution");
    setErrorText("");

    try {
      const response = await fetch(`${apiBaseUrl}/api/executions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workflow_definition_id: workflow.id,
          input: JSON.parse(inputText)
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
            <li>Persist one task instance in Postgres</li>
            <li>Publish via outbox to Redis Streams</li>
            <li>Process with a mock handler</li>
            <li>Persist success back into Postgres</li>
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
          <button onClick={() => void createWorkflow()} disabled={loading !== null}>
            {loading === "create-workflow" ? "Creating..." : "Create Workflow"}
          </button>
        </section>

        <section className="panel">
          <h2>2. Trigger execution</h2>
          <p className="muted">
            The first pass always expands to one hardcoded sample task so you can implement
            real workflow graph expansion yourself later.
          </p>
          <label>
            Execution input JSON
            <textarea
              value={inputText}
              onChange={(event) => setInputText(event.target.value)}
              rows={8}
            />
          </label>
          <button
            onClick={() => void triggerExecution()}
            disabled={loading !== null || workflow === null}
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
              <div className="task-list">
                {snapshot.tasks.map((task) => (
                  <div className="task-card" key={task.id}>
                    <div className="snapshot-header">
                      <span>{task.task_name}</span>
                      <strong>{task.status}</strong>
                    </div>
                    <code>{task.handler_key}</code>
                    <p>Attempts: {task.attempts_total}</p>
                  </div>
                ))}
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

