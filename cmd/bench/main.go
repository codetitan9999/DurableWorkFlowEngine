package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	scenarioSuccessLinear            = "success-linear"
	scenarioSuccessDeepChain         = "success-deep-chain"
	scenarioRetryInvalidInput        = "retry-invalid-input"
	scenarioDeadLetterMissingHandler = "dead-letter-missing-handler"
	scenarioReplayMissingHandler     = "replay-missing-handler"
)

type workflowCreateRequest struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Definition  json.RawMessage `json:"definition"`
}

type workflowCreateResponse struct {
	ID string `json:"id"`
}

type executionTriggerRequest struct {
	WorkflowDefinitionID string          `json:"workflow_definition_id"`
	Input                json.RawMessage `json:"input"`
}

type executionTriggerResponse struct {
	Execution struct {
		ID string `json:"id"`
	} `json:"execution"`
}

type executionSnapshot struct {
	Execution struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		StartedAt   string `json:"started_at"`
		CompletedAt string `json:"completed_at"`
	} `json:"execution"`
	Tasks []struct {
		Task struct {
			ID            string `json:"id"`
			Status        string `json:"status"`
			AttemptsTotal int    `json:"attempts_total"`
		} `json:"task"`
		Attempts []struct {
			Status string `json:"status"`
		} `json:"attempts"`
	} `json:"tasks"`
}

type executionRun struct {
	ExecutionID       string
	FinalStatus       string
	ObservedLatency   time.Duration
	ReportedLatency   time.Duration
	AttemptsObserved  int
	TerminalTaskCount int
}

type benchmarkResult struct {
	Scenario             string         `json:"scenario"`
	WorkflowDefinitionID string         `json:"workflow_definition_id"`
	Executions           int            `json:"executions"`
	Concurrency          int            `json:"concurrency"`
	OverallElapsed       time.Duration  `json:"overall_elapsed"`
	ThroughputPerSecond  float64        `json:"throughput_per_second"`
	Succeeded            int            `json:"succeeded"`
	Failed               int            `json:"failed"`
	ObservedLatency      latencySummary `json:"observed_latency"`
	ReportedLatency      latencySummary `json:"reported_latency"`
	AttemptsPerExecution numberSummary  `json:"attempts_per_execution"`
	Runs                 []executionRun `json:"runs,omitempty"`
}

type latencySummary struct {
	Count int           `json:"count"`
	Min   time.Duration `json:"min"`
	Avg   time.Duration `json:"avg"`
	P50   time.Duration `json:"p50"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`
	Max   time.Duration `json:"max"`
}

type numberSummary struct {
	Count int     `json:"count"`
	Min   int     `json:"min"`
	Avg   float64 `json:"avg"`
	P50   int     `json:"p50"`
	P95   int     `json:"p95"`
	P99   int     `json:"p99"`
	Max   int     `json:"max"`
}

type runner struct {
	client       *http.Client
	apiBaseURL   string
	pollInterval time.Duration
	timeout      time.Duration
}

func main() {
	var (
		apiBaseURL   = flag.String("api-base-url", "http://localhost:8080", "DurableFlow API base URL")
		scenario     = flag.String("scenario", scenarioSuccessLinear, "Benchmark scenario")
		executions   = flag.Int("executions", 25, "Total executions to trigger")
		concurrency  = flag.Int("concurrency", 5, "Concurrent execution triggers/pollers")
		pollInterval = flag.Duration("poll-interval", 200*time.Millisecond, "Polling interval for execution snapshots")
		timeout      = flag.Duration("timeout", 30*time.Second, "Per-execution timeout")
		maxAttempts  = flag.Int("max-attempts", 3, "Task max_attempts for generated workflow definition")
		backoff      = flag.Int("backoff-seconds", 1, "Task backoff_seconds for generated workflow definition")
		chainLength  = flag.Int("chain-length", 5, "Number of tasks in deep-chain scenario")
		includeRuns  = flag.Bool("include-runs", false, "Include per-execution results in JSON output")
		jsonOutput   = flag.Bool("json", false, "Emit machine-readable JSON output")
	)
	flag.Parse()

	if *executions <= 0 {
		exitf("executions must be greater than 0")
	}
	if *concurrency <= 0 {
		exitf("concurrency must be greater than 0")
	}
	if *maxAttempts <= 0 {
		exitf("max-attempts must be greater than 0")
	}
	if *backoff < 0 {
		exitf("backoff-seconds must be at least 0")
	}
	if *chainLength < 2 {
		exitf("chain-length must be at least 2")
	}

	spec, err := buildScenarioSpec(*scenario, *maxAttempts, *backoff, *chainLength)
	if err != nil {
		exitf("%v", err)
	}

	r := &runner{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		apiBaseURL:   strings.TrimRight(*apiBaseURL, "/"),
		pollInterval: *pollInterval,
		timeout:      *timeout,
	}

	ctx := context.Background()
	workflowName := fmt.Sprintf("bench-%s-%d", *scenario, time.Now().UTC().UnixNano())
	workflowID, err := r.createWorkflow(ctx, workflowName, spec.Description, spec.Definition)
	if err != nil {
		exitf("create benchmark workflow: %v", err)
	}

	result, err := r.runBenchmark(ctx, benchmarkConfig{
		Scenario:             *scenario,
		WorkflowDefinitionID: workflowID,
		Executions:           *executions,
		Concurrency:          *concurrency,
		IncludeRuns:          *includeRuns,
		Input:                spec.Input,
		ReplayAfterTerminal:  spec.ReplayAfterTerminal,
	})
	if err != nil {
		exitf("run benchmark: %v", err)
	}

	if *jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result); err != nil {
			exitf("encode result: %v", err)
		}
		return
	}

	printHumanSummary(result)
}

type benchmarkConfig struct {
	Scenario             string
	WorkflowDefinitionID string
	Executions           int
	Concurrency          int
	IncludeRuns          bool
	Input                json.RawMessage
	ReplayAfterTerminal  bool
}

func (r *runner) runBenchmark(ctx context.Context, cfg benchmarkConfig) (benchmarkResult, error) {
	inputs := make([]json.RawMessage, cfg.Executions)
	for i := 0; i < cfg.Executions; i++ {
		if len(cfg.Input) != 0 {
			inputs[i] = append(json.RawMessage(nil), cfg.Input...)
			continue
		}
		payload, err := json.Marshal(map[string]any{
			"benchmark_run": i + 1,
			"scenario":      cfg.Scenario,
			"requested_at":  time.Now().UTC().Format(time.RFC3339Nano),
		})
		if err != nil {
			return benchmarkResult{}, err
		}
		inputs[i] = payload
	}

	results := make([]executionRun, cfg.Executions)
	var (
		wg      sync.WaitGroup
		sem     = make(chan struct{}, cfg.Concurrency)
		errOnce sync.Once
		runErr  error
	)

	started := time.Now()
	for i := range inputs {
		wg.Add(1)
		sem <- struct{}{}
		go func(index int) {
			defer wg.Done()
			defer func() { <-sem }()

			run, err := r.runExecution(ctx, cfg.WorkflowDefinitionID, inputs[index], cfg.ReplayAfterTerminal)
			if err != nil {
				errOnce.Do(func() { runErr = err })
				return
			}
			results[index] = run
		}(i)
	}
	wg.Wait()

	if runErr != nil {
		return benchmarkResult{}, runErr
	}

	overallElapsed := time.Since(started)
	result := benchmarkResult{
		Scenario:             cfg.Scenario,
		WorkflowDefinitionID: cfg.WorkflowDefinitionID,
		Executions:           cfg.Executions,
		Concurrency:          cfg.Concurrency,
		OverallElapsed:       overallElapsed,
		ThroughputPerSecond:  float64(cfg.Executions) / overallElapsed.Seconds(),
		ObservedLatency:      summarizeDurations(extractDurations(results, func(run executionRun) time.Duration { return run.ObservedLatency })),
		ReportedLatency:      summarizeDurations(extractDurations(results, func(run executionRun) time.Duration { return run.ReportedLatency })),
		AttemptsPerExecution: summarizeInts(extractInts(results, func(run executionRun) int { return run.AttemptsObserved })),
	}

	for _, run := range results {
		switch run.FinalStatus {
		case "succeeded":
			result.Succeeded++
		case "failed":
			result.Failed++
		}
	}

	if cfg.IncludeRuns {
		result.Runs = results
	}

	return result, nil
}

func (r *runner) createWorkflow(ctx context.Context, name, description string, definition json.RawMessage) (string, error) {
	reqBody, err := json.Marshal(workflowCreateRequest{
		Name:        name,
		Description: description,
		Definition:  definition,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.apiBaseURL+"/api/workflows", bytes.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("unexpected status %d creating workflow: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var created workflowCreateResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return "", err
	}
	if strings.TrimSpace(created.ID) == "" {
		return "", errors.New("workflow creation response missing id")
	}
	return created.ID, nil
}

func (r *runner) runExecution(ctx context.Context, workflowID string, input json.RawMessage, replayAfterTerminal bool) (executionRun, error) {
	observedStart := time.Now()
	reqBody, err := json.Marshal(executionTriggerRequest{
		WorkflowDefinitionID: workflowID,
		Input:                input,
	})
	if err != nil {
		return executionRun{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.apiBaseURL+"/api/executions", bytes.NewReader(reqBody))
	if err != nil {
		return executionRun{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return executionRun{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return executionRun{}, fmt.Errorf("unexpected status %d triggering execution: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var triggered executionTriggerResponse
	if err := json.NewDecoder(resp.Body).Decode(&triggered); err != nil {
		return executionRun{}, err
	}
	if strings.TrimSpace(triggered.Execution.ID) == "" {
		return executionRun{}, errors.New("execution trigger response missing execution id")
	}

	pollCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		snapshot, err := r.getExecutionSnapshot(pollCtx, triggered.Execution.ID)
		if err != nil {
			return executionRun{}, err
		}

		if isTerminal(snapshot.Execution.Status) {
			if replayAfterTerminal {
				taskID, err := firstDeadLetteredTaskID(snapshot)
				if err != nil {
					return executionRun{}, err
				}
				if err := r.replayTask(pollCtx, taskID); err != nil {
					return executionRun{}, err
				}
				replayAfterTerminal = false
				continue
			}

			reportedLatency, err := parseReportedLatency(snapshot)
			if err != nil {
				return executionRun{}, err
			}

			return executionRun{
				ExecutionID:       snapshot.Execution.ID,
				FinalStatus:       snapshot.Execution.Status,
				ObservedLatency:   time.Since(observedStart),
				ReportedLatency:   reportedLatency,
				AttemptsObserved:  totalAttempts(snapshot),
				TerminalTaskCount: countTerminalTasks(snapshot),
			}, nil
		}

		select {
		case <-pollCtx.Done():
			return executionRun{}, fmt.Errorf("execution %s did not complete before timeout", triggered.Execution.ID)
		case <-ticker.C:
		}
	}
}

func (r *runner) getExecutionSnapshot(ctx context.Context, executionID string) (executionSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.apiBaseURL+"/api/executions/"+executionID, nil)
	if err != nil {
		return executionSnapshot{}, err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return executionSnapshot{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return executionSnapshot{}, fmt.Errorf("unexpected status %d fetching snapshot: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var snapshot executionSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return executionSnapshot{}, err
	}
	return snapshot, nil
}

type scenarioSpec struct {
	Definition          json.RawMessage
	Description         string
	Input               json.RawMessage
	ReplayAfterTerminal bool
}

func buildScenarioSpec(scenario string, maxAttempts, backoffSeconds, chainLength int) (scenarioSpec, error) {
	switch scenario {
	case scenarioSuccessLinear:
		payload, err := json.Marshal(map[string]any{
			"entry_task": "validate-order",
			"tasks": []map[string]any{
				{
					"name":            "validate-order",
					"handler_key":     "sample.echo",
					"max_attempts":    maxAttempts,
					"backoff_seconds": backoffSeconds,
					"next_task":       "send-confirmation",
				},
				{
					"name":            "send-confirmation",
					"handler_key":     "notifications.send",
					"max_attempts":    maxAttempts,
					"backoff_seconds": backoffSeconds,
				},
			},
		})
		return scenarioSpec{
			Definition:  payload,
			Description: "Benchmark workflow for steady-state success latency and throughput.",
		}, err
	case scenarioSuccessDeepChain:
		tasks := make([]map[string]any, 0, chainLength)
		for i := 0; i < chainLength; i++ {
			name := fmt.Sprintf("step-%d", i+1)
			handler := "sample.echo"
			if i%2 == 1 {
				handler = "notifications.send"
			}
			task := map[string]any{
				"name":            name,
				"handler_key":     handler,
				"max_attempts":    maxAttempts,
				"backoff_seconds": backoffSeconds,
			}
			if i < chainLength-1 {
				task["next_task"] = fmt.Sprintf("step-%d", i+2)
			}
			tasks = append(tasks, task)
		}
		payload, err := json.Marshal(map[string]any{
			"entry_task": "step-1",
			"tasks":      tasks,
		})
		return scenarioSpec{
			Definition:  payload,
			Description: fmt.Sprintf("Benchmark workflow for %d-step chained execution latency and throughput.", chainLength),
		}, err
	case scenarioRetryInvalidInput:
		payload, err := json.Marshal(map[string]any{
			"entry_task": "retry-step",
			"tasks": []map[string]any{
				{
					"name":            "retry-step",
					"handler_key":     "sample.echo",
					"max_attempts":    maxAttempts,
					"backoff_seconds": backoffSeconds,
				},
			},
		})
		return scenarioSpec{
			Definition:  payload,
			Description: "Benchmark workflow for persisted retries and eventual dead-letter after invalid handler input.",
			Input:       json.RawMessage(`123`),
		}, err
	case scenarioDeadLetterMissingHandler:
		payload, err := json.Marshal(map[string]any{
			"entry_task": "broken-step",
			"tasks": []map[string]any{
				{
					"name":            "broken-step",
					"handler_key":     "missing.handler",
					"max_attempts":    maxAttempts,
					"backoff_seconds": backoffSeconds,
				},
			},
		})
		return scenarioSpec{
			Definition:  payload,
			Description: "Benchmark workflow for immediate terminal dead-letter behavior caused by a missing handler.",
		}, err
	case scenarioReplayMissingHandler:
		payload, err := json.Marshal(map[string]any{
			"entry_task": "broken-step",
			"tasks": []map[string]any{
				{
					"name":            "broken-step",
					"handler_key":     "missing.handler",
					"max_attempts":    maxAttempts,
					"backoff_seconds": backoffSeconds,
				},
			},
		})
		return scenarioSpec{
			Definition:          payload,
			Description:         "Benchmark workflow for dead-letter replay overhead through the normal dispatch path.",
			ReplayAfterTerminal: true,
		}, err
	default:
		return scenarioSpec{}, fmt.Errorf("unsupported scenario %q", scenario)
	}
}

func (r *runner) replayTask(ctx context.Context, taskID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.apiBaseURL+"/api/tasks/"+taskID+"/replay", nil)
	if err != nil {
		return err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d replaying task: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func firstDeadLetteredTaskID(snapshot executionSnapshot) (string, error) {
	for _, task := range snapshot.Tasks {
		if task.Task.Status == "dead_lettered" {
			return task.Task.ID, nil
		}
	}
	return "", errors.New("no dead-lettered task found for replay")
}

func parseReportedLatency(snapshot executionSnapshot) (time.Duration, error) {
	if snapshot.Execution.StartedAt == "" || snapshot.Execution.CompletedAt == "" {
		return 0, errors.New("snapshot missing started_at or completed_at")
	}
	startedAt, err := time.Parse(time.RFC3339Nano, snapshot.Execution.StartedAt)
	if err != nil {
		return 0, fmt.Errorf("parse started_at: %w", err)
	}
	completedAt, err := time.Parse(time.RFC3339Nano, snapshot.Execution.CompletedAt)
	if err != nil {
		return 0, fmt.Errorf("parse completed_at: %w", err)
	}
	return completedAt.Sub(startedAt), nil
}

func totalAttempts(snapshot executionSnapshot) int {
	total := 0
	for _, task := range snapshot.Tasks {
		total += len(task.Attempts)
	}
	return total
}

func countTerminalTasks(snapshot executionSnapshot) int {
	count := 0
	for _, task := range snapshot.Tasks {
		if isTerminal(task.Task.Status) {
			count++
		}
	}
	return count
}

func isTerminal(status string) bool {
	switch status {
	case "succeeded", "failed", "dead_lettered":
		return true
	default:
		return false
	}
}

func extractDurations(runs []executionRun, getter func(executionRun) time.Duration) []time.Duration {
	values := make([]time.Duration, 0, len(runs))
	for _, run := range runs {
		values = append(values, getter(run))
	}
	return values
}

func extractInts(runs []executionRun, getter func(executionRun) int) []int {
	values := make([]int, 0, len(runs))
	for _, run := range runs {
		values = append(values, getter(run))
	}
	return values
}

func summarizeDurations(values []time.Duration) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}

	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, value := range sorted {
		total += value
	}

	return latencySummary{
		Count: len(sorted),
		Min:   sorted[0],
		Avg:   time.Duration(int64(total) / int64(len(sorted))),
		P50:   percentileDuration(sorted, 0.50),
		P95:   percentileDuration(sorted, 0.95),
		P99:   percentileDuration(sorted, 0.99),
		Max:   sorted[len(sorted)-1],
	}
}

func summarizeInts(values []int) numberSummary {
	if len(values) == 0 {
		return numberSummary{}
	}

	sorted := append([]int(nil), values...)
	sort.Ints(sorted)

	total := 0
	for _, value := range sorted {
		total += value
	}

	return numberSummary{
		Count: len(sorted),
		Min:   sorted[0],
		Avg:   float64(total) / float64(len(sorted)),
		P50:   percentileInt(sorted, 0.50),
		P95:   percentileInt(sorted, 0.95),
		P99:   percentileInt(sorted, 0.99),
		Max:   sorted[len(sorted)-1],
	}
}

func percentileDuration(sorted []time.Duration, quantile float64) time.Duration {
	return sorted[percentileIndex(len(sorted), quantile)]
}

func percentileInt(sorted []int, quantile float64) int {
	return sorted[percentileIndex(len(sorted), quantile)]
}

func percentileIndex(length int, quantile float64) int {
	if length == 1 {
		return 0
	}
	index := int(math.Ceil(quantile*float64(length))) - 1
	if index < 0 {
		return 0
	}
	if index >= length {
		return length - 1
	}
	return index
}

func printHumanSummary(result benchmarkResult) {
	fmt.Printf("Scenario: %s\n", result.Scenario)
	fmt.Printf("Workflow definition: %s\n", result.WorkflowDefinitionID)
	fmt.Printf("Executions: %d\n", result.Executions)
	fmt.Printf("Concurrency: %d\n", result.Concurrency)
	fmt.Printf("Succeeded: %d\n", result.Succeeded)
	fmt.Printf("Failed: %d\n", result.Failed)
	fmt.Printf("Overall elapsed: %s\n", result.OverallElapsed)
	fmt.Printf("Throughput: %.2f executions/sec\n", result.ThroughputPerSecond)
	fmt.Println()
	fmt.Println("Observed latency (trigger -> terminal snapshot)")
	printLatencySummary(result.ObservedLatency)
	fmt.Println()
	fmt.Println("Reported engine latency (started_at -> completed_at)")
	printLatencySummary(result.ReportedLatency)
	fmt.Println()
	fmt.Println("Attempts per execution")
	printNumberSummary(result.AttemptsPerExecution)
}

func printLatencySummary(summary latencySummary) {
	fmt.Printf("  count: %d\n", summary.Count)
	fmt.Printf("  min:   %s\n", summary.Min)
	fmt.Printf("  avg:   %s\n", summary.Avg)
	fmt.Printf("  p50:   %s\n", summary.P50)
	fmt.Printf("  p95:   %s\n", summary.P95)
	fmt.Printf("  p99:   %s\n", summary.P99)
	fmt.Printf("  max:   %s\n", summary.Max)
}

func printNumberSummary(summary numberSummary) {
	fmt.Printf("  count: %d\n", summary.Count)
	fmt.Printf("  min:   %d\n", summary.Min)
	fmt.Printf("  avg:   %.2f\n", summary.Avg)
	fmt.Printf("  p50:   %d\n", summary.P50)
	fmt.Printf("  p95:   %d\n", summary.P95)
	fmt.Printf("  p99:   %d\n", summary.P99)
	fmt.Printf("  max:   %d\n", summary.Max)
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
