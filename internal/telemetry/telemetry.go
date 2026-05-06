package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var (
	HTTPRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "durableflow_http_requests_total",
			Help: "Total HTTP requests served by DurableFlow services.",
		},
		[]string{"service", "route", "method"},
	)

	WorkflowDefinitionsCreated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "durableflow_workflow_definitions_created_total",
			Help: "Total workflow definitions created.",
		},
		[]string{"service"},
	)

	WorkflowExecutionsCreated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "durableflow_workflow_executions_created_total",
			Help: "Total workflow executions triggered.",
		},
		[]string{"service"},
	)

	DispatchEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "durableflow_dispatch_events_total",
			Help: "Total task dispatch attempts from the outbox publisher.",
		},
		[]string{"service", "status"},
	)

	TasksProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "durableflow_tasks_processed_total",
			Help: "Total task processing attempts by handler and result.",
		},
		[]string{"service", "handler", "status"},
	)
)

func Setup(ctx context.Context, serviceName, endpoint string, logger *slog.Logger) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	if endpoint == "" {
		tp := sdktrace.NewTracerProvider()
		otel.SetTracerProvider(tp)
		return tp.Shutdown, nil
	}

	options := []otlptracehttp.Option{}
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
		options = append(options, otlptracehttp.WithEndpoint(parsed.Host))
		if parsed.Scheme == "http" {
			options = append(options, otlptracehttp.WithInsecure())
		}
	}

	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	logger.Info("telemetry configured", "service", serviceName, "traces", endpoint)
	return tp.Shutdown, nil
}

func Middleware(service string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := r.URL.Path
		HTTPRequests.WithLabelValues(service, route, r.Method).Inc()

		ctx, span := otel.Tracer(service).Start(r.Context(), route)
		defer span.End()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
