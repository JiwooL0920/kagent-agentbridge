package telemetry

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

const (
	defaultOTLPEndpoint = "localhost:4317"
	defaultVersion      = "dev"
)

func Init(ctx context.Context, serviceName string) (func(), error) {
	if envService := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); envService != "" {
		serviceName = envService
	}
	if serviceName == "" {
		serviceName = "kagent-agentbridge"
	}

	serviceVersion := strings.TrimSpace(os.Getenv("OTEL_SERVICE_VERSION"))
	if serviceVersion == "" {
		serviceVersion = defaultVersion
	}

	endpoint, insecure := parseEndpoint(getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", defaultOTLPEndpoint))

	exportOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	if insecure {
		exportOpts = append(exportOpts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, exportOpts...)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create otel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	shutdown := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}

	return shutdown, nil
}

func getEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parseEndpoint(raw string) (endpoint string, insecure bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultOTLPEndpoint, true
	}

	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err == nil && u.Host != "" {
			return u.Host, u.Scheme != "https"
		}
	}

	return raw, true
}
