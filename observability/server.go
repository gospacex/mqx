package observability

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/gospacex/mqx/observability/exporter/kafkatopic"
	"github.com/gospacex/mqx/observability/exporter/redisstream"
)

// 三个 trace backend 标识常量；与 OTel collector / mqx 测试矩阵对齐。
const (
	TraceBackendJaeger      = "jaeger"
	TraceBackendRedisStream = "redis_stream"
	TraceBackendKafkaTopic  = "kafka_topic"
)

// envVarPattern 匹配 ${env:VAR} 或 ${env:VAR:-default}（OTel confmap 兼容）。
// 测试场景里大量用 ${env:VAR:-xxx} 切换 broker / trace 端点。
var envVarPattern = regexp.MustCompile(`\$\{env:([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// ExpandEnvVars 替换字符串里的 ${env:VAR} 和 ${env:VAR:-default}（OTel confmap 兼容）。
// 命中环境变量优先；未设置时使用 default（若提供，且允许空串）；都没有则保留原 ${...} 字面量。
//
// 判定 "是否提供了 default" 的依据是原 match 是否含 ":-" 序列——比
// `sub[2] != ""` 更严格，避免误把 "未提供" 当作 "提供空 default"。
//
// 用法：driver 解析 yaml 拿到 *Config 后，对每个字符串字段调一次
//
//	cfg.JaegerEndpoint = observability.ExpandEnvVars(cfg.JaegerEndpoint)
//	cfg.Trace.Exporter = observability.ExpandEnvVars(cfg.Trace.Exporter)
//
// 详见 example/<driver>_test/e2e_test.go。
func ExpandEnvVars(s string) string {
	return envVarPattern.ReplaceAllStringFunc(s, func(match string) string {
		sub := envVarPattern.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		if v, ok := os.LookupEnv(sub[1]); ok {
			return v
		}
		// 用 ":-" 在原 match 里的存在性判断是否提供了 default；
		// `${env:VAR:-}` 这种空 default 也算提供了，返回 ""。
		if strings.Contains(match, ":-") {
			return sub[2]
		}
		return match
	})
}

var (
	tracerProvider *sdktrace.TracerProvider
	server         *http.Server
)

// Config Observability 服务配置
type Config struct {
	// Enabled 控制是否启动 OpenTelemetry tracer。
	// 当 Enabled 为 false 时，InitTracing 直接返回 no-op cleanup，
	// 不会创建 OTLP exporter、TracerProvider，也不会覆盖全局 TracerProvider。
	// 这样调用方可以无条件把配置传进来，由库自身根据 Enabled 决定是否启动调用链。
	Enabled        bool
	MetricsPort    int    // Prometheus metrics 端口号，默认 9091
	JaegerEndpoint string // Jaeger OTLP 端点，默认 localhost:4317
	ServiceName    string // 服务名，默认 "mqx"

	// Insecure 控制是否跳过 TLS 验证；生产环境必须为 false。
	Insecure bool
	// Headers 透传给 OTLP gRPC exporter；支持 Bearer Token / Basic Auth 等。
	Headers map[string]string

	// Backend 选择 trace 后端：jaeger（默认）/ redis_stream / kafka_topic。
	// 当 Backend=jaeger 时使用 JaegerEndpoint + Insecure + Headers 装配 OTLP gRPC exporter。
	// 当 Backend=redis_stream 时使用 RedisClient + RedisStream（要求非 nil）。
	// 当 Backend=kafka_topic 时使用 KafkaProducer + KafkaTopic（要求非 nil）。
	// 空值等同于 "jaeger"，保持向后兼容。
	Backend string

	// RedisClient 仅在 Backend=redis_stream 时使用。
	// 调用方管理 client 生命周期，exporter 不会 Close。
	RedisClient *redis.Client
	// RedisStream 是 XAdd 的目标 stream 名，如 "trace:redisx"。
	RedisStream string

	// KafkaProducer 仅在 Backend=kafka_topic 时使用。
	// 调用方管理 producer 生命周期，exporter 不会 Close。
	KafkaProducer *kafka.Producer
	// KafkaTopic 是 Produce 的目标 topic，如 "trace-spans-redisx"。
	KafkaTopic string
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		// 默认关闭，避免在未显式开启时启动任何外部依赖
		Enabled:        false,
		MetricsPort:    9091,
		JaegerEndpoint: "localhost:4317",
		ServiceName:    "mqx",
		Insecure:       true, // 默认 insecure 与原行为保持一致
	}
}

// InitTracing 初始化 OpenTelemetry tracer。
// 当 cfg.Enabled 为 false 时，函数立即返回一个 no-op cleanup。
//
// Backend 字段决定 trace 后端：
//   - "" / "jaeger"（默认）：走 OTLP gRPC → jaegerEndpoint，**与早期版本兼容**
//   - "redis_stream"：走 observability/exporter/redisstream（需 RedisClient + RedisStream 非 nil）
//   - "kafka_topic"：走 observability/exporter/kafkatopic（需 KafkaProducer + KafkaTopic 非 nil）
//
// 三种 backend 装配的 SpanExporter 都装到同一个 sdktrace.TracerProvider，
// 通过 sdktrace.WithBatcher 包成 batch 上报。Resource / Sampler /
// Propagator 装配逻辑三 backend 共享。
func InitTracing(ctx context.Context, cfg *Config) (func(context.Context), error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	// 关闭时直接返回 no-op：调用链不应启动
	if !cfg.Enabled {
		log.Println("[observability] tracing disabled, skipping OTel initialization")
		return func(ctx context.Context) {}, nil
	}

	spanExporter, err := buildSpanExporter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("observability: build span exporter: %w", err)
	}
	if spanExporter == nil {
		// buildSpanExporter 已经 log 了 warning；返回 no-op 避免启动失败。
		return func(ctx context.Context) {}, nil
	}

	// 创建 resource
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// 创建 TracerProvider
	tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(spanExporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	// 设置全局 tracer provider
	otel.SetTracerProvider(tracerProvider)

	// 设置全局 propagator
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("[observability] tracing initialized, backend=%s service=%s", cfg.Backend, cfg.ServiceName)

	return func(ctx context.Context) {
		log.Println("[observability] shutting down tracer provider...")
		if tracerProvider != nil {
			log.Println("[observability] force flushing spans...")
			if err := tracerProvider.ForceFlush(ctx); err != nil {
				log.Printf("[observability] force flush error: %v", err)
			}
			log.Println("[observability] waiting for export to complete...")
			time.Sleep(2 * time.Second)
			log.Println("[observability] force flush complete, shutting down...")
			if err := tracerProvider.Shutdown(ctx); err != nil {
				log.Printf("[observability] error shutting down tracer provider: %v", err)
			}
		}
	}, nil
}

// buildSpanExporter 根据 cfg.Backend 装配对应的 SpanExporter。
// backend 字段空值时默认走 jaeger 路径（向后兼容）。
// 返回 (nil, nil) 表示 exporter 创建失败但调用方决定继续（仅 log warning）。
func buildSpanExporter(ctx context.Context, cfg *Config) (sdktrace.SpanExporter, error) {
	switch cfg.Backend {
	case "", TraceBackendJaeger:
		jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
		if jaegerEndpoint == "" {
			jaegerEndpoint = cfg.JaegerEndpoint
		}
		exporterOpts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(jaegerEndpoint),
		}
		if cfg.Insecure {
			exporterOpts = append(exporterOpts, otlptracegrpc.WithInsecure())
		}
		if len(cfg.Headers) > 0 {
			exporterOpts = append(exporterOpts, otlptracegrpc.WithHeaders(cfg.Headers))
		}
		exporter, err := otlptracegrpc.New(ctx, exporterOpts...)
		if err != nil {
			log.Printf("[observability] warning: failed to create Jaeger exporter: %v", err)
			return nil, nil
		}
		return exporter, nil

	case TraceBackendRedisStream:
		if cfg.RedisClient == nil {
			return nil, fmt.Errorf("backend=redis_stream requires cfg.RedisClient != nil")
		}
		if cfg.RedisStream == "" {
			return nil, fmt.Errorf("backend=redis_stream requires cfg.RedisStream != \"\"")
		}
		exp, err := redisstream.New(cfg.RedisClient, cfg.RedisStream)
		if err != nil {
			return nil, fmt.Errorf("redisstream: %w", err)
		}
		return exp, nil

	case TraceBackendKafkaTopic:
		if cfg.KafkaProducer == nil {
			return nil, fmt.Errorf("backend=kafka_topic requires cfg.KafkaProducer != nil")
		}
		if cfg.KafkaTopic == "" {
			return nil, fmt.Errorf("backend=kafka_topic requires cfg.KafkaTopic != \"\"")
		}
		exp, err := kafkatopic.New(cfg.KafkaProducer, cfg.KafkaTopic)
		if err != nil {
			return nil, fmt.Errorf("kafkatopic: %w", err)
		}
		return exp, nil

	default:
		return nil, fmt.Errorf("unknown trace backend %q (want: %s | %s | %s)",
			cfg.Backend, TraceBackendJaeger, TraceBackendRedisStream, TraceBackendKafkaTopic)
	}
}

// StartMetricsServer启动 Prometheus metrics HTTP 服务
func StartMetricsServer(cfg *Config) *http.Server {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	mux := http.NewServeMux()

	// Prometheus metrics端点
	mux.Handle("/metrics", promhttp.Handler())

	// 健康检查端点
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	addr := fmt.Sprintf(":%d", cfg.MetricsPort)
	server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("[observability] starting metrics server on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[observability] metrics server error: %v", err)
		}
	}()

	return server
}

// ShutdownMetricsServer 关闭 metrics 服务
func ShutdownMetricsServer(ctx context.Context) error {
	if server == nil {
		return nil
	}
	log.Println("[observability] shutting down metrics server...")
	return server.Shutdown(ctx)
}

// GetTracer 返回 tracer 实例
func GetTracer(name string) interface {
	Trace(ctx context.Context, name string, fn func(ctx context.Context) error) error
} {
	return &tracerWrapper{name: name}
}

type tracerWrapper struct {
	name string
}

func (t *tracerWrapper) Trace(ctx context.Context, spanName string, fn func(ctx context.Context) error) error {
	tracer := otel.Tracer(t.name)
	ctx, span := tracer.Start(ctx, spanName)
	defer span.End()
	return fn(ctx)
}

// Shutdown 关停全局 OTel tracer provider；多次调用幂等。
// 通过 otel.GetTracerProvider 取出全局 TP；如该 TP 实现了 Shutdown(ctx) error，
// 则转发；否则返回 nil（兼容 no-op provider）。
//
// 用于 depth-4a 模拟 backend 不可达：调一次 Shutdown 后 BatchSpanProcessor
// 不再 flush，便于 e2e 验证"trace 断了后 producer 是否仍然能写"。
func Shutdown(ctx context.Context) error {
	tp := otel.GetTracerProvider()
	if s, ok := tp.(interface{ Shutdown(context.Context) error }); ok {
		return s.Shutdown(ctx)
	}
	return nil
}
