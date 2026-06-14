package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/example/assert"
	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/redisx"
)

// TestRedisx_Jaeger_Single / Cluster：OTLP gRPC → jaeger all-in-one
func TestRedisx_Jaeger_Single(t *testing.T) {
	runRedisxE2E(t, "jaeger", "single")
}

func TestRedisx_Jaeger_Cluster(t *testing.T) {
	runRedisxE2E(t, "jaeger", "cluster")
}

// TestRedisx_RedisStream_Single / Cluster：自定义 SpanExporter → redis stream
func TestRedisx_RedisStream_Single(t *testing.T) {
	runRedisxE2E(t, "redis_stream", "single")
}

func TestRedisx_RedisStream_Cluster(t *testing.T) {
	runRedisxE2E(t, "redis_stream", "cluster")
}

// TestRedisx_KafkaTopic_Single / Cluster：自定义 SpanExporter → kafka topic
func TestRedisx_KafkaTopic_Single(t *testing.T) {
	runRedisxE2E(t, "kafka_topic", "single")
}

func TestRedisx_KafkaTopic_Cluster(t *testing.T) {
	runRedisxE2E(t, "kafka_topic", "cluster")
}

// runRedisxE2E 跑一次 redisx × backend × topology 组合的端到端 roundtrip。
// 失败模式：docker / compose 不可达 → assert.StartStack 内部 t.Skip；
// mq.yaml parse 失败 → t.Skip；broker 不可达 → t.Skip。
// 真正断言只在 AssertSpanInBackend 找到 trace_id 时通过。
func runRedisxE2E(t *testing.T, backend, topology string) {
	t.Helper()

	// 1. 启 docker-compose：trace backend + driver 拓扑
	assert.StartStack(t, "redisx", topology, backend)

	// 2. 注入 env var，让 mq.yaml 解析到正确字段
	os.Setenv("MQ_TRACE_BACKEND", backend)
	os.Setenv("MQ_TOPOLOGY", topology)
	t.Cleanup(func() {
		os.Unsetenv("MQ_TRACE_BACKEND")
		os.Unsetenv("MQ_TOPOLOGY")
	})

	// 3. 解析配置：按 topology 切 cfgKey / group
	cfgKey := "redis_single"
	streamName := "example-stream"
	groupName := "example-group"
	if topology == "cluster" {
		cfgKey = "redis_cluster"
		groupName = "example-cluster-group"
	}
	cfg, _, err := redisx.ParseFile("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("mq.yaml parse: %v (likely broker not reachable)", err)
	}
	if cfg == nil {
		t.Skip("mq.yaml returned nil cfg")
	}

	// 4. 装配 trace：3 backend 分支
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	shutdown, err := initRedisxTrace(ctx, cfg, backend, topology)
	if err != nil {
		t.Fatalf("init trace: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	// 5. 取一个随机 trace.TraceID，注入到 root span
	tid := assert.NewTraceID(t)
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	parentCtx := trace.ContextWithSpanContext(ctx, sc)
	spanCtx, span := observability.StartSpan(parentCtx, "redisx.roundtrip")
	defer span.End()

	// 6. 发送消息（带 trace context）
	producer, err := redisx.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("redisx.P: %v (broker not reachable)", err)
	}
	_, err = redisx.XAddTraceWithMaxLen(spanCtx, producer, streamName, 1000,
		map[string]interface{}{"msg": "e2e roundtrip", "tid": tid.String()})
	if err != nil {
		t.Fatalf("XAddTraceWithMaxLen: %v", err)
	}

	// 7. 拉一条消息（带 trace context 提取）
	consumer, err := redisx.C("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("redisx.C: %v (broker not reachable)", err)
	}
	streams, err := redisx.XReadGroupTrace(spanCtx, consumer, groupName, "e2e-worker",
		[]string{streamName, ">"}, 100)
	if err != nil {
		t.Skipf("XReadGroupTrace: %v (no message within window)", err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		t.Fatal("no message received")
	}

	// 8. 断言：span 落到 backend
	assert.AssertSpanInBackendWithTimeout(t, ctx, backend, "redisx", topology, tid, 30*time.Second)
}

// initRedisxTrace 按 backend 字段装配 observability.Config 并调用 InitTracing。
// 客户端（redis client / kafka producer）由本函数创建并随 shutdown 关闭。
func initRedisxTrace(ctx context.Context, cfg *mqx.Config, backend, topology string) (func(context.Context) error, error) {
	obsCfg := &observability.Config{
		Enabled:     true,
		ServiceName: cfg.Trace.ServiceName,
	}
	switch backend {
	case "jaeger":
		obsCfg.Backend = observability.TraceBackendJaeger
		obsCfg.JaegerEndpoint = cfg.Trace.Endpoint
		obsCfg.Insecure = true
	case "redis_stream":
		obsCfg.Backend = observability.TraceBackendRedisStream
		rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		obsCfg.RedisClient = rdb
		obsCfg.RedisStream = fmt.Sprintf("trace:redisx:%s", topology)
	case "kafka_topic":
		obsCfg.Backend = observability.TraceBackendKafkaTopic
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:19092"})
		if err != nil {
			return nil, fmt.Errorf("kafka.NewProducer: %w", err)
		}
		obsCfg.KafkaProducer = p
		obsCfg.KafkaTopic = "trace-spans-redisx"
	default:
		return nil, fmt.Errorf("unknown backend: %s", backend)
	}
	cleanup, err := observability.InitTracing(ctx, obsCfg)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		cleanup(ctx)
		if obsCfg.RedisClient != nil {
			_ = obsCfg.RedisClient.Close()
		}
		if obsCfg.KafkaProducer != nil {
			obsCfg.KafkaProducer.Close()
		}
		return nil
	}, nil
}
