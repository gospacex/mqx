package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/example/assert"
	"github.com/gospacex/mqx/natsx"
	"github.com/gospacex/mqx/observability"
)

// TestNatsx_Jaeger_Single / Cluster：OTLP gRPC → jaeger all-in-one
func TestNatsx_Jaeger_Single(t *testing.T) {
	runNatsxE2E(t, "jaeger", "single")
}

func TestNatsx_Jaeger_Cluster(t *testing.T) {
	runNatsxE2E(t, "jaeger", "cluster")
}

// TestNatsx_RedisStream_Single / Cluster：自定义 SpanExporter → redis stream
func TestNatsx_RedisStream_Single(t *testing.T) {
	runNatsxE2E(t, "redis_stream", "single")
}

func TestNatsx_RedisStream_Cluster(t *testing.T) {
	runNatsxE2E(t, "redis_stream", "cluster")
}

// TestNatsx_KafkaTopic_Single / Cluster：自定义 SpanExporter → kafka topic
func TestNatsx_KafkaTopic_Single(t *testing.T) {
	runNatsxE2E(t, "kafka_topic", "single")
}

func TestNatsx_KafkaTopic_Cluster(t *testing.T) {
	runNatsxE2E(t, "kafka_topic", "cluster")
}

// runNatsxE2E 跑一次 natsx × backend × topology 组合的端到端 roundtrip。
// 失败模式：docker / compose 不可达 → assert.StartStack 内部 t.Skip；
// mq.yaml parse 失败 → t.Skip；broker 不可达 → t.Skip。
// 真正断言只在 AssertSpanInBackend 找到 trace_id 时通过。
func runNatsxE2E(t *testing.T, backend, topology string) {
	t.Helper()

	// 1. 启 docker-compose：trace backend + driver 拓扑
	assert.StartStack(t, "natsx", topology, backend)

	// 2. 注入 env var，让 mq.yaml 解析到正确字段
	os.Setenv("MQ_TRACE_BACKEND", backend)
	os.Setenv("MQ_TOPOLOGY", topology)
	t.Cleanup(func() {
		os.Unsetenv("MQ_TRACE_BACKEND")
		os.Unsetenv("MQ_TOPOLOGY")
	})

	// 3. 解析配置：按 topology 切 cfgKey / subject / queue
	cfgKey := "nats_single"
	subject := "example.subject"
	queue := "example-queue"
	if topology == "cluster" {
		cfgKey = "nats_cluster"
		queue = "example-cluster-queue"
	}
	cfg, _, err := natsx.ParseFile("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("mq.yaml parse: %v (likely broker not reachable)", err)
	}
	if cfg == nil {
		t.Skip("mq.yaml returned nil cfg")
	}

	// 4. 装配 trace：3 backend 分支
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	shutdown, err := initNatsxTrace(ctx, cfg, backend, topology)
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
	spanCtx, span := observability.StartSpan(parentCtx, "natsx.roundtrip")
	defer span.End()

	// 6. 发送消息（带 trace context 注入到 NATS headers）
	producer, err := natsx.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("natsx.P: %v (broker not reachable)", err)
	}
	nc, ok := producer.(*nats.Conn)
	if !ok {
		t.Fatalf("natsx.P returned %T, want *nats.Conn", producer)
	}
	if err := natsx.PublishTrace(spanCtx, nc, subject, []byte("e2e roundtrip")); err != nil {
		t.Fatalf("PublishTrace: %v", err)
	}

	// 7. 订阅消息（带 trace context 提取）
	consumer, err := natsx.C("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("natsx.C: %v (broker not reachable)", err)
	}
	nc2, ok := consumer.(*nats.Conn)
	if !ok {
		t.Fatalf("natsx.C returned %T, want *nats.Conn", consumer)
	}
	var received atomic.Bool
	subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
	defer subCancel()
	_, err = natsx.QueueSubscribeTrace(subCtx, nc2, subject, queue, func(ctx context.Context, msg *nats.Msg) {
		if string(msg.Data) == "e2e roundtrip" {
			received.Store(true)
		}
	})
	if err != nil {
		t.Skipf("QueueSubscribeTrace: %v (broker not reachable)", err)
	}
	defer func() { _ = nc2.FlushTimeout(500 * time.Millisecond) }()

	// 8. 等待收到消息（最多 10 秒）
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !received.Load() {
		time.Sleep(100 * time.Millisecond)
	}
	if !received.Load() {
		t.Fatal("no message received within 10s")
	}

	// 9. 断言：span 落到 backend
	assert.AssertSpanInBackendWithTimeout(t, ctx, backend, "natsx", topology, tid, 30*time.Second)
}

// initNatsxTrace 按 backend 字段装配 observability.Config 并调用 InitTracing。
// 客户端（redis client / kafka producer）由本函数创建并随 shutdown 关闭。
func initNatsxTrace(ctx context.Context, cfg *mqx.Config, backend, topology string) (func(context.Context) error, error) {
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
		obsCfg.RedisStream = fmt.Sprintf("trace:natsx:%s", topology)
	case "kafka_topic":
		obsCfg.Backend = observability.TraceBackendKafkaTopic
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:19092"})
		if err != nil {
			return nil, fmt.Errorf("kafka.NewProducer: %w", err)
		}
		obsCfg.KafkaProducer = p
		obsCfg.KafkaTopic = "trace-spans-natsx"
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
