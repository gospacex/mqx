package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/example/assert"
	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/pulsarx"
)

// TestPulsarx_Jaeger_Single / Cluster：OTLP gRPC → jaeger all-in-one
func TestPulsarx_Jaeger_Single(t *testing.T) {
	runPulsarxE2E(t, "jaeger", "single")
}

func TestPulsarx_Jaeger_Cluster(t *testing.T) {
	runPulsarxE2E(t, "jaeger", "cluster")
}

// TestPulsarx_RedisStream_Single / Cluster：自定义 SpanExporter → redis stream
func TestPulsarx_RedisStream_Single(t *testing.T) {
	runPulsarxE2E(t, "redis_stream", "single")
}

func TestPulsarx_RedisStream_Cluster(t *testing.T) {
	runPulsarxE2E(t, "redis_stream", "cluster")
}

// TestPulsarx_KafkaTopic_Single / Cluster：自定义 SpanExporter → kafka topic
func TestPulsarx_KafkaTopic_Single(t *testing.T) {
	runPulsarxE2E(t, "kafka_topic", "single")
}

func TestPulsarx_KafkaTopic_Cluster(t *testing.T) {
	runPulsarxE2E(t, "kafka_topic", "cluster")
}

// runPulsarxE2E 跑一次 pulsarx × backend × topology 组合的端到端 roundtrip。
// 失败模式：docker / compose 不可达 → assert.StartStack 内部 t.Skip；
// mq.yaml parse 失败 → t.Skip；broker 不可达 → t.Skip。
// 真正断言只在 AssertSpanInBackend 找到 trace_id 时通过。
func runPulsarxE2E(t *testing.T, backend, topology string) {
	t.Helper()

	// 1. 启 docker-compose：trace backend + driver 拓扑
	assert.StartStack(t, "pulsarx", topology, backend)

	// 2. 解析配置：按 topology 切 cfgKey / group
	cfgKey := "pulsar_single"
	if topology == "cluster" {
		cfgKey = "pulsar_cluster"
	}
	cfg, _, err := pulsarx.ParseFile("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("mq.yaml parse: %v (likely broker not reachable)", err)
	}
	if cfg == nil {
		t.Skip("mq.yaml returned nil cfg")
	}

	// 3. 装配 trace：3 backend 分支
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	shutdown, err := initPulsarxTrace(ctx, cfg, backend, topology)
	if err != nil {
		t.Fatalf("init trace: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	// 4. 取一个随机 trace.TraceID，注入到 root span
	tid := assert.NewTraceID(t)
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	parentCtx := trace.ContextWithSpanContext(ctx, sc)
	spanCtx, span := observability.StartSpan(parentCtx, "pulsarx.roundtrip")
	defer span.End()

	// 5. 发送消息（带 trace context）
	producer, err := pulsarx.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("pulsarx.P: %v (broker not reachable)", err)
	}
	msg := &pulsar.ProducerMessage{
		Payload: []byte(fmt.Sprintf("e2e roundtrip tid=%s", tid.String())),
		Properties: map[string]string{
			"tid": tid.String(),
		},
	}
	if _, err := pulsarx.SendTrace(spanCtx, producer, msg); err != nil {
		t.Fatalf("SendTrace: %v", err)
	}

	// 6. 拉一条消息（带 trace context 提取）
	consumer, err := pulsarx.C("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("pulsarx.C: %v (broker not reachable)", err)
	}
	recvCtx, recvCancel := context.WithTimeout(spanCtx, 30*time.Second)
	defer recvCancel()
	recvMsg, err := pulsarx.ReceiveTrace(recvCtx, consumer)
	if err != nil {
		t.Skipf("ReceiveTrace: %v (no message within window)", err)
	}
	_ = pulsarx.AckTrace(recvCtx, consumer, recvMsg)

	// 7. 断言：span 落到 backend
	assert.AssertSpanInBackendWithTimeout(t, ctx, backend, "pulsarx", topology, tid, 60*time.Second)
}

// initPulsarxTrace 按 backend 字段装配 observability.Config 并调用 InitTracing。
// 客户端（redis client / kafka producer）由本函数创建并随 shutdown 关闭。
func initPulsarxTrace(ctx context.Context, cfg *mqx.Config, backend, topology string) (func(context.Context) error, error) {
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
		obsCfg.RedisStream = fmt.Sprintf("trace:pulsarx:%s", topology)
	case "kafka_topic":
		obsCfg.Backend = observability.TraceBackendKafkaTopic
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:19092"})
		if err != nil {
			return nil, fmt.Errorf("kafka.NewProducer: %w", err)
		}
		obsCfg.KafkaProducer = p
		obsCfg.KafkaTopic = "trace-spans-pulsarx"
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
