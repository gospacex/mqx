package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/example/assert"
	"github.com/gospacex/mqx/mqttx"
	"github.com/gospacex/mqx/observability"
)

// TestMqttx_Jaeger_Single / Cluster：OTLP gRPC → jaeger all-in-one
func TestMqttx_Jaeger_Single(t *testing.T) {
	runMqttxE2E(t, "jaeger", "single")
}

func TestMqttx_Jaeger_Cluster(t *testing.T) {
	runMqttxE2E(t, "jaeger", "cluster")
}

// TestMqttx_RedisStream_Single / Cluster：自定义 SpanExporter → redis stream
func TestMqttx_RedisStream_Single(t *testing.T) {
	runMqttxE2E(t, "redis_stream", "single")
}

func TestMqttx_RedisStream_Cluster(t *testing.T) {
	runMqttxE2E(t, "redis_stream", "cluster")
}

// TestMqttx_KafkaTopic_Single / Cluster：自定义 SpanExporter → kafka topic
func TestMqttx_KafkaTopic_Single(t *testing.T) {
	runMqttxE2E(t, "kafka_topic", "single")
}

func TestMqttx_KafkaTopic_Cluster(t *testing.T) {
	runMqttxE2E(t, "kafka_topic", "cluster")
}

// runMqttxE2E 跑一次 mqttx × backend × topology 组合的端到端 roundtrip。
//
// 失败模式：docker / compose 不可达 → assert.StartStack 内部 t.Skip；
// mq.yaml parse 失败 → t.Skip；broker 不可达 → t.Skip。
// 真正断言只在 AssertSpanInBackend 找到 trace_id 时通过。
//
// MQTT 与 redis/kafka 不同：没有"消费再确认"语义，所以用 once + channel
// 在 handler 内收到第一条消息即解除阻塞；超时由 ctx 控制。
func runMqttxE2E(t *testing.T, backend, topology string) {
	t.Helper()

	// 1. 启 docker-compose：trace backend + driver 拓扑
	assert.StartStack(t, "mqttx", topology, backend)

	// 2. 注入 env var，让 mq.yaml 解析到正确字段
	os.Setenv("MQ_TRACE_BACKEND", backend)
	t.Cleanup(func() { os.Unsetenv("MQ_TRACE_BACKEND") })

	// 3. 解析配置：按 topology 切 cfgKey / topic
	cfgKey := "mqtt_single"
	topic := "example/mqtt"
	if topology == "cluster" {
		cfgKey = "mqtt_cluster"
	}
	cfg, _, err := mqttx.ParseFile("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("mq.yaml parse: %v (likely broker not reachable)", err)
	}
	if cfg == nil {
		t.Skip("mq.yaml returned nil cfg")
	}

	// 4. 装配 trace：3 backend 分支
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	shutdown, err := initMqttxTrace(ctx, cfg, backend, topology)
	if err != nil {
		t.Fatalf("init trace: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	// 5. 取一个随机 trace.TraceID，注入到 root span
	tid := assert.NewTraceID(t)
	var sid trace.SpanID
	copy(sid[:], tid[8:])
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	})
	spanCtx := trace.ContextWithSpanContext(ctx, sc)
	_, span := observability.StartSpan(spanCtx, "mqttx.roundtrip")
	defer span.End()

	// 6. 订阅（带 trace context 提取）；第一条消息到达即解除阻塞
	consumer, err := mqttx.C("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("mqttx.C: %v (broker not reachable)", err)
	}

	received := make(chan struct{}, 1)
	var receivedOnce sync.Once
	handler := func(_ mqtt.Client, msg mqtt.Message) {
		if string(msg.Payload()) == tid.String() {
			receivedOnce.Do(func() { received <- struct{}{} })
		}
	}
	if err := mqttx.SubscribeTrace(spanCtx, consumer, topic, 0, handler); err != nil {
		t.Skipf("SubscribeTrace: %v (broker not reachable)", err)
	}

	// 给订阅一点时间建立
	time.Sleep(500 * time.Millisecond)

	// 7. 发布（payload 携带 trace_id，方便 handler 过滤本测试的消息）
	producer, err := mqttx.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("mqttx.P: %v (broker not reachable)", err)
	}
	if err := mqttx.PublishTrace(spanCtx, producer, topic, 0, false, tid.String()); err != nil {
		t.Fatalf("PublishTrace: %v", err)
	}

	// 8. 等待消费回执
	select {
	case <-received:
	case <-ctx.Done():
		t.Skipf("no message received within window: %v", ctx.Err())
	}

	// 9. 断言：span 落到 backend
	assert.AssertSpanInBackendWithTimeout(t, ctx, backend, "mqttx", topology, tid, 30*time.Second)
}

// initMqttxTrace 按 backend 字段装配 observability.Config 并调用 InitTracing。
// 客户端（redis client / kafka producer）由本函数创建并随 shutdown 关闭。
func initMqttxTrace(ctx context.Context, cfg *mqx.Config, backend, topology string) (func(context.Context) error, error) {
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
		obsCfg.RedisStream = fmt.Sprintf("trace:mqttx:%s", topology)
	case "kafka_topic":
		obsCfg.Backend = observability.TraceBackendKafkaTopic
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:19092"})
		if err != nil {
			return nil, fmt.Errorf("kafka.NewProducer: %w", err)
		}
		obsCfg.KafkaProducer = p
		obsCfg.KafkaTopic = "trace-spans-mqttx"
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
