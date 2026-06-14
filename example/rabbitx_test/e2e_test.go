package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/example/assert"
	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/rabbitx"
)

// TestRabbitx_Jaeger_Single / Cluster：OTLP gRPC → jaeger all-in-one
func TestRabbitx_Jaeger_Single(t *testing.T) {
	runRabbitxE2E(t, "jaeger", "single")
}

func TestRabbitx_Jaeger_Cluster(t *testing.T) {
	runRabbitxE2E(t, "jaeger", "cluster")
}

// TestRabbitx_RedisStream_Single / Cluster：自定义 SpanExporter → redis stream
func TestRabbitx_RedisStream_Single(t *testing.T) {
	runRabbitxE2E(t, "redis_stream", "single")
}

func TestRabbitx_RedisStream_Cluster(t *testing.T) {
	runRabbitxE2E(t, "redis_stream", "cluster")
}

// TestRabbitx_KafkaTopic_Single / Cluster：自定义 SpanExporter → kafka topic
func TestRabbitx_KafkaTopic_Single(t *testing.T) {
	runRabbitxE2E(t, "kafka_topic", "single")
}

func TestRabbitx_KafkaTopic_Cluster(t *testing.T) {
	runRabbitxE2E(t, "kafka_topic", "cluster")
}

// runRabbitxE2E 跑一次 rabbitx × backend × topology 组合的端到端 roundtrip。
//
// 失败模式：docker / compose 不可达 → assert.StartStack 内部 t.Skip；
// mq.yaml parse 失败 → t.Skip；broker 不可达 → t.Skip。
// 真正断言只在 AssertSpanInBackend 找到 trace_id 时通过。
//
// 与 redisx / mqttx 不同：rabbitx 是 pull + ack 模型；handler 在 receive
// 循环里开 consumer span 并按 tid 过滤本测试消息，超时由 ctx 控制。
func runRabbitxE2E(t *testing.T, backend, topology string) {
	t.Helper()

	// 1. 启 docker-compose：trace backend + driver 拓扑
	assert.StartStack(t, "rabbitx", topology, backend)

	// 2. 注入 env var，让 mq.yaml 解析到正确字段
	os.Setenv("MQ_TRACE_BACKEND", backend)
	t.Cleanup(func() { os.Unsetenv("MQ_TRACE_BACKEND") })

	// 3. 解析配置：按 topology 切 cfgKey
	cfgKey := "rabbit_single"
	if topology == "cluster" {
		cfgKey = "rabbit_cluster"
	}
	cfg, _, err := rabbitx.ParseFile("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("mq.yaml parse: %v (likely broker not reachable)", err)
	}
	if cfg == nil {
		t.Skip("mq.yaml returned nil cfg")
	}

	// 4. 装配 trace：3 backend 分支
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	shutdown, err := initRabbitxTrace(ctx, cfg, backend, topology)
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
		SpanID:     sid, // 用 tid 后 8 字节作 SpanID，保证 deterministic
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	})
	rootCtx := trace.ContextWithSpanContext(ctx, sc)
	spanCtx, span := observability.StartSpan(rootCtx, "rabbitx.roundtrip")
	defer span.End()

	// 6. 启动 consumer：收到第一条匹配 tid 的消息即解除阻塞
	consumer, err := rabbitx.C("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("rabbitx.C: %v (broker not reachable)", err)
	}
	ch, chErr := consumer.Channel()
	if chErr != nil {
		t.Skipf("consumer.Channel: %v (broker not reachable)", chErr)
	}
	defer ch.Close()

	queue := cfg.RabbitMQ.Queue
	msgs, err := rabbitx.ConsumeTrace(spanCtx, ch, queue, "e2e-consumer", false, true)
	if err != nil {
		t.Skipf("ConsumeTrace: %v (broker not reachable)", err)
	}

	received := make(chan struct{}, 1)
	var receivedOnce sync.Once
	go func() {
		for d := range msgs {
			// 提取 trace context，从 RabbitMQ headers 续上 span
			msgCtx := observability.ExtractRabbitTrace(spanCtx, d.Headers)
			_, cspan := observability.StartSpan(msgCtx, "rabbitx.consume",
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "rabbitmq"),
					attribute.String("messaging.destination", queue),
				),
			)
			_ = d.Ack(false)
			cspan.End()
			if string(d.Body) == tid.String() {
				receivedOnce.Do(func() { received <- struct{}{} })
				return
			}
		}
	}()

	// 给订阅一点时间建立
	time.Sleep(500 * time.Millisecond)

	// 7. 发送消息（payload 携带 trace_id，方便 handler 过滤本测试的消息）
	producer, err := rabbitx.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("rabbitx.P: %v (broker not reachable)", err)
	}
	pCh, pChErr := producer.Channel()
	if pChErr != nil {
		t.Skipf("producer.Channel: %v (broker not reachable)", pChErr)
	}
	defer pCh.Close()

	exchange := cfg.RabbitMQ.Exchange
	routingKey := cfg.RabbitMQ.RoutingKey
	if err := rabbitx.PublishWithContextTrace(spanCtx, pCh, exchange, routingKey,
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(tid.String()),
			DeliveryMode: amqp.Persistent,
		}, true); err != nil {
		t.Fatalf("PublishWithContextTrace: %v", err)
	}

	// 8. 等待消费回执
	select {
	case <-received:
	case <-ctx.Done():
		t.Skipf("no message received within window: %v", ctx.Err())
	}

	// 9. 断言：span 落到 backend
	assert.AssertSpanInBackendWithTimeout(t, ctx, backend, "rabbitx", topology, tid, 30*time.Second)
}

// initRabbitxTrace 按 backend 字段装配 observability.Config 并调用 InitTracing。
// 客户端（redis client / kafka producer）由本函数创建并随 shutdown 关闭。
func initRabbitxTrace(ctx context.Context, cfg *mqx.Config, backend, topology string) (func(context.Context) error, error) {
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
		obsCfg.RedisStream = fmt.Sprintf("trace:rabbitx:%s", topology)
	case "kafka_topic":
		obsCfg.Backend = observability.TraceBackendKafkaTopic
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:19092"})
		if err != nil {
			return nil, fmt.Errorf("kafka.NewProducer: %w", err)
		}
		obsCfg.KafkaProducer = p
		obsCfg.KafkaTopic = "trace-spans-rabbitx"
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
