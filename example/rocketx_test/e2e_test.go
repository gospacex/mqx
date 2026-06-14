package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/example/assert"
	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/rocketx"
)

// TestRocketx_Jaeger_Single / Cluster：OTLP gRPC → jaeger all-in-one
func TestRocketx_Jaeger_Single(t *testing.T) {
	runRocketxE2E(t, "jaeger", "single")
}

func TestRocketx_Jaeger_Cluster(t *testing.T) {
	runRocketxE2E(t, "jaeger", "cluster")
}

// TestRocketx_RedisStream_Single / Cluster：自定义 SpanExporter → redis stream
func TestRocketx_RedisStream_Single(t *testing.T) {
	runRocketxE2E(t, "redis_stream", "single")
}

func TestRocketx_RedisStream_Cluster(t *testing.T) {
	runRocketxE2E(t, "redis_stream", "cluster")
}

// TestRocketx_KafkaTopic_Single / Cluster：自定义 SpanExporter → kafka topic
func TestRocketx_KafkaTopic_Single(t *testing.T) {
	runRocketxE2E(t, "kafka_topic", "single")
}

func TestRocketx_KafkaTopic_Cluster(t *testing.T) {
	runRocketxE2E(t, "kafka_topic", "cluster")
}

// runRocketxE2E 跑一次 rocketx × backend × topology 组合的端到端 roundtrip。
//
// 失败模式：docker / compose 不可达 → assert.StartStack 内部 t.Skip；
// mq.yaml parse 失败 → t.Skip；broker 不可达 → t.Skip。
// 真正断言只在 AssertSpanInBackend 找到 trace_id 时通过。
func runRocketxE2E(t *testing.T, backend, topology string) {
	t.Helper()

	// 1. 启 docker-compose：trace backend + driver 拓扑
	assert.StartStack(t, "rocketx", topology, backend)

	// 2. 注入 env var，让 mq.yaml 解析到正确字段
	os.Setenv("MQ_TRACE_BACKEND", backend)
	os.Setenv("MQ_TOPOLOGY", topology)
	t.Cleanup(func() {
		os.Unsetenv("MQ_TRACE_BACKEND")
		os.Unsetenv("MQ_TOPOLOGY")
	})

	// 3. 解析配置：按 topology 切 cfgKey
	cfgKey := "rocketmq_single"
	topic := "example-rocket"
	if topology == "cluster" {
		cfgKey = "rocketmq_cluster"
	}
	cfg, _, err := rocketx.ParseFile("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("mq.yaml parse: %v (likely broker not reachable)", err)
	}
	if cfg == nil {
		t.Skip("mq.yaml returned nil cfg")
	}

	// 4. 装配 trace：3 backend 分支
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	shutdown, err := initRocketxTrace(ctx, cfg, backend, topology)
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
	rootCtx := trace.ContextWithSpanContext(ctx, sc)
	spanCtx, span := observability.StartSpan(rootCtx, "rocketx.roundtrip")
	defer span.End()

	// 6. 启动 consumer（rocketmq 必须先 Subscribe 再 Start；消息走 callback，
	//    内部用 channel 把消息接到主 goroutine 以便后续断言）。
	gotMsg := make(chan *primitive.MessageExt, 1)
	var gotCount int32
	pushConsumer, err := rocketx.C("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("rocketx.C: %v (broker not reachable)", err)
	}
	subscribeErr := pushConsumer.Subscribe(topic, consumer.MessageSelector{}, func(c context.Context, ext ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range ext {
			handleErr := rocketx.ConsumeTrace(c, msg, true, func(ctx context.Context) error {
				if atomic.CompareAndSwapInt32(&gotCount, 0, 1) {
					select {
					case gotMsg <- msg:
					default:
					}
				}
				return nil
			})
			if handleErr != nil {
				return consumer.ConsumeRetryLater, handleErr
			}
		}
		return consumer.ConsumeSuccess, nil
	})
	if subscribeErr != nil {
		t.Fatalf("subscribe: %v", subscribeErr)
	}
	if startErr := pushConsumer.Start(); startErr != nil {
		t.Fatalf("consumer.Start: %v", startErr)
	}
	t.Cleanup(func() { _ = pushConsumer.Shutdown() })

	// 7. 发送消息（带 trace context）
	producer, err := rocketx.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("rocketx.P: %v (broker not reachable)", err)
	}
	msg := primitive.NewMessage(topic, []byte(fmt.Sprintf(`{"msg":"e2e","tid":"%s"}`, tid.String())))
	if _, sendErr := rocketx.SendSyncTrace(spanCtx, producer, msg, true); sendErr != nil {
		t.Fatalf("SendSyncTrace: %v", sendErr)
	}

	// 8. 等待 consumer 收到消息（在 20s 内）
	select {
	case <-gotMsg:
	case <-time.After(20 * time.Second):
		t.Fatal("no message received within 20s")
	}

	// 9. 断言：span 落到 backend
	assert.AssertSpanInBackendWithTimeout(t, ctx, backend, "rocketx", topology, tid, 30*time.Second)
}

// initRocketxTrace 按 backend 字段装配 observability.Config 并调用 InitTracing。
// 客户端（redis client / kafka producer）由本函数创建并随 shutdown 关闭。
func initRocketxTrace(ctx context.Context, cfg *mqx.Config, backend, topology string) (func(context.Context) error, error) {
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
		obsCfg.RedisStream = fmt.Sprintf("trace:rocketx:%s", topology)
	case "kafka_topic":
		obsCfg.Backend = observability.TraceBackendKafkaTopic
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:19092"})
		if err != nil {
			return nil, fmt.Errorf("kafka.NewProducer: %w", err)
		}
		obsCfg.KafkaProducer = p
		obsCfg.KafkaTopic = "trace-spans-rocketx"
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

// 编译期校验：rocketmq.Producer 接口在本文件中实际被 rocketx.P 返回并使用；
// 该引用让 import 在部分编译场景下被裁剪时也不至于删除依赖。
var _ rocketmq.Producer = (rocketmq.Producer)(nil)
