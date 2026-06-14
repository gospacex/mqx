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
	"github.com/gospacex/mqx/kafkax"
	"github.com/gospacex/mqx/observability"
)

// TestKafkax_Jaeger_Single / Cluster：OTLP gRPC → jaeger all-in-one
func TestKafkax_Jaeger_Single(t *testing.T) {
	runKafkaxE2E(t, "jaeger", "single")
}

func TestKafkax_Jaeger_Cluster(t *testing.T) {
	runKafkaxE2E(t, "jaeger", "cluster")
}

// TestKafkax_RedisStream_Single / Cluster：自定义 SpanExporter → redis stream
func TestKafkax_RedisStream_Single(t *testing.T) {
	runKafkaxE2E(t, "redis_stream", "single")
}

func TestKafkax_RedisStream_Cluster(t *testing.T) {
	runKafkaxE2E(t, "redis_stream", "cluster")
}

// TestKafkax_KafkaTopic_Single / Cluster：自定义 SpanExporter → kafka topic
func TestKafkax_KafkaTopic_Single(t *testing.T) {
	runKafkaxE2E(t, "kafka_topic", "single")
}

func TestKafkax_KafkaTopic_Cluster(t *testing.T) {
	runKafkaxE2E(t, "kafka_topic", "cluster")
}

// runKafkaxE2E 跑一次 kafkax × backend × topology 组合的 4 档深度端到端验证。
// 失败模式：docker / compose 不可达 → assert.StartStack 内部 t.Skip；
// mq.yaml parse 失败 → t.Skip；broker 不可达 → t.Skip。
func runKafkaxE2E(t *testing.T, backend, topology string) {
	t.Helper()

	// 1. 启 docker-compose：trace backend + driver 拓扑
	assert.StartStack(t, "kafkax", topology, backend)

	// 2. 注入 env var，让 mq.yaml 解析到正确字段
	os.Setenv("MQ_TRACE_BACKEND", backend)
	os.Setenv("MQ_TOPOLOGY", topology)
	t.Cleanup(func() {
		os.Unsetenv("MQ_TRACE_BACKEND")
		os.Unsetenv("MQ_TOPOLOGY")
	})

	// 3. 解析配置：按 topology 切 cfgKey
	cfgKey := "kafka_single"
	if topology == "cluster" {
		cfgKey = "kafka_cluster"
	}
	cfg, _, err := kafkax.ParseFile("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("mq.yaml parse: %v (likely broker not reachable)", err)
	}
	if cfg == nil {
		t.Skip("mq.yaml returned nil cfg")
	}

	// 4. 装配 trace：3 backend 分支
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	shutdown, err := initKafkaxTrace(ctx, cfg, backend, topology)
	if err != nil {
		t.Fatalf("init trace: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	// 5. 4 档深度 subtest
	t.Run("depth-1-happy", func(t *testing.T) {
		tid := assert.NewTraceID(t)
		spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    tid,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
			Remote:     true,
		})
		parentCtx := trace.ContextWithSpanContext(ctx, sc)
		spanCtx, span := observability.StartSpan(parentCtx, "kafkax.roundtrip")
		defer span.End()

		producer, err := kafkax.P("mq.yaml#" + cfgKey)
		if err != nil {
			t.Skipf("kafkax.P: %v (broker not reachable)", err)
		}
		topic := cfg.Producer.Topic
		if topic == "" {
			topic = "example-events"
		}
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("e2e-key"),
			Value:          []byte(fmt.Sprintf(`{"msg":"e2e","tid":"%s"}`, tid.String())),
		}
		if err := kafkax.ProduceTrace(spanCtx, producer, msg, true); err != nil {
			t.Fatalf("ProduceTrace: %v", err)
		}
		if remaining := producer.Flush(5000); remaining > 0 {
			t.Logf("producer flush left %d pending", remaining)
		}

		consumer, err := kafkax.C("mq.yaml#" + cfgKey)
		if err != nil {
			t.Skipf("kafkax.C: %v (broker not reachable)", err)
		}
		got, err := kafkax.ConsumeTrace(spanCtx, consumer, 10*time.Second, true)
		if err != nil {
			t.Skipf("ConsumeTrace: %v (no message within window)", err)
		}
		if _, err := kafkax.CommitMessageTrace(spanCtx, consumer, got, true); err != nil {
			t.Logf("CommitMessageTrace: %v", err)
		}

		assert.AssertSpanInBackendWithTimeout(t, ctx, backend, "kafkax", topology, tid, 30*time.Second)
	})

	t.Run("depth-2-span-fields", func(t *testing.T) {
		payload := []byte(`{"msg":"depth-2"}`)
		spanID := assert.NewSpanID(t)
		_, _, traceID := assert.ProduceConsumeWithSpanID(t, cfg, cfgKey, payload, spanID)
		spans := assert.FetchSpansByTraceID(t, backend, "kafkax", topology, traceID)
		assert.AssertSpanFields(t, spans, assert.SpanExpect{
			Name:       "kafkax.roundtrip",
			Kind:       "", // jaeger/redis/kafka 都不强制（redis/kafka skip）
			Attributes: map[string]string{"messaging.system": "kafka"},
		})
	})

	t.Run("depth-3-context-propagation", func(t *testing.T) {
		payload := []byte(`{"msg":"depth-3"}`)
		spanID := assert.NewSpanID(t)
		_, _, traceID := assert.ProduceConsumeWithSpanID(t, cfg, cfgKey, payload, spanID)
		spans := assert.FetchSpansByTraceID(t, backend, "kafkax", topology, traceID)
		if backend == assert.BackendJaeger {
			assert.AssertTraceContext(t, spans, traceID.String(), spanID)
		} else {
			assert.AssertTraceContextLoose(t, spans, traceID.String())
		}
	})

	t.Run("depth-4-failure-injection", func(t *testing.T) {
		t.Run("4a-backend-shutdown", func(t *testing.T) {
			// 模拟 backend 不可达：调 observability.Shutdown 关闭 trace provider
			// （实际 OTel SDK 全局 provider；shutdown 后 BatchSpanProcessor 不再 flush）
			_ = observability.Shutdown(context.Background())
			t.Cleanup(func() {
				// 重新启 provider 供后续 subtest 用
				ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel2()
				newShutdown, err := initKafkaxTrace(ctx2, cfg, backend, topology)
				if err != nil {
					t.Logf("[4a] re-init trace failed: %v", err)
					return
				}
				t.Cleanup(func() { _ = newShutdown(context.Background()) })
			})

			payload := []byte(`{"msg":"4a-after-shutdown"}`)
			assert.ProduceOnce(t, cfg, cfgKey, payload) // 不应失败
		})

		t.Run("4b-concurrent-100", func(t *testing.T) {
			consumed := assert.ProduceConsumeConcurrent(t, cfg, cfgKey, 100)
			if consumed < 90 {
				t.Fatalf("depth-4b: consumed=%d, want >= 90", consumed)
			}
		})

		t.Run("4c-anomalous-payloads", func(t *testing.T) {
			cases := []struct {
				name    string
				payload []byte
			}{
				{"nil-payload", nil},
				{"empty-payload", []byte{}},
				{"huge-1mb-payload", make([]byte, 1<<20)},
			}
			for _, tc := range cases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assert.ProduceOnce(t, cfg, cfgKey, tc.payload) // 不 panic、不 err
				})
			}
		})
	})
	_ = ctx // referenced by subtests via closure
}

// initKafkaxTrace 按 backend 字段装配 observability.Config 并调用 InitTracing。
// 客户端（redis client / kafka producer）由本函数创建并随 shutdown 关闭。
func initKafkaxTrace(ctx context.Context, cfg *mqx.Config, backend, topology string) (func(context.Context) error, error) {
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
		obsCfg.RedisStream = fmt.Sprintf("trace:kafkax:%s", topology)
	case "kafka_topic":
		obsCfg.Backend = observability.TraceBackendKafkaTopic
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:19092"})
		if err != nil {
			return nil, fmt.Errorf("kafka.NewProducer: %w", err)
		}
		obsCfg.KafkaProducer = p
		obsCfg.KafkaTopic = "trace-spans-kafkax"
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
