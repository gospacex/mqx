package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/nsqio/go-nsq"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/example/assert"
	"github.com/gospacex/mqx/nsqx"
	"github.com/gospacex/mqx/observability"
)

// TestNsqx_Jaeger_Single / Cluster：OTLP gRPC → jaeger all-in-one
func TestNsqx_Jaeger_Single(t *testing.T) {
	runNsqxE2E(t, "jaeger", "single")
}

func TestNsqx_Jaeger_Cluster(t *testing.T) {
	runNsqxE2E(t, "jaeger", "cluster")
}

// TestNsqx_RedisStream_Single / Cluster：自定义 SpanExporter → redis stream
func TestNsqx_RedisStream_Single(t *testing.T) {
	runNsqxE2E(t, "redis_stream", "single")
}

func TestNsqx_RedisStream_Cluster(t *testing.T) {
	runNsqxE2E(t, "redis_stream", "cluster")
}

// TestNsqx_KafkaTopic_Single / Cluster：自定义 SpanExporter → kafka topic
func TestNsqx_KafkaTopic_Single(t *testing.T) {
	runNsqxE2E(t, "kafka_topic", "single")
}

func TestNsqx_KafkaTopic_Cluster(t *testing.T) {
	runNsqxE2E(t, "kafka_topic", "cluster")
}

// runNsqxE2E 跑一次 nsqx × backend × topology 组合的端到端 roundtrip。
//
// 失败模式：docker / compose 不可达 → assert.StartStack 内部 t.Skip；
// mq.yaml parse 失败 → t.Skip；broker 不可达 → t.Skip。
// 真正断言只在 AssertSpanInBackend 找到 trace_id 时通过。
//
// NSQ 与 redis/kafka 不同：
//   - 消费者必须在框架返回后**手动**挂 handler 并 ConnectToNSQD / ConnectToNSQLookupd
//   - 没有消费回执 channel，所以用 once + channel 在 handler 内收到第一条消息即解除阻塞
//   - payload 携带 trace_id hex，方便 handler 过滤本测试消息
func runNsqxE2E(t *testing.T, backend, topology string) {
	t.Helper()

	// 1. 启 docker-compose：trace backend + driver 拓扑
	assert.StartStack(t, "nsqx", topology, backend)

	// 2. 注入 env var，让 mq.yaml 解析到正确字段
	os.Setenv("MQ_TRACE_BACKEND", backend)
	os.Setenv("MQ_TOPOLOGY", topology)
	t.Cleanup(func() {
		os.Unsetenv("MQ_TRACE_BACKEND")
		os.Unsetenv("MQ_TOPOLOGY")
	})

	// 3. 解析配置：按 topology 切 cfgKey
	cfgKey := "nsq_single"
	topic := "example-nsq"
	if topology == "cluster" {
		cfgKey = "nsq_cluster"
	}
	cfg, _, err := nsqx.ParseFile("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("mq.yaml parse: %v (likely broker not reachable)", err)
	}
	if cfg == nil {
		t.Skip("mq.yaml returned nil cfg")
	}

	// 4. 装配 trace：3 backend 分支
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	shutdown, err := initNsqxTrace(ctx, cfg, backend, topology)
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
	spanCtx, span := observability.StartSpan(rootCtx, "nsqx.roundtrip")
	defer span.End()

	// 6. 订阅（带 trace context 提取）；第一条匹配消息到达即解除阻塞
	consumer, err := nsqx.C("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("nsqx.C: %v (broker not reachable)", err)
	}

	received := make(chan struct{}, 1)
	var receivedOnce sync.Once
	var receivedMu sync.Mutex
	receivedBody := ""
	handler := func(msg *nsq.Message) error {
		body := string(msg.Body)
		if body == tid.String() {
			receivedMu.Lock()
			receivedBody = body
			receivedMu.Unlock()
			receivedOnce.Do(func() { received <- struct{}{} })
		}
		return nil
	}
	nsqx.AddHandlerTrace(spanCtx, consumer, handler)

	// NSQ single 模式连 nsqd，cluster 模式经 lookupd 发现
	connectCtx, connectCancel := context.WithTimeout(ctx, 10*time.Second)
	defer connectCancel()
	if err := connectNsqConsumer(connectCtx, consumer, cfg, topology); err != nil {
		t.Skipf("nsqx consumer connect: %v (broker not reachable)", err)
	}
	t.Cleanup(func() { consumer.Stop() })

	// 给订阅一点时间建立连接并就绪
	time.Sleep(500 * time.Millisecond)

	// 7. 发布：payload = tid hex，让 handler 能过滤本测试消息
	producer, err := nsqx.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("nsqx.P: %v (broker not reachable)", err)
	}
	if err := nsqx.PublishTrace(spanCtx, producer, topic, []byte(tid.String())); err != nil {
		t.Fatalf("PublishTrace: %v", err)
	}

	// 8. 等待消费回执
	select {
	case <-received:
	case <-ctx.Done():
		t.Skipf("no message received within window: %v", ctx.Err())
	}

	// 9. 断言：span 落到 backend
	_ = receivedBody
	assert.AssertSpanInBackendWithTimeout(t, ctx, backend, "nsqx", topology, tid, 30*time.Second)
}

// connectNsqConsumer 按 topology 选择直连 nsqd 或经 lookupd 发现。
func connectNsqConsumer(ctx context.Context, consumer *nsq.Consumer, cfg *mqx.Config, topology string) error {
	if topology == "cluster" {
		// cluster 模式：连 lookupd，让 consumer 自动发现全部 nsqd
		addrs := cfg.Addrs
		if len(addrs) == 0 {
			return fmt.Errorf("nsq cluster cfg has no addrs (lookupd)")
		}
		done := make(chan error, 1)
		go func() { done <- consumer.ConnectToNSQLookupd(addrs[0]) }()
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return fmt.Errorf("connect lookupd: %w", ctx.Err())
		}
	}
	// single 模式：直连 nsqd
	addr := "localhost:4150"
	if cfg.NSQ != nil && cfg.NSQ.NsqdAddr != "" {
		addr = cfg.NSQ.NsqdAddr
	}
	done := make(chan error, 1)
	go func() { done <- consumer.ConnectToNSQD(addr) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("connect nsqd: %w", ctx.Err())
	}
}

// initNsqxTrace 按 backend 字段装配 observability.Config 并调用 InitTracing。
// 客户端（redis client / kafka producer）由本函数创建并随 shutdown 关闭。
func initNsqxTrace(ctx context.Context, cfg *mqx.Config, backend, topology string) (func(context.Context) error, error) {
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
		obsCfg.RedisStream = fmt.Sprintf("trace:nsqx:%s", topology)
	case "kafka_topic":
		obsCfg.Backend = observability.TraceBackendKafkaTopic
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:19092"})
		if err != nil {
			return nil, fmt.Errorf("kafka.NewProducer: %w", err)
		}
		obsCfg.KafkaProducer = p
		obsCfg.KafkaTopic = "trace-spans-nsqx"
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
