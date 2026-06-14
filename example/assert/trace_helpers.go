// trace_helpers.go 是 package assert 的辅助实现文件，与 trace.go 同 package。
//
// 拆分动机：trace.go 在追加 Task 1 (Assert Package Helpers) 的 8 个新函数后
// 达到 748 行，超过项目 500-line 单文件硬约束。把这批与 trace_id 生成 /
// backend span 拉取 / produce-consume 模式相关的 helper 拆到本文件，让
// trace.go 回到 ~400 行，本文件控制在 ~350 行。
//
// 公开 API 不变：example/<driver>_test/e2e_test.go 通过 package assert 调用
// 本文件的所有函数，无需调整 import。
package assert

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/kafkax"
	"github.com/gospacex/mqx/observability"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
)

// NewSpanID 返回 16 hex 字符的 span id（与 OTel 8-byte SpanID 兼容）。
// 用于 producer 端预设 SpanID，使 consumer 端可断言 ParentSpanID。
func NewSpanID(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// FetchSpansByTraceID 拉取指定 trace_id 的所有 SpanRecord。
// backend 决定查询端点（jaeger HTTP / redis XRange / kafka consumer pull）。
// 返回空切片（不报错）表示 backend 暂未 flush，可由调用方重试。
func FetchSpansByTraceID(t *testing.T, backend, driver, topology string, traceID trace.TraceID) []SpanRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	switch backend {
	case BackendJaeger:
		return fetchJaegerSpans(ctx, traceID)
	case BackendRedisStream:
		return fetchRedisStreamSpans(ctx, driver, topology, traceID)
	case BackendKafkaTopic:
		return fetchKafkaTopicSpans(ctx, driver, topology, traceID)
	default:
		t.Fatalf("FetchSpansByTraceID: unknown backend %q", backend)
		return nil
	}
}

func fetchJaegerSpans(ctx context.Context, want trace.TraceID) []SpanRecord {
	url := fmt.Sprintf("http://localhost:16686/api/traces/%s", want.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var body struct {
		Data []struct {
			Spans []struct {
				TraceID       string                        `json:"traceID"`
				SpanID        string                        `json:"spanID"`
				ParentSpanID  string                        `json:"parentSpanID"`
				OperationName string                        `json:"operationName"`
				Tags          []struct{ Key, Value string } `json:"tags"`
			} `json:"spans"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil
	}
	var out []SpanRecord
	for _, d := range body.Data {
		for _, s := range d.Spans {
			rec := SpanRecord{
				TraceID:      s.TraceID,
				SpanID:       s.SpanID,
				ParentSpanID: s.ParentSpanID,
				Name:         s.OperationName,
				Attributes:   map[string]string{},
			}
			for _, tag := range s.Tags {
				rec.Attributes[tag.Key] = tag.Value
			}
			out = append(out, rec)
		}
	}
	return out
}

func fetchRedisStreamSpans(ctx context.Context, driver, topology string, want trace.TraceID) []SpanRecord {
	stream := fmt.Sprintf("trace:%s:%s", driver, topology)
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer func() { _ = client.Close() }()
	res, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		return nil
	}
	var out []SpanRecord
	for _, msg := range res {
		payload, ok := msg.Values["span"].(string)
		if !ok {
			continue
		}
		var rec struct {
			TraceID string            `json:"trace_id"`
			SpanID  string            `json:"span_id"`
			Name    string            `json:"name"`
			Kind    string            `json:"kind"`
			Attrs   map[string]string `json:"attributes"`
		}
		if err := json.Unmarshal([]byte(payload), &rec); err != nil {
			continue
		}
		if rec.TraceID != want.String() {
			continue
		}
		out = append(out, SpanRecord{
			TraceID:    rec.TraceID,
			SpanID:     rec.SpanID,
			Name:       rec.Name,
			Kind:       rec.Kind,
			Attributes: rec.Attrs,
		})
	}
	return out
}

func fetchKafkaTopicSpans(ctx context.Context, driver, topology string, want trace.TraceID) []SpanRecord {
	topic := fmt.Sprintf("trace-spans-%s", driver)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:19092",
		"group.id":          fmt.Sprintf("assert-trace-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil
	}
	defer func() { _ = consumer.Close() }()
	if err := consumer.Subscribe(topic, nil); err != nil {
		return nil
	}
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var out []SpanRecord
	for readCtx.Err() == nil {
		msg, err := consumer.ReadMessage(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var rec struct {
			TraceID string            `json:"trace_id"`
			SpanID  string            `json:"span_id"`
			Name    string            `json:"name"`
			Attrs   map[string]string `json:"attributes"`
		}
		if err := json.Unmarshal(msg.Value, &rec); err != nil {
			continue
		}
		if rec.TraceID != want.String() {
			continue
		}
		out = append(out, SpanRecord{
			TraceID:    rec.TraceID,
			SpanID:     rec.SpanID,
			Name:       rec.Name,
			Attributes: rec.Attrs,
		})
	}
	return out
}

// ProduceConsume 单条 produce + 1 条 consume 同步等待，payload 来回拷贝。
// 返回消费到的 payload（仅用于断言不参与匹配）。
// 缺 broker / 超时 → t.Skip，不 Fail。
func ProduceConsume(t *testing.T, cfg *mqx.Config, cfgKey string, payload []byte) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	producer, err := kafkax.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("kafkax.P: %v", err)
	}
	topic := cfg.Producer.Topic
	if topic == "" {
		topic = "example-events"
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte("e2e-key"),
		Value:          payload,
	}
	if err := kafkax.ProduceTrace(ctx, producer, msg, true); err != nil {
		t.Skipf("ProduceTrace: %v", err)
	}
	if remaining := producer.Flush(5000); remaining > 0 {
		t.Logf("producer flush left %d pending", remaining)
	}

	consumer, err := kafkax.C("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("kafkax.C: %v", err)
	}
	got, err := kafkax.ConsumeTrace(ctx, consumer, 10*time.Second, true)
	if err != nil {
		t.Skipf("ConsumeTrace: %v", err)
	}
	if _, err := kafkax.CommitMessageTrace(ctx, consumer, got, true); err != nil {
		t.Logf("CommitMessageTrace: %v", err)
	}
	return got.Value
}

// ProduceConsumeWithSpanID 同 ProduceConsume，但 producer 端预设 SpanID。
// spanID 是 16 hex 字符串（NewSpanID 产出）。
// 返回消费到的 payload、producer span 的 SpanID（用于 ParentSpanID 断言）、
// 以及本次 roundtrip 注入的 trace.TraceID（用于调用方 FetchSpansByTraceID 拉
// 取 span 列表做强匹配）。traceID 是内部 NewTraceID(t) 生成，与 consumer
// 端从 header 解析出的 traceID 一致。
func ProduceConsumeWithSpanID(t *testing.T, cfg *mqx.Config, cfgKey string, payload []byte, spanID string) (consumed []byte, producerSpanID string, traceID trace.TraceID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spanIDBytes, err := hex.DecodeString(spanID)
	if err != nil || len(spanIDBytes) != 8 {
		t.Fatalf("invalid spanID %q (want 16 hex): %v", spanID, err)
	}
	var sid trace.SpanID
	copy(sid[:], spanIDBytes)
	tid := NewTraceID(t)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	parentCtx := trace.ContextWithSpanContext(ctx, sc)
	spanCtx, span := observability.StartSpan(parentCtx, "kafkax.roundtrip")
	defer span.End()

	producer, err := kafkax.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("kafkax.P: %v", err)
	}
	topic := cfg.Producer.Topic
	if topic == "" {
		topic = "example-events"
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte("e2e-key"),
		Value:          payload,
	}
	if err := kafkax.ProduceTrace(spanCtx, producer, msg, true); err != nil {
		t.Skipf("ProduceTrace: %v", err)
	}
	if remaining := producer.Flush(5000); remaining > 0 {
		t.Logf("producer flush left %d pending", remaining)
	}

	consumer, err := kafkax.C("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("kafkax.C: %v", err)
	}
	got, err := kafkax.ConsumeTrace(spanCtx, consumer, 10*time.Second, true)
	if err != nil {
		t.Skipf("ConsumeTrace: %v", err)
	}
	if _, err := kafkax.CommitMessageTrace(spanCtx, consumer, got, true); err != nil {
		t.Logf("CommitMessageTrace: %v", err)
	}
	return got.Value, spanID, tid
}

// ProduceConsumeConcurrent n 条并发 producer + 1 consumer drain 30s。
// 返回 consumer 实际收到的条数（≥90/100 视为通过）。
func ProduceConsumeConcurrent(t *testing.T, cfg *mqx.Config, cfgKey string, n int) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	producer, err := kafkax.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("kafkax.P: %v", err)
	}
	topic := cfg.Producer.Topic
	if topic == "" {
		topic = "example-events"
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf(`{"i":%d}`, i))
			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(fmt.Sprintf("k-%d", i)),
				Value:          payload,
			}
			if err := kafkax.ProduceTrace(ctx, producer, msg, true); err != nil {
				t.Logf("concurrent produce %d: %v", i, err)
			}
		}(i)
	}
	if remaining := producer.Flush(5000); remaining > 0 {
		t.Logf("producer flush left %d pending", remaining)
	}
	wg.Wait()

	consumer, err := kafkax.C("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("kafkax.C: %v", err)
	}
	consumed := 0
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && consumed < n {
		got, err := kafkax.ConsumeTrace(ctx, consumer, 2*time.Second, true)
		if err != nil {
			continue
		}
		if _, err := kafkax.CommitMessageTrace(ctx, consumer, got, true); err != nil {
			t.Logf("CommitMessageTrace: %v", err)
		}
		consumed++
	}
	return consumed
}

// ProduceOnce 单条 produce，不等待 consumer。用于 depth-4a（backend 已 shutdown 仍应返回）。
func ProduceOnce(t *testing.T, cfg *mqx.Config, cfgKey string, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	producer, err := kafkax.P("mq.yaml#" + cfgKey)
	if err != nil {
		t.Skipf("kafkax.P: %v", err)
	}
	topic := cfg.Producer.Topic
	if topic == "" {
		topic = "example-events"
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte("e2e-key-4a"),
		Value:          payload,
	}
	if err := kafkax.ProduceTrace(ctx, producer, msg, true); err != nil {
		t.Fatalf("ProduceOnce: %v", err)
	}
	if remaining := producer.Flush(2000); remaining > 0 {
		t.Logf("ProduceOnce flush left %d pending", remaining)
	}
}
