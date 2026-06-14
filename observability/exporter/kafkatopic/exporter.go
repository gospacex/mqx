// Package kafkatopic implements an OpenTelemetry SpanExporter that publishes
// each batched span as one Kafka message to a configured topic.
//
// 与 driver 完全解耦：kafkax driver 的 trace 链路不会强制走本 exporter，
// 任意 driver 都可以选 kafkatopic 作为 trace backend。
package kafkatopic

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/sdk/trace"
)

// flushTimeoutMs 是每次 ExportSpans 后 Flush 等待时间（毫秒）。
// 测试场景里 5s 足够把 batch 内的所有消息推完；生产环境可由调用方
// 通过私有构造器（future work）覆盖。
const flushTimeoutMs = 5000

// Exporter 写入 spans 到 Kafka topic。
//
// 每次 ExportSpans 调用对每个 span 产生一条 Produce 记录，Value 是 JSON
// 编码的 span 摘要（含 trace_id / span_id / name / start_time /
// attributes），Key 是 trace_id 字符串，便于消费端按 trace_id partition。
type Exporter struct {
	producer *kafka.Producer
	topic    string
}

// spanRecord 是写入 topic 的载荷；只保留断言 trace_id 所需的最少字段。
type spanRecord struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	Name       string            `json:"name"`
	StartTime  string            `json:"start_time"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// New 构造一个 Kafka Topic SpanExporter。
// topic 不可为空；producer 由调用方管理生命周期，Shutdown 不会关闭它。
func New(producer *kafka.Producer, topic string) (*Exporter, error) {
	if producer == nil {
		return nil, fmt.Errorf("kafkatopic.New: producer is nil")
	}
	if topic == "" {
		return nil, fmt.Errorf("kafkatopic.New: topic is empty")
	}
	return &Exporter{producer: producer, topic: topic}, nil
}

// ExportSpans 把每条 span 序列化为 JSON 并 Produce 到 topic，最后 Flush 一次
// 保证 batch 内消息全部推送完成。
//
// Produce 是异步的（confluent-kafka-go 设计），Flush 失败时返回 error 让
// OTel BatchSpanProcessor 记录；**不在 exporter 内重试**。
func (e *Exporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	for _, s := range spans {
		rec := spanRecord{
			TraceID:   s.SpanContext().TraceID().String(),
			SpanID:    s.SpanContext().SpanID().String(),
			Name:      s.Name(),
			StartTime: s.StartTime().String(),
		}
		if attrs := s.Attributes(); len(attrs) != 0 {
			rec.Attributes = make(map[string]string, len(attrs))
			for _, kv := range attrs {
				rec.Attributes[string(kv.Key)] = kv.Value.Emit()
			}
		}
		payload, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("kafkatopic: marshal span: %w", err)
		}
		topic := e.topic
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(rec.TraceID),
			Value:          payload,
		}
		if err := e.producer.Produce(msg, nil); err != nil {
			return fmt.Errorf("kafkatopic: Produce: %w", err)
		}
	}
	// Flush 等待 batch 内所有 Produce 推送完成；confluent-kafka-go 规定
	// Flush 失败可能是部分消息失败，返回的 remaining 数量由调用方处理。
	if remaining := e.producer.Flush(flushTimeoutMs); remaining > 0 {
		return fmt.Errorf("kafkatopic: flush timed out, %d messages still pending", remaining)
	}
	return nil
}

// Shutdown 关闭 exporter；不关闭外部传入的 producer（生命周期归调用方）。
func (e *Exporter) Shutdown(ctx context.Context) error {
	// 最后一次 Flush 保证 Shutdown 前 pending 消息全推完。
	if remaining := e.producer.Flush(flushTimeoutMs); remaining > 0 {
		return fmt.Errorf("kafkatopic: shutdown flush, %d messages still pending", remaining)
	}
	return nil
}
