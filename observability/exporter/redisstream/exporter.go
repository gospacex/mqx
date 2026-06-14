// Package redisstream implements an OpenTelemetry SpanExporter that writes
// each batched span as one entry to a Redis Stream.
//
// 与 driver 完全解耦：可被任意 driver（kafkax / pulsarx / mqttx 等）的 trace
// 链路复用，不引入 driver 间错位耦合。
package redisstream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/sdk/trace"
)

// Exporter 写入 spans 到 Redis Stream。
//
// 每次 ExportSpans 调用对每个 span 产生一条 XAdd 记录，Values["span"] 是
// JSON 编码的 span 摘要（含 trace_id / span_id / name / start_time /
// attributes），便于消费端做 trace_id 断言。
type Exporter struct {
	client *redis.Client
	stream string
}

// spanRecord 是写入 stream 的载荷；只保留断言 trace_id 所需的最少字段，
// 不导出全量 ReadOnlySpan 以减小 stream 单条大小。
type spanRecord struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	Name       string            `json:"name"`
	StartTime  string            `json:"start_time"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// New 构造一个 Redis Stream SpanExporter。
// stream 不可为空；client 由调用方管理生命周期，Shutdown 不会关闭它。
func New(client *redis.Client, stream string) (*Exporter, error) {
	if client == nil {
		return nil, fmt.Errorf("redisstream.New: client is nil")
	}
	if stream == "" {
		return nil, fmt.Errorf("redisstream.New: stream is empty")
	}
	return &Exporter{client: client, stream: stream}, nil
}

// ExportSpans 把每条 span 序列化为 JSON 并 XAdd 到 stream。
//
// OTel BatchSpanProcessor 已经做了批量 + 重试；本函数失败返回 error
// 让 processor 记录后丢弃，**不在 exporter 内重试**（与 OTel collector
// fileexporter / otlp 行为一致）。
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
			return fmt.Errorf("redisstream: marshal span: %w", err)
		}
		if err := e.client.XAdd(ctx, &redis.XAddArgs{
			Stream: e.stream,
			Values: map[string]interface{}{"span": payload},
		}).Err(); err != nil {
			return fmt.Errorf("redisstream: XAdd: %w", err)
		}
	}
	return nil
}

// Shutdown 关闭 exporter；不关闭外部传入的 client（生命周期归调用方）。
func (e *Exporter) Shutdown(ctx context.Context) error {
	return nil
}
