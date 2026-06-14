package redisx

import (
	"context"
	"fmt"

	"github.com/gospacex/mqx/observability"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// traceCtxCarrier 实现 propagation.TextMapCarrier，用于注入 trace context 到 Redis 消息
type traceCtxCarrier struct {
	values map[string]interface{}
}

func (c *traceCtxCarrier) Get(key string) string {
	if v, ok := c.values[key]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func (c *traceCtxCarrier) Set(key, value string) {
	c.values[key] = value
}

func (c *traceCtxCarrier) Keys() []string {
	keys := make([]string, 0, len(c.values))
	for k := range c.values {
		keys = append(keys, k)
	}
	return keys
}

// ============================================================================
// 生产者 Trace 封装 (XAdd)
// ============================================================================

// XAddTrace 添加消息到 Stream，带 tracing
func XAddTrace(ctx context.Context, client *redis.Client, stream string, values map[string]interface{}) (string, error) {
	ctx, span := observability.StartSpan(ctx, "redisx.XAdd",
		trace.WithAttributes(
			attribute.String("stream", stream),
			attribute.Int("values_count", len(values)),
		),
	)
	defer span.End()

	result, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Result()

	if err != nil {
		span.RecordError(err)
	}

	return result, err
}

// XAddTraceWithMaxLen 添加消息到 Stream，带 tracing 和 MaxLen，并将 trace context 注入消息
func XAddTraceWithMaxLen(ctx context.Context, client *redis.Client, stream string, maxLen int64, values map[string]interface{}) (string, error) {
	ctx, span := observability.StartSpan(ctx, "redisx.XAdd",
		trace.WithAttributes(
			attribute.String("stream", stream),
			attribute.Int64("max_len", maxLen),
			attribute.Int("values_count", len(values)),
		),
	)
	defer span.End()

	// 注入 trace context 到消息中
	carrier := &traceCtxCarrier{values: values}
	propagator := observability.GetGlobalPropagator()
	propagator.Inject(ctx, carrier)

	result, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
		MaxLen: maxLen,
	}).Result()

	if err != nil {
		span.RecordError(err)
	}

	return result, err
}

// ============================================================================
// 消费者 Trace 封装 (XReadGroup)
// ============================================================================

// messageCarrier 从 Redis Stream消息中提取 trace context
type messageCarrier struct {
	msg map[string]interface{}
}

func (m *messageCarrier) Get(key string) string {
	if v, ok := m.msg[key]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func (m *messageCarrier) Set(key, value string) {
	m.msg[key] = value
}

func (m *messageCarrier) Keys() []string {
	keys := make([]string, 0, len(m.msg))
	for k := range m.msg {
		keys = append(keys, k)
	}
	return keys
}

// extractTraceContext 从消息中提取 trace context 并创建子 span
func extractTraceContext(ctx context.Context, msg map[string]interface{}) context.Context {
	carrier := &messageCarrier{msg: msg}
	propagator := observability.GetGlobalPropagator()
	return propagator.Extract(ctx, carrier)
}

// XReadGroupTrace 消费消息，带 tracing，并从消息中提取 trace context 继续链路
func XReadGroupTrace(ctx context.Context, client *redis.Client, group, consumer string, streams []string, blockMs int) ([]redis.XStream, error) {
	ctx, span := observability.StartSpan(ctx, "redisx.XReadGroup",
		trace.WithAttributes(
			attribute.String("group", group),
			attribute.String("consumer", consumer),
			attribute.StringSlice("streams", streams),
			attribute.Int("block_ms", blockMs),
		),
	)
	defer span.End()

	result, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  streams,
		Block:    0,
		Count:    10,
	}).Result()

	if err != nil && err != redis.Nil {
		span.RecordError(err)
	}

	// 为每条消息创建子 span，形成链路
	for _, stream := range result {
		for _, msg := range stream.Messages {
			msgCtx := extractTraceContext(ctx, msg.Values)
			_, msgSpan := observability.StartSpan(msgCtx, "redisx.consume",
				trace.WithAttributes(
					attribute.String("stream", stream.Stream),
					attribute.String("msg_id", msg.ID),
				),
			)
			msgSpan.End()
		}
	}

	span.SetAttributes(attribute.Int("messages_count", len(result)))
	return result, err
}

// XAckTrace 确认消息已处理
func XAckTrace(ctx context.Context, client *redis.Client, stream, group string, ids ...string) (int64, error) {
	ctx, span := observability.StartSpan(ctx, "redisx.XAck",
		trace.WithAttributes(
			attribute.String("stream", stream),
			attribute.String("group", group),
			attribute.Int("ids_count", len(ids)),
		),
	)
	defer span.End()

	result, err := client.XAck(ctx, stream, group, ids...).Result()
	if err != nil {
		span.RecordError(err)
	}
	return result, err
}

// ============================================================================
// 连接健康检查 Trace 封装
// ============================================================================

// PingTrace Ping Redis，带 tracing
func PingTrace(ctx context.Context, client *redis.Client) error {
	ctx, span := observability.StartSpan(ctx, "redisx.Ping")
	defer span.End()

	err := client.Ping(ctx).Err()
	if err != nil {
		span.RecordError(err)
	}
	return err
}