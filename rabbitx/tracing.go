package rabbitx

import (
	"context"
	"fmt"

	"github.com/gospacex/mqx/observability"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// PublishWithContextTrace 发送 RabbitMQ 消息，按 enabled 决定是否开启 OTel 追踪。
//
// enabled=false: 直接走 ch.PublishWithContext，不创建 span、不注入 headers。
// enabled=true:  创建 producer span，注入 trace context 到 amqp.Table，
//
//	messaging.* OTel semantic attributes，记录发送错误。
//
// msg.Headers 若为 nil 会被原地初始化。
func PublishWithContextTrace(
	ctx context.Context,
	ch *amqp.Channel,
	exchange, key string,
	msg amqp.Publishing,
	enabled bool,
) error {
	if !enabled {
		return ch.PublishWithContext(ctx, exchange, key, false, false, msg)
	}

	ctx, span := observability.StartSpan(ctx, "rabbitx.Publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination", exchange),
			attribute.String("messaging.rabbitmq.routing_key", key),
		),
	)
	defer span.End()

	if msg.Headers == nil {
		msg.Headers = make(amqp.Table)
	}
	observability.InjectRabbitTrace(ctx, msg.Headers)

	err := ch.PublishWithContext(ctx, exchange, key, false, false, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// ConsumeTrace 启动 RabbitMQ 消费者通道。
//
// enabled 参数是配置开关的实现位点 —— 调用方在拿到 delivery 后是否创建子 span
// 由 enabled 决定：enabled=false 时跳过所有 span 操作；enabled=true 时调用方
// 用 observability.ExtractRabbitTrace 提取父 ctx 后开 consumer span。
//
// 这里只做 ch.Consume 的转发（包装 enabled 检查，避免在未启用时引入 noop tracer
// 开销），span 在调用方 receive 循环里按需创建 —— 因为 amqp.Delivery 是值类型，
// span 生命周期必须在调用方 End 才能覆盖业务处理耗时。
func ConsumeTrace(
	ctx context.Context,
	ch *amqp.Channel,
	queue, consumer string,
	autoAck bool,
	enabled bool,
) (<-chan amqp.Delivery, error) {
	msgs, err := ch.Consume(queue, consumer, autoAck, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("rabbitx.ConsumeTrace: %w", err)
	}
	// enabled 在此签名中作为配置开关位点；调用方负责按 enabled 选择是否创建 span。
	// ctx 预留给将来需要在 goroutine 内做 batch span 的扩展点。
	_ = ctx
	_ = enabled
	return msgs, nil
}