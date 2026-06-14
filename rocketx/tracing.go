package rocketx

import (
	"context"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/gospacex/mqx/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// propMapCarrier 基于 map[string]string 实现 propagation.TextMapCarrier。
// 用于在 RocketMQ primitive.Message 的 Properties 与 OTel propagator 之间桥接。
type propMapCarrier struct {
	props map[string]string
}

func (c *propMapCarrier) Get(key string) string {
	if c.props == nil {
		return ""
	}
	return c.props[key]
}

func (c *propMapCarrier) Set(key, value string) {
	if c.props == nil {
		c.props = make(map[string]string)
	}
	c.props[key] = value
}

func (c *propMapCarrier) Keys() []string {
	if c.props == nil {
		return nil
	}
	keys := make([]string, 0, len(c.props))
	for k := range c.props {
		keys = append(keys, k)
	}
	return keys
}

// SendSyncTrace 发送消息，带 tracing 与配置开关。
//
//   - enabled=false: 直接调底层 producer.SendSync，不创建 span、不注入 trace 上下文。
//   - enabled=true:  StartSpan 创建发送 span，将 ctx 中的 trace 上下文注入到 msg 的
//     Properties 后再发送。
//
// 注入语义在 enabled=false 时被跳过，以保证 trace pipeline 关闭时无任何 OTel 副作用。
func SendSyncTrace(ctx context.Context, p rocketmq.Producer, msg *primitive.Message, enabled bool) (*primitive.SendResult, error) {
	if !enabled {
		return p.SendSync(ctx, msg)
	}

	ctx, span := observability.StartSpan(ctx, "rocketx.SendSync",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rocketmq"),
			attribute.String("messaging.destination", msg.Topic),
		),
	)
	defer span.End()

	carrier := &propMapCarrier{props: msg.GetProperties()}
	propagator := observability.GetGlobalPropagator()
	propagator.Inject(ctx, carrier)
	if carrier.props != nil {
		for k, v := range carrier.props {
			msg.WithProperty(k, v)
		}
	}

	res, err := p.SendSync(ctx, msg)
	if err != nil {
		span.RecordError(err)
	}
	return res, err
}

// ConsumeTrace 在消费者 handler 内部使用：提取消息属性中的 trace context，
// 创建 ConsumerKind span 后调用 fn(ctx)。fn 通常执行业务处理逻辑。
//
//   - enabled=false: 直接调用 fn(ctx)，不创建 span、不提取 trace 上下文。
//   - enabled=true:  从 msg.Properties 提取上游 trace 上下文，创建 ConsumerKind
//     span 后调用 fn，并在 fn 返回时结束 span。
//
// 返回值透传 fn 的 error。
func ConsumeTrace(ctx context.Context, msg *primitive.MessageExt, enabled bool, fn func(context.Context) error) error {
	if !enabled {
		return fn(ctx)
	}

	carrier := &propMapCarrier{props: msg.GetProperties()}
	propagator := observability.GetGlobalPropagator()
	extractedCtx := propagator.Extract(ctx, carrier)

	_, span := observability.StartSpan(extractedCtx, "rocketx.Consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rocketmq"),
			attribute.String("messaging.destination", msg.Topic),
			attribute.String("messaging.message_id", msg.MsgId),
		),
	)
	defer span.End()

	if err := fn(extractedCtx); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// 确保 propagation.TextMapCarrier 接口被 propMapCarrier 完整实现。
// 此行不会执行，仅用于编译期校验。
var _ propagation.TextMapCarrier = (*propMapCarrier)(nil)