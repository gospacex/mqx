package natsx

import (
	"context"

	"github.com/gospacex/mqx/observability"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// PublishTrace 发送 Core NATS 消息，带 tracing，将 trace context 注入到 NATS Headers
func PublishTrace(ctx context.Context, nc *nats.Conn, subj string, data []byte) error {
	ctx, span := observability.StartSpan(ctx, "natsx.Publish",
		trace.WithAttributes(attribute.String("subject", subj)),
	)
	defer span.End()

	msg := nats.NewMsg(subj)
	msg.Data = data
	observability.InjectNatsTrace(ctx, msg.Header)

	err := nc.PublishMsg(msg)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// QueueSubscribeTrace 订阅 Core NATS 队列，带 tracing，从 NATS Headers 提取 trace context 并创建消费 span
func QueueSubscribeTrace(ctx context.Context, nc *nats.Conn, subj, queue string, handler func(ctx context.Context, msg *nats.Msg)) (*nats.Subscription, error) {
	sub, err := nc.QueueSubscribe(subj, queue, func(msg *nats.Msg) {
		// 从 headers 提取 trace context
		extractCtx := observability.ExtractNatsTrace(ctx, msg.Header)

		extractCtx, span := observability.StartSpan(extractCtx, "natsx.consume",
			trace.WithAttributes(
				attribute.String("subject", msg.Subject),
				attribute.String("queue", queue),
			),
		)
		defer span.End()

		handler(extractCtx, msg)
	})
	return sub, err
}

// JSPublishTrace 发送 JetStream 消息，带 tracing
func JSPublishTrace(ctx context.Context, js nats.JetStreamContext, subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	ctx, span := observability.StartSpan(ctx, "natsx.JSPublish",
		trace.WithAttributes(attribute.String("subject", subj)),
	)
	defer span.End()

	msg := nats.NewMsg(subj)
	msg.Data = data
	observability.InjectNatsTrace(ctx, msg.Header)

	ack, err := js.PublishMsg(msg, opts...)
	if err != nil {
		span.RecordError(err)
	}
	return ack, err
}

// JSQueueSubscribeTrace JetStream 队列订阅，带 tracing
func JSQueueSubscribeTrace(ctx context.Context, js nats.JetStreamContext, subj, queue string, handler func(ctx context.Context, msg *nats.Msg), opts ...nats.SubOpt) (*nats.Subscription, error) {
	sub, err := js.QueueSubscribe(subj, queue, func(msg *nats.Msg) {
		extractCtx := observability.ExtractNatsTrace(ctx, msg.Header)

		extractCtx, span := observability.StartSpan(extractCtx, "natsx.js.consume",
			trace.WithAttributes(
				attribute.String("subject", msg.Subject),
				attribute.String("queue", queue),
			),
		)
		defer span.End()

		handler(extractCtx, msg)
	}, opts...)
	return sub, err
}
