package observability

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
)

type natsHeaderCarrier struct {
	header nats.Header
}

func (c natsHeaderCarrier) Get(key string) string { return c.header.Get(key) }
func (c natsHeaderCarrier) Set(key string, value string) { c.header.Set(key, value) }
func (c natsHeaderCarrier) Keys() []string {
	var keys []string
	for k := range c.header { keys = append(keys, k) }
	return keys
}

// InjectNatsTrace 注入 Trace 到 NATS Headers
func InjectNatsTrace(ctx context.Context, header nats.Header) {
	otel.GetTextMapPropagator().Inject(ctx, natsHeaderCarrier{header: header})
}

// ExtractNatsTrace 提取 Trace
func ExtractNatsTrace(ctx context.Context, header nats.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, natsHeaderCarrier{header: header})
}
