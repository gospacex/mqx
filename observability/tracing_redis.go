package observability

import (
	"context"

	"go.opentelemetry.io/otel"
)

type redisHeaderCarrier struct {
	values map[string]interface{}
}

func (c redisHeaderCarrier) Get(key string) string {
	if c.values == nil { return "" }
	if val, ok := c.values[key]; ok {
		if str, ok := val.(string); ok { return str }
	}
	return ""
}

func (c redisHeaderCarrier) Set(key string, value string) {
	if c.values != nil {
		c.values[key] = value
	}
}

func (c redisHeaderCarrier) Keys() []string {
	var keys []string
	for k := range c.values { keys = append(keys, k) }
	return keys
}

// InjectRedisTrace 注入 Trace 到 Redis Stream Values 字典
func InjectRedisTrace(ctx context.Context, values map[string]interface{}) {
	carrier := redisHeaderCarrier{values: values}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// ExtractRedisTrace 提取 Trace
func ExtractRedisTrace(ctx context.Context, values map[string]interface{}) context.Context {
	carrier := redisHeaderCarrier{values: values}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
