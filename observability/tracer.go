package observability

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracerName 是 mqx 在全局 OTel 注册时使用的 instrumentation 名。
// 所有通过 observability.StartSpan 创建的 span 都挂在这个 instrumentation 名下。
const tracerName = "mqx"

// GetGlobalTracer 返回 mqx 全局 tracer。
//
// 注意：每次调用都从 otel 全局重新取 tracer，**绝不**在包初始化阶段缓存。
// 早期版本在 init() 里执行 `GlobalTracer = otel.Tracer("mqx")`，但那时调用方
// 还没机会调 otel.SetTracerProvider(...)——拿到的是默认 NoopTracerProvider 的
// tracer 并永久冻住。后续 InitTracing / initOTel 切换全局 TracerProvider
// 也无效，所有 span 走 noop，调用链静默丢失。这是 pulsarx_test 看不到 Jaeger
// 数据的根因。
//
// 修复策略：跟 server.go:tracerWrapper.Trace 保持一致——每次都从全局查。
func GetGlobalTracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// GetGlobalPropagator 返回全局 propagator
func GetGlobalPropagator() propagation.TextMapPropagator {
	return otel.GetTextMapPropagator()
}

// StartSpan 启动一个新的 span。
// 每次都从当前全局 TracerProvider 拿 tracer，保证 InitTracing / initOTel
// 设置的 TracerProvider 生效。绝不缓存（见 GetGlobalTracer 注释）。
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx, name, opts...)
}

// SpanFromContext 从 context 获取当前 span
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// AddEvent 向当前 span 添加事件
func AddEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	SpanFromContext(ctx).AddEvent(name, trace.WithAttributes(attrs...))
}

// SetAttributes 设置 span 属性
func SetAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	SpanFromContext(ctx).SetAttributes(attrs...)
}

// RecordError 记录错误到 span
func RecordError(ctx context.Context, err error) {
	SpanFromContext(ctx).RecordError(err)
}
