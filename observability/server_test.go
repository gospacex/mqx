package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestInitTracing_Disabled 验证：当 Config.Enabled 为 false 时，InitTracing
// 不会创建任何 OTel 资源，也不会替换全局 TracerProvider。这是修复
// "trace.enabled:false 时调用链不应启动" 行为的核心契约。
func TestInitTracing_Disabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
	}{
		{
			name: "explicitly disabled config",
			cfg: &Config{
				Enabled:        false,
				JaegerEndpoint: "should-not-be-used:4317",
				ServiceName:    "should-not-be-used",
			},
		},
		{
			name: "nil config falls back to DefaultConfig (disabled by default)",
			cfg:  nil,
		},
		{
			name: "default config",
			cfg:  DefaultConfig(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 抓取调用前的全局 TracerProvider，调用后必须保持一致。
			before := otel.GetTracerProvider()

			cleanup, err := InitTracing(context.Background(), tc.cfg)
			if err != nil {
				t.Fatalf("InitTracing returned error: %v", err)
			}
			if cleanup == nil {
				t.Fatal("InitTracing returned nil cleanup; expected no-op function")
			}

			// cleanup 必须可以安全调用，因为底层 tracerProvider 仍为 nil。
			// 关闭分支返回的 no-op cleanup 不返回 error，单纯调用不能 panic 即可。
			cleanup(context.Background())

			// 关闭配置下，全局 TracerProvider 绝不能被替换。
			after := otel.GetTracerProvider()
			if before != after {
				t.Errorf("InitTracing replaced the global TracerProvider when disabled")
			}
		})
	}
}

// TestDefaultConfig_DisabledByDefault 锁住安全默认值：DefaultConfig() 必须
// 把 Enabled 设为 false，避免调用方在传入 nil 时意外启动调用链。
func TestDefaultConfig_DisabledByDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Enabled {
		t.Errorf("DefaultConfig().Enabled = true; expected false to keep tracing off by default")
	}
}

// TestStartSpan_UsesGlobalTracerProvider 是冻结 noop tracer bug 的回归测试。
//
// 早期版本的 tracer.go 在 init() 里执行 `GlobalTracer = otel.Tracer("mqx")`，
// 但那是在 otel.SetTracerProvider 被调用之前——拿到的是 NoopTracerProvider 的
// tracer 并永久冻住。后续 InitTracing / initOTel 切换全局 TracerProvider 也无效，
// 所有 span 走 noop，调用链静默丢失（pulsarx_test 看不到 Jaeger 数据的根因）。
//
// 修复后 StartSpan 每次都从 otel.Tracer 重新拿；本测试断言切换全局 provider 后
// 立即生效。
func TestStartSpan_UsesGlobalTracerProvider(t *testing.T) {
	// 隔离全局状态，避免污染同包其他测试。
	prev := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	// 装一个真实的 TracerProvider 当全局——这里不连 exporter，只验证 StartSpan
	// 能不能拿到非 noop 的 tracer。
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)

	_, span := StartSpan(context.Background(), "regression-test-span")
	defer span.End()

	if !span.SpanContext().IsValid() {
		t.Fatal("StartSpan returned an invalid SpanContext; the tracer is still " +
			"frozen to NoopTracerProvider. StartSpan must read the current " +
			"global TracerProvider, not a cached one from package init().")
	}
	if span.SpanContext().TraceID().IsValid() == false {
		t.Errorf("expected valid TraceID from a real TracerProvider, got zero value")
	}
}
