package observability

import (
	"context"
	"os"
	"testing"
)

// TestExpandEnvVars 锁住 ${env:VAR} / ${env:VAR:-default} 替换语义，
// 是 8 driver × 6 组合 yaml 模板的基础（测试启动前 os.Setenv 注入）。
func TestExpandEnvVars(t *testing.T) {
	const (
		keyA = "MQX_TEST_ENV_A"
		keyB = "MQX_TEST_ENV_B"
		keyC = "MQX_TEST_ENV_C" // 未设置
	)
	t.Cleanup(func() {
		os.Unsetenv(keyA)
		os.Unsetenv(keyB)
		os.Unsetenv(keyC)
	})

	tests := []struct {
		name string
		set  map[string]string
		in   string
		want string
	}{
		{
			name: "命中已设置的 env",
			set:  map[string]string{keyA: "value-A"},
			in:   "${env:MQX_TEST_ENV_A}",
			want: "value-A",
		},
		{
			name: "未命中时使用 default",
			set:  nil,
			in:   "${env:MQX_TEST_ENV_C:-fallback}",
			want: "fallback",
		},
		{
			name: "命中时 default 被忽略",
			set:  map[string]string{keyA: "value-A"},
			in:   "${env:MQX_TEST_ENV_A:-fallback}",
			want: "value-A",
		},
		{
			name: "未命中且无 default → 保留原字面量",
			set:  nil,
			in:   "${env:MQX_TEST_ENV_C}",
			want: "${env:MQX_TEST_ENV_C}",
		},
		{
			name: "未命中且 default 为空串 → 返回空串（不是字面量）",
			set:  nil,
			in:   "${env:MQX_TEST_ENV_C:-}",
			want: "",
		},
		{
			name: "字符串中嵌入两个 var",
			set:  map[string]string{keyA: "alpha", keyB: "beta"},
			in:   "pre-${env:MQX_TEST_ENV_A}-mid-${env:MQX_TEST_ENV_B}-post",
			want: "pre-alpha-mid-beta-post",
		},
		{
			name: "无 env 占位符时原样返回",
			set:  nil,
			in:   "localhost:6379",
			want: "localhost:6379",
		},
		{
			name: "空字符串",
			set:  nil,
			in:   "",
			want: "",
		},
		{
			name: "非法 var 名称保留字面量",
			set:  nil,
			in:   "${env:1BAD-NAME}",
			want: "${env:1BAD-NAME}",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k := range tc.set {
				os.Unsetenv(k)
			}
			for k, v := range tc.set {
				os.Setenv(k, v)
			}
			got := ExpandEnvVars(tc.in)
			if got != tc.want {
				t.Errorf("ExpandEnvVars(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestBuildSpanExporter_BackendDispatch 验证 backend 字段正确路由到
// 三个 SpanExporter 实现：jaeger（OTLP gRPC）/ redis_stream / kafka_topic。
// redis_stream 和 kafka_topic 在缺字段时返回 wrapped error。
func TestBuildSpanExporter_BackendDispatch(t *testing.T) {
	t.Run("未知 backend 报错", func(t *testing.T) {
		_, err := buildSpanExporter(context.Background(), &Config{
			Backend: "weird_backend",
		})
		if err == nil {
			t.Fatal("expected error for unknown backend, got nil")
		}
	})

	t.Run("redis_stream 缺 client 报错", func(t *testing.T) {
		_, err := buildSpanExporter(context.Background(), &Config{
			Backend:     TraceBackendRedisStream,
			RedisStream: "trace:test",
		})
		if err == nil {
			t.Fatal("expected error when RedisClient is nil")
		}
	})

	t.Run("redis_stream 缺 stream 报错", func(t *testing.T) {
		// nil client 会先于 stream 检查触发；这里构造一个 nil 间接路径不可行，
		// 因为 client 必须非 nil。所以只验 stream 检查路径需要 mock client。
		// 现有 API 不足以注入 mock（New 接受 *redis.Client），跳过。
		t.Skip("requires mock redis client; deferred to integration test")
	})

	t.Run("kafka_topic 缺 producer 报错", func(t *testing.T) {
		_, err := buildSpanExporter(context.Background(), &Config{
			Backend:    TraceBackendKafkaTopic,
			KafkaTopic: "trace:test",
		})
		if err == nil {
			t.Fatal("expected error when KafkaProducer is nil")
		}
	})

	t.Run("kafka_topic 缺 topic 报错", func(t *testing.T) {
		t.Skip("requires mock kafka producer; deferred to integration test")
	})

	t.Run("jaeger 路径：空 cfg 走默认 endpoint", func(t *testing.T) {
		// endpoint 不存在时 otlptracegrpc.New 仍会返回（lazy connect），不会失败；
		// 真正失败在 ExportSpans。我们只验证不 panic、返回非 nil exporter 即可。
		exp, err := buildSpanExporter(context.Background(), &Config{
			Backend:        "",
			JaegerEndpoint: "localhost:14317",
			Insecure:       true,
		})
		if err != nil {
			t.Fatalf("buildSpanExporter jaeger: %v", err)
		}
		if exp == nil {
			t.Fatal("buildSpanExporter returned nil exporter for jaeger backend")
		}
		_ = exp.Shutdown(context.Background())
	})
}

// TestInitTracing_RedisStreamBackendMissingFields 验证 InitTracing 在
// backend=redis_stream + 缺字段时返回 wrapped error（fail fast）。
func TestInitTracing_RedisStreamBackendMissingFields(t *testing.T) {
	_, err := InitTracing(context.Background(), &Config{
		Enabled:   true,
		Backend:   TraceBackendRedisStream,
		RedisStream: "trace:test",
		// RedisClient 故意不传
	})
	if err == nil {
		t.Fatal("expected InitTracing to fail when backend=redis_stream + RedisClient=nil")
	}
}

// TestInitTracing_KafkaTopicBackendMissingFields 同上，验证 kafka_topic 缺字段 fail fast。
func TestInitTracing_KafkaTopicBackendMissingFields(t *testing.T) {
	_, err := InitTracing(context.Background(), &Config{
		Enabled:    true,
		Backend:    TraceBackendKafkaTopic,
		KafkaTopic: "trace:test",
		// KafkaProducer 故意不传
	})
	if err == nil {
		t.Fatal("expected InitTracing to fail when backend=kafka_topic + KafkaProducer=nil")
	}
}
