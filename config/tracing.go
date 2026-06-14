package config

import (
	"encoding/base64"
	"strings"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TracingConfig V2 unified configuration for OpenTelemetry tracing
type TracingConfig struct {
	Enabled      bool   `yaml:"enabled" json:"enabled" mapstructure:"enabled"`
	ServiceName  string `yaml:"service_name" json:"service_name" mapstructure:"service_name"`
	Exporter     string `yaml:"exporter" json:"exporter" mapstructure:"exporter"`
	Endpoint     string `yaml:"endpoint" json:"endpoint" mapstructure:"endpoint"`
	Protocol     string `yaml:"protocol" json:"protocol" mapstructure:"protocol"`
	SamplerType  string `yaml:"sampler_type" json:"sampler_type" mapstructure:"sampler_type"`
	SamplerRatio float64 `yaml:"sampler_ratio" json:"sampler_ratio" mapstructure:"sampler_ratio"`
	Topic        string `yaml:"topic" json:"topic" mapstructure:"topic"`
	Stream       string `yaml:"stream" json:"stream" mapstructure:"stream"`

	// Insecure 关闭 TLS，仅用于本地/开发环境；生产环境必须为 false。
	// 适用 exporter: jaeger (gRPC/HTTP)、kafka。
	Insecure bool `yaml:"insecure" json:"insecure" mapstructure:"insecure"`

	// Username / Password 提供 OTLP gRPC/HTTP Basic Auth。
	// 业界惯例更推荐使用 Headers 携带 Bearer Token；这里同时支持两种：
	// 当 Username/Password 都非空时，会被序列化为 "Authorization: Basic <base64>" header。
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`

	// Headers 透传给 OTLP exporter 的自定义 header (e.g. "Authorization=Bearer xxx")。
	// 优先级高于 Username/Password 推导出的 Basic header。
	Headers map[string]string `yaml:"headers" json:"headers" mapstructure:"headers"`

	// RedisPassword 当 exporter=redis_stream 时，访问 Redis 的密码。
	// 默认空字符串（无鉴权）。
	RedisPassword string `yaml:"redis_password" json:"redis_password" mapstructure:"redis_password"`
	// RedisUsername 当 exporter=redis_stream 时，访问 Redis 的用户名 (Redis 6+ ACL)。
	// 默认空字符串（使用 default user）。
	RedisUsername string `yaml:"redis_username" json:"redis_username" mapstructure:"redis_username"`
}

// Validate applies default values, clamps ranges, and normalizes inputs safely without returning errors.
func (c *TracingConfig) Validate() {
	if !c.Enabled {
		return
	}

	if c.ServiceName == "" {
		c.ServiceName = "dbx"
	}

	// 1. Validate Exporter
	if c.Exporter == "" {
		c.Exporter = "jaeger"
	} else {
		c.Exporter = strings.ToLower(c.Exporter)
		if c.Exporter != "jaeger" && c.Exporter != "kafka" && c.Exporter != "redis_stream" {
			c.Exporter = "jaeger" // fallback to jaeger
		}
	}

	// 2. Validate Protocol
	if c.Protocol == "" {
		c.Protocol = "http"
	} else {
		c.Protocol = strings.ToLower(c.Protocol)
		if c.Protocol != "http" && c.Protocol != "grpc" {
			c.Protocol = "http" // fallback to http
		}
	}

	// 3. Validate Endpoint
	if c.Endpoint == "" {
		if c.Exporter == "kafka" {
			c.Endpoint = "localhost:9092"
		} else if c.Exporter == "redis_stream" {
			c.Endpoint = "localhost:6379"
		} else {
			if c.Protocol == "grpc" {
				c.Endpoint = "localhost:4317"
			} else {
				c.Endpoint = "localhost:4318"
			}
		}
	}

	// 4. Validate Sampler Type and Ratio
	if c.SamplerType == "" {
		c.SamplerType = "parent_based_trace_id_ratio"
	} else {
		c.SamplerType = strings.ToLower(c.SamplerType)
	}

	validSamplers := map[string]bool{
		"always_on":                   true,
		"always_off":                  true,
		"trace_id_ratio":              true,
		"parent_based":                true,
		"parent_based_trace_id_ratio": true,
	}

	if !validSamplers[c.SamplerType] {
		// fallback for invalid sampler type
		c.SamplerType = "parent_based_trace_id_ratio"
		c.SamplerRatio = 0.1
	}

	// Apply default ratio if unset for ratio-based samplers
	// (0.0 implies no sampling, users should use always_off for explicit 0)
	if c.SamplerRatio == 0 && strings.Contains(c.SamplerType, "ratio") {
		c.SamplerRatio = 0.1
	}

	// Clamp ratio between 0.0 and 1.0
	if c.SamplerRatio < 0.0 {
		c.SamplerRatio = 0.0
	} else if c.SamplerRatio > 1.0 {
		c.SamplerRatio = 1.0
	}

	// 5. Exporter specific defaults
	if c.Exporter == "kafka" && c.Topic == "" {
		c.Topic = "otel-traces"
	}
	if c.Exporter == "redis_stream" && c.Stream == "" {
		c.Stream = "otel-traces"
	}

	// 6. Headers: 当 Username/Password 非空且未显式设置 Authorization header 时，
	//    自动注入 Basic Auth header。这是业界 OTLP 后端（如 Tempo / Jaeger-with-auth）
	//    推荐的兜底方式；如果用户已经在 Headers 里写了 Authorization，则尊重用户配置。
	if c.Headers == nil {
		c.Headers = make(map[string]string)
	}
	if _, hasAuth := c.Headers["Authorization"]; !hasAuth && c.Username != "" {
		cred := c.Username + ":" + c.Password
		c.Headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(cred))
	}
}

// GetSampler returns the standard OTel sdktrace.Sampler based on the configuration.
func (c *TracingConfig) GetSampler() sdktrace.Sampler {
	switch c.SamplerType {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "trace_id_ratio":
		return sdktrace.TraceIDRatioBased(c.SamplerRatio)
	case "parent_based":
		// root span uses always_on
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	case "parent_based_trace_id_ratio":
		// root span uses trace_id_ratio
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(c.SamplerRatio))
	default:
		// fallback
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.1))
	}
}
