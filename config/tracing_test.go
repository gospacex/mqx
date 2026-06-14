package config

import (
	"encoding/base64"
	"strings"
	"testing"
)

// decodeBasicAuth 反序列化 "Basic <base64>" header，校验前缀并返回
// 解码后的 user:pass 字符串，方便断言原始凭据。
//
// 失败时 t.Fatal —— Basic 头若解码不出原文，说明编码过程损坏，不是凭据问题。
func decodeBasicAuth(t *testing.T, header string) string {
	t.Helper()
	const prefix = "Basic "
	if !strings.HasPrefix(header, prefix) {
		t.Fatalf("expected header to start with %q, got %q", prefix, header)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(header, prefix))
	if err != nil {
		t.Fatalf("base64 decode failed for %q: %v", header, err)
	}
	return string(raw)
}

// TestValidate_BasicAuth_AutoInject 锁住正向路径：Username + Password 都非空、
// 用户未在 Headers 里显式写 Authorization 时，Validate 必须把凭据编码为
// "Basic <base64(user:pass)>" 注入到 Headers["Authorization"]。
//
// 这是 OTLP 后端（Tempo / Jaeger-with-auth / SigNoz）推荐的兜底方式。
func TestValidate_BasicAuth_AutoInject(t *testing.T) {
	c := &TracingConfig{
		Enabled:  true,
		Username: "alice",
		Password: "s3cret",
	}
	c.Validate()

	got, ok := c.Headers["Authorization"]
	if !ok {
		t.Fatal("expected Headers[\"Authorization\"] to be auto-injected, but key absent")
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:s3cret"))
	if got != want {
		t.Errorf("Authorization header mismatch:\n  got:  %q\n  want: %q", got, want)
	}
	if decoded := decodeBasicAuth(t, got); decoded != "alice:s3cret" {
		t.Errorf("decoded Basic Auth payload = %q, want %q", decoded, "alice:s3cret")
	}
}

// TestValidate_BasicAuth_EmptyPassword 覆盖边界：Password 为空但 Username 非空
// 仍要注入。HTTP Basic Auth 规范允许 user: 后无 password；这里不能因为
// Password=="" 就跳过注入（否则 Username 设置形同虚设）。
func TestValidate_BasicAuth_EmptyPassword(t *testing.T) {
	c := &TracingConfig{
		Enabled:  true,
		Username: "alice",
		Password: "",
	}
	c.Validate()

	got, ok := c.Headers["Authorization"]
	if !ok {
		t.Fatal("expected Basic Auth to be injected even with empty Password")
	}
	if decoded := decodeBasicAuth(t, got); decoded != "alice:" {
		t.Errorf("decoded = %q, want %q (trailing colon must be preserved)", decoded, "alice:")
	}
}

// TestValidate_BasicAuth_EmptyUsername 覆盖反向边界：Username 为空则**不**注入。
// 这是契约 —— Validate 不会凭空生成凭据；空 Username 表示未启用 Basic Auth。
func TestValidate_BasicAuth_EmptyUsername(t *testing.T) {
	c := &TracingConfig{
		Enabled:  true,
		Username: "",
		Password: "s3cret",
		Headers:  map[string]string{"X-Other": "value"},
	}
	c.Validate()

	if _, hasAuth := c.Headers["Authorization"]; hasAuth {
		t.Error("Authorization must NOT be injected when Username is empty")
	}
	// 其它已存在的 header 必须保留。
	if c.Headers["X-Other"] != "value" {
		t.Errorf("unrelated header lost: got %q", c.Headers["X-Other"])
	}
}

// TestValidate_BasicAuth_RespectsUserHeader 锁住用户优先级：Headers 里已有
// Authorization（无论是 Bearer Token、ApiKey 还是其它 scheme）时，Validate
// **不得**覆盖。这是"显式 > 推导"的契约。
func TestValidate_BasicAuth_RespectsUserHeader(t *testing.T) {
	c := &TracingConfig{
		Enabled:  true,
		Username: "alice",
		Password: "s3cret",
		Headers: map[string]string{
			"Authorization": "Bearer my-token-xyz",
		},
	}
	c.Validate()

	if got, want := c.Headers["Authorization"], "Bearer my-token-xyz"; got != want {
		t.Errorf("user-provided Authorization was overwritten:\n  got:  %q\n  want: %q", got, want)
	}
}

// TestValidate_BasicAuth_NilHeadersInit 验证 Headers 为 nil 时也能被正确初始化。
// 防止调用方期望 c.Headers["Authorization"] 直接索引就拿到值（map 零值是 nil，
// 写入前必须 make）。
func TestValidate_BasicAuth_NilHeadersInit(t *testing.T) {
	c := &TracingConfig{
		Enabled:  true,
		Username: "alice",
		Password: "s3cret",
		Headers:  nil, // 显式 nil，模拟 yaml 解析后未初始化的 map
	}
	c.Validate()

	if c.Headers == nil {
		t.Fatal("Validate did not initialize Headers map; downstream range/index will panic")
	}
	got, ok := c.Headers["Authorization"]
	if !ok {
		t.Fatal("expected Authorization key after Validate")
	}
	if !strings.HasPrefix(got, "Basic ") {
		t.Errorf("expected Basic prefix, got %q", got)
	}
}

// TestValidate_BasicAuth_Disabled 锁住 Enabled=false 的早返回：Validate 必须在
// 检测到 Enabled=false 时立即返回，**绝不**触碰 Headers —— 否则就是把"未启用"
// 误升级为"启用 Basic Auth"，是 silent behavior change。
func TestValidate_BasicAuth_Disabled(t *testing.T) {
	c := &TracingConfig{
		Enabled:  false,
		Username: "alice",
		Password: "s3cret",
		Headers:  map[string]string{"X-Keep": "me"},
	}
	c.Validate()

	if _, hasAuth := c.Headers["Authorization"]; hasAuth {
		t.Error("Authorization was injected despite Enabled=false")
	}
	if c.Headers["X-Keep"] != "me" {
		t.Error("unrelated header was modified on disabled path")
	}
}

// TestValidate_BasicAuth_SpecialChars 验证凭据含特殊字符（含冒号、空格、UTF-8）
// 时 base64 编码仍能 round-trip 出原文。Basic Auth 标准只保证 ASCII，但 mqx
// 自身不限制 —— 透传给标准库 base64 即可。
func TestValidate_BasicAuth_SpecialChars(t *testing.T) {
	c := &TracingConfig{
		Enabled:  true,
		Username: "al:ice",
		Password: "p@ss w0rd 中文",
	}
	c.Validate()

	got := c.Headers["Authorization"]
	decoded := decodeBasicAuth(t, got)
	if decoded != "al:ice:p@ss w0rd 中文" {
		t.Errorf("decoded = %q, want %q (colon in username and UTF-8 must round-trip)", decoded, "al:ice:p@ss w0rd 中文")
	}
}
