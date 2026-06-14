package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/gospacex/mqx"
)

// ConfigFingerprint 生成配置指纹（去重与缓存 Key）
// 按照执行计划 Task 1.1 必须对切片属性进行字典序排序，保证防抖幂等性。
func ConfigFingerprint(cfg *mqx.Config) string {
	if cfg == nil {
		return ""
	}

	// 1. 克隆并排序 Addrs 避免顺序不同导致建多条连接
	addrs := make([]string, len(cfg.Addrs))
	copy(addrs, cfg.Addrs)
	sort.Strings(addrs)

	// 2. 消费者 Topics 也可能顺序不同
	topics := make([]string, len(cfg.Consumer.Topics))
	copy(topics, cfg.Consumer.Topics)
	sort.Strings(topics)

	raw := fmt.Sprintf("%s|%s|%v|%s|%s|%s|%v",
		cfg.Driver,
		cfg.Mode,
		addrs,
		cfg.Auth.Username,
		cfg.Producer.Topic,
		cfg.Consumer.Group,
		topics,
	)
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:16]) // 取前 32 字符作为指纹
}
