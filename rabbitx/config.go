package rabbitx

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/gospacex/mqx"
	"gopkg.in/yaml.v3"
)

// activeConfigCache 保存基于 path 的最新解析快照，供热更新机制使用
var activeConfigCache sync.Map // key: path, val: *mqx.Config

// ParseFile 从 YAML 文件读取指定 key 的配置
func ParseFile(path string) (*mqx.Config, string, error) {
	if val, ok := activeConfigCache.Load(path); ok {
		cfg := val.(*mqx.Config)
		_, configKey := splitPath(path)
		if configKey == "" { configKey = "default" }
		return cfg, configKey, nil
	}

	return parseFileFromDisk(path)
}

func parseFileFromDisk(path string) (*mqx.Config, string, error) {
	filePath, configKey := splitPath(path)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, "", fmt.Errorf("rabbitx: read config file: %w", err)
	}

	var all map[string]*mqx.Config
	if err := yaml.Unmarshal(data, &all); err != nil {
		var single mqx.Config
		if err2 := yaml.Unmarshal(data, &single); err2 != nil {
			return nil, "", fmt.Errorf("rabbitx: parse yaml: %w", err)
		}
		if configKey == "" { configKey = "default" }
		activeConfigCache.Store(path, &single)
		return &single, configKey, nil
	}

	if configKey == "" {
		for k, v := range all {
			activeConfigCache.Store(path, v)
			return v, k, nil
		}
		return nil, "", fmt.Errorf("rabbitx: empty config file")
	}

	cfg, ok := all[configKey]
	if !ok {
		return nil, "", fmt.Errorf("rabbitx: config key %q not found", configKey)
	}
	
	activeConfigCache.Store(path, cfg)
	return cfg, configKey, nil
}

func splitPath(path string) (string, string) {
	if i := strings.LastIndex(path, "#"); i > 0 {
		return path[:i], path[i+1:]
	}
	return path, ""
}

// BuildAMQPURI 将 mqx.Config 的参数拼接成标准的 amqp/amqps URI
func BuildAMQPURI(cfg *mqx.Config) (string, error) {
	if len(cfg.Addrs) == 0 {
		return "", fmt.Errorf("rabbitx: no addresses provided")
	}
	
	baseURI := cfg.Addrs[0]

	if !strings.HasPrefix(baseURI, "amqp://") && !strings.HasPrefix(baseURI, "amqps://") {
		if cfg.TLS.Enabled {
			baseURI = "amqps://" + baseURI
		} else {
			baseURI = "amqp://" + baseURI
		}
	}

	if cfg.Auth.Username != "" && !strings.Contains(baseURI, "@") {
		parts := strings.SplitN(baseURI, "://", 2)
		if len(parts) == 2 {
			authPart := fmt.Sprintf("%s:%s@", cfg.Auth.Username, cfg.Auth.Password)
			baseURI = parts[0] + "://" + authPart + parts[1]
		}
	}

	if cfg.RabbitMQ != nil && cfg.RabbitMQ.VHost != "" {
		vhost := cfg.RabbitMQ.VHost
		if !strings.HasPrefix(vhost, "/") { vhost = "/" + vhost }
		if !strings.HasSuffix(baseURI, vhost) {
			if vhost == "/" {
				baseURI = strings.TrimSuffix(baseURI, "/") + "/%2f"
			} else {
				baseURI = strings.TrimSuffix(baseURI, "/") + vhost
			}
		}
	}

	return baseURI, nil
}
