package rocketx

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/gospacex/mqx"
	"gopkg.in/yaml.v3"
)

var activeConfigCache sync.Map

// ParseFile 从 YAML 文件读取指定 key 的配置 (支持热更新快照)
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
	if err != nil { return nil, "", fmt.Errorf("rocketx: read config file: %w", err) }

	var all map[string]*mqx.Config
	if err := yaml.Unmarshal(data, &all); err != nil {
		var single mqx.Config
		if err2 := yaml.Unmarshal(data, &single); err2 != nil { return nil, "", fmt.Errorf("rocketx: parse yaml: %w", err) }
		if configKey == "" { configKey = "default" }
		activeConfigCache.Store(path, &single)
		return &single, configKey, nil
	}

	if configKey == "" {
		for k, v := range all {
			activeConfigCache.Store(path, v)
			return v, k, nil
		}
		return nil, "", fmt.Errorf("rocketx: empty config file")
	}

	cfg, ok := all[configKey]
	if !ok { return nil, "", fmt.Errorf("rocketx: config key %q not found", configKey) }
	
	activeConfigCache.Store(path, cfg)
	return cfg, configKey, nil
}

func splitPath(path string) (string, string) {
	if i := strings.LastIndex(path, "#"); i > 0 { return path[:i], path[i+1:] }
	return path, ""
}
