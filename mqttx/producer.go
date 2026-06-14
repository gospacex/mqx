package mqttx

import (
	"fmt"
	"github.com/gospacex/mqx"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// =====================================================================
// 生产者 (Single / Cluster) -> 返回 mqtt.Client
// =====================================================================

func P(path string) (mqtt.Client, error) { return PPS(path) }

func PPS(path string) (mqtt.Client, error) {
	cfg, _, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("mqttx.PPS: %w", err)
	}
	cfg.Mode = "single"
	client, _, err := getOrCreateClient(cfg)
	return client, err
}

func POS(cfg mqx.Config) (mqtt.Client, error) {
	cfg.Mode = "single"
	client, _, err := getOrCreateClient(&cfg)
	return client, err
}

func MustP(path string) mqtt.Client { return MustPPS(path) }

func MustPPS(path string) mqtt.Client {
	p, err := PPS(path)
	if err != nil {
		panic(fmt.Errorf("mqttx MustPPS failure: %w", err))
	}
	return p
}

func MustPOS(cfg mqx.Config) mqtt.Client {
	p, err := POS(cfg)
	if err != nil {
		panic(fmt.Errorf("mqttx MustPOS failure: %w", err))
	}
	return p
}

func PPC(path string) (mqtt.Client, error) {
	cfg, _, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("mqttx.PPC: %w", err)
	}
	cfg.Mode = "cluster"
	client, _, err := getOrCreateClient(cfg)
	return client, err
}

func POC(cfg mqx.Config) (mqtt.Client, error) {
	cfg.Mode = "cluster"
	client, _, err := getOrCreateClient(&cfg)
	return client, err
}

func MustPPC(path string) mqtt.Client {
	p, err := PPC(path)
	if err != nil {
		panic(fmt.Errorf("mqttx MustPPC failure: %w", err))
	}
	return p
}

func MustPOC(cfg mqx.Config) mqtt.Client {
	p, err := POC(cfg)
	if err != nil {
		panic(fmt.Errorf("mqttx MustPOC failure: %w", err))
	}
	return p
}
