package utils

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger 结构化日志器
type Logger struct {
	*zap.Logger
	driver string
}

var (
	globalLogger     *Logger
	globalLoggerOnce sync.Once
)

// LogConfig 日志配置
type LogConfig struct {
	Level      string `json:"level" yaml:"level"`                   // debug, info, warn, error
	Format     string `json:"format" yaml:"format"`                 // json, console
	OutputPath string `json:"output_path" yaml:"output_path"`       // 输出路径，stdout 表示标准输出
	Driver     string `json:"driver" yaml:"driver"`                 // 驱动名称，用于上下文注入
}

// BuildLogConfig 从 MQX 配置构建日志配置
func BuildLogConfig(cfg *LogConfig) (*Logger, error) {
	if cfg == nil {
		cfg = &LogConfig{
			Level:      "info",
			Format:     "console",
			OutputPath: "stdout",
		}
	}

	// 解析日志级别
	var level zapcore.Level
	switch cfg.Level {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn", "warning":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	default:
		level = zapcore.InfoLevel
	}

	// 解析编码器
	var encoderConfig zapcore.EncoderConfig
	if cfg.Format == "json" {
		encoderConfig = zap.NewProductionEncoderConfig()
	} else {
		encoderConfig = zap.NewDevelopmentEncoderConfig()
	}
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.LevelKey = "level"
	encoderConfig.MessageKey = "message"
	encoderConfig.CallerKey = "caller"

	var encoder zapcore.Encoder
	if cfg.Format == "json" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// 配置输出
	var writeSyncer zapcore.WriteSyncer
	if cfg.OutputPath == "" || cfg.OutputPath == "stdout" {
		writeSyncer = zapcore.AddSync(os.Stdout)
	} else {
		file, err := os.OpenFile(cfg.OutputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		writeSyncer = zapcore.AddSync(file)
	}

	// 创建核心
	core := zapcore.NewCore(encoder, writeSyncer, level)

	// 创建 logger
	zapLogger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))

	logger := &Logger{
		Logger: zapLogger,
		driver: cfg.Driver,
	}

	return logger, nil
}

// GetLogger 获取全局日志器
func GetLogger() *Logger {
	globalLoggerOnce.Do(func() {
		globalLogger, _ = BuildLogConfig(nil)
	})
	return globalLogger
}

// SetLogger 设置全局日志器
func SetLogger(logger *Logger) {
	globalLogger = logger
}

// WithDriver 创建带驱动上下文的日志器
func (l *Logger) WithDriver(driver string) *Logger {
	return &Logger{
		Logger: l.Logger.With(zap.String("driver", driver)),
		driver: driver,
	}
}

// Debug 打印调试日志
func (l *Logger) Debug(msg string, fields ...zap.Field) {
	l.Logger.Debug(msg, fields...)
}

// Info 打印信息日志
func (l *Logger) Info(msg string, fields ...zap.Field) {
	l.Logger.Info(msg, fields...)
}

// Warn 打印警告日志
func (l *Logger) Warn(msg string, fields ...zap.Field) {
	l.Logger.Warn(msg, fields...)
}

// Error 打印错误日志
func (l *Logger) Error(msg string, fields ...zap.Field) {
	l.Logger.Error(msg, fields...)
}

// Sync 同步日志到磁盘
func (l *Logger) Sync() error {
	return l.Logger.Sync()
}
