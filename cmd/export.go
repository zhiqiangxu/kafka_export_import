package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"kafka_export_import/cmd/flag"
	"kafka_export_import/internal/pkg/config"
	"kafka_export_import/internal/pkg/exporter"
	"kafka_export_import/internal/pkg/logger"

	"github.com/urfave/cli"
	"github.com/zeromicro/go-zero/core/logx"
)

var ExportCmd = cli.Command{
	Name:  "export",
	Usage: "export from kafka",
	Flags: []cli.Flag{
		flag.ConfigFlag,
	},
	Action: exportAction,
}

type ExportConfig struct {
	exporter.ExporterConfig `yaml:",inline"`
	LogConf                 logger.LogConfig `json:"logger" yaml:"logger"` // 日志配置
}

func exportAction(c *cli.Context) error {
	confFile := c.String(flag.ConfigFlag.Name)

	// 读取配置文件
	var conf ExportConfig
	err := config.Load(confFile, &conf)
	if err != nil {
		return fmt.Errorf("config.Load failed: %w", err)
	}

	// 初始化 zap 日志
	logger.InitLogger(conf.LogConf)
	logx.SetWriter(logger.ZapWriter{})

	confBytes, _ := json.Marshal(conf)
	logger.Infof("config:%v", string(confBytes))

	exp, err := exporter.New(&conf.ExporterConfig)
	if err != nil {
		return fmt.Errorf("exporter.New failed: %w", err)
	}
	// 启动，这里为非阻塞
	err = exp.Start()
	if err != nil {
		return fmt.Errorf("exp.Start failed: %w", err)
	}

	// 等待退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down services...")
	exp.Stop()
	return nil
}
