package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"kafka_export_import/cmd/flag"
	"kafka_export_import/internal/pkg/config"
	"kafka_export_import/internal/pkg/importer"
	"kafka_export_import/internal/pkg/logger"

	"github.com/urfave/cli"
	"github.com/zeromicro/go-zero/core/logx"
)

var ImportCmd = cli.Command{
	Name:  "import",
	Usage: "import to kafka",
	Flags: []cli.Flag{
		flag.ConfigFlag,
	},
	Action: importAction,
}

type ImportConfig struct {
	importer.ImportConfig `yaml:",inline"`
	LogConf               logger.LogConfig `json:"logger" yaml:"logger"` // 日志配置
}

func importAction(c *cli.Context) error {
	confFile := c.String(flag.ConfigFlag.Name)

	// 读取配置文件
	var conf ImportConfig
	err := config.Load(confFile, &conf)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// 初始化 zap 日志
	logger.InitLogger(conf.LogConf)
	logx.SetWriter(logger.ZapWriter{})

	imp, err := importer.New(&conf.ImportConfig)
	if err != nil {
		return fmt.Errorf("importer.New failed: %w", err)
	}
	// 启动，这里为非阻塞
	err = imp.Start()
	if err != nil {
		return fmt.Errorf("imp.Start failed: %w", err)
	}

	// 等待退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down services...")
	imp.Stop()
	return nil
}
