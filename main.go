package main

import (
	"os"
	"runtime"
	"runtime/debug"

	"kafka_export_import/cmd"
	"kafka_export_import/internal/pkg/logger"

	"github.com/urfave/cli"
)

func setupAPP() *cli.App {
	app := cli.NewApp()
	app.Copyright = "Copyright in 2025"
	app.Commands = []cli.Command{
		cmd.ExportCmd,
		cmd.ImportCmd,
		cmd.CheckCmd,
	}
	app.Flags = []cli.Flag{}
	app.Before = func(context *cli.Context) error {
		runtime.GOMAXPROCS(runtime.NumCPU())
		return nil
	}
	return app
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic: %+v\nstack: %s", r, debug.Stack())
		}
	}()
	defer logger.Sync()

	if err := setupAPP().Run(os.Args); err != nil {
		logger.Errorf("Run failed:%v", err)
		logger.Sync()
		os.Exit(1)
	}
}
