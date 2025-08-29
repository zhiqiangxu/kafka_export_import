package cmd

import (
	"fmt"
	"kafka_export_import/cmd/flag"
	"kafka_export_import/internal/pkg/diskqueue"
	"kafka_export_import/internal/pkg/utils"

	"github.com/urfave/cli"
)

var CheckCmd = cli.Command{
	Name:  "check",
	Usage: "check exported data",
	Flags: []cli.Flag{
		flag.DataFlag,
	},
	Action: checkAction,
}

func checkAction(c *cli.Context) error {
	dataPath := c.String(flag.DataFlag.Name)
	subDirs, err := utils.ReadSubDirs(dataPath)
	if err != nil {
		return fmt.Errorf("utils.ReadSubDirs failed with %v", err)
	}

	for _, topic := range subDirs {
		dq := diskqueue.New(dataPath, topic)
		fmt.Printf("%s msg count: %d\n", topic, dq.Depth())
		dq.Close()
	}
	return nil
}
