package flag

import "github.com/urfave/cli"

var ConfigFlag = cli.StringFlag{
	Name:     "config",
	Usage:    "specify config file",
	Required: true,
}

var DataFlag = cli.StringFlag{
	Name:     "data",
	Usage:    "specify data dir",
	Required: true,
}
