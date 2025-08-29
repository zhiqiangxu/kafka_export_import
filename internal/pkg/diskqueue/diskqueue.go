package diskqueue

import (
	"path/filepath"
	"time"

	"github.com/nsqio/go-diskqueue"
)

func New(dataPath, topic string) diskqueue.Interface {
	dqPath := filepath.Join(dataPath, topic)
	return diskqueue.New(
		topic,
		dqPath,
		1<<30, // max bytes per file
		1,     // min msg size
		1<<24, // max msg size
		2500,  // sync every N writes
		time.Second*2,
		func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
			// fmt.Printf(f, args...)
		},
	)
}
