package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/src-d/borges"
	"github.com/src-d/borges/lock"
	"github.com/src-d/borges/storage"

	"gopkg.in/src-d/go-cli.v0"
	"gopkg.in/src-d/go-queue.v1/memory"
)

func init() {
	app.AddCommand(&packerCmd{})
}

type packerCmd struct {
	cli.Command `name:"pack" short-description:"pack remote or local repositories into siva files" long-description:""`
	consumerOpts
	PositionalArgs struct {
		File string `positional-arg-name:"path" description:"file with repositories to pack, one per line"`
	} `positional-args:"true" required:"1"`
}

func (c *packerCmd) Execute(args []string) error {
	tmp, err := c.newTemporaryFilesystem()
	if err != nil {
		return err
	}

	locking, err := lock.New(c.Locking)
	if err != nil {
		return err
	}

	broker := memory.New()
	q, err := broker.Queue("jobs")
	if err != nil {
		return fmt.Errorf("unable to start an in-memory queue: %s", err)
	}

	store := storage.Local()
	timeout, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return fmt.Errorf("invalid format in the given `--timeout` flag: %s", err)
	}

	transactioner, err := c.newRootedTransactioner(tmp)
	if err != nil {
		return fmt.Errorf("unable to initialize rooted transactioner: %s", err)
	}

	wp := borges.NewArchiverWorkerPool(
		store,
		transactioner,
		borges.NewTemporaryCloner(tmp),
		locking,
		timeout,
	)

	if c.Workers <= 0 {
		c.Workers = runtime.NumCPU()
	}
	wp.SetWorkerCount(c.Workers)

	f, err := os.Open(c.PositionalArgs.File)
	if err != nil {
		return fmt.Errorf("unable to open file %q with repositories: %s",
			c.PositionalArgs.File, err)
	}

	executor := borges.NewExecutor(
		q,
		wp,
		store,
		borges.NewLineJobIter(f, store),
	)

	return executor.Execute()
}
