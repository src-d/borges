package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/src-d/borges"
	"github.com/src-d/borges/lock"
	"github.com/src-d/borges/storage"

	"gopkg.in/src-d/go-queue.v1/memory"
)

const (
	packerCmdName      = "pack"
	packerCmdShortDesc = "quickly pack remote or local repositories into siva files"
	packerCmdLongDesc  = ""
)

var packerCommand = &packerCmd{consumerSubcmd: newConsumerSubcmd(
	packerCmdName,
	packerCmdShortDesc,
	packerCmdLongDesc,
)}

type packerCmd struct {
	consumerSubcmd

	filePositionalArgs `positional-args:"true" required:"1"`
}

func (c *packerCmd) Execute(args []string) error {
	c.init()

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

	f, err := os.Open(c.File)
	if err != nil {
		return fmt.Errorf("unable to open file %q with repositories: %s", c.File, err)
	}

	executor := borges.NewExecutor(
		q,
		wp,
		store,
		borges.NewLineJobIter(f, store),
	)

	return executor.Execute()
}

func init() {
	_, err := parser.AddCommand(
		packerCommand.Name(),
		packerCommand.ShortDescription(),
		packerCommand.LongDescription(),
		packerCommand)

	if err != nil {
		panic(err)
	}
}
