package main

import (
	"time"

	"github.com/src-d/borges"
	core "gopkg.in/src-d/core-retrieval.v0"
)

const (
	updaterCmdName      = "updater"
	updaterCmdShortDesc = "creates new jobs to update fetched repos"
	updaterCmdLongDesc  = ""
)

type updaterCmd struct {
	cmd
	Time    float32 `long:"time" default:"5" description:"time in minutes between each update, it can be fractional (0.5 = 30 seconds)"`
	MaxJobs uint    `long:"max" default:"200" description:"maximum number of jobs sent per update"`
	OldJobs float32 `long:"old" default:"30" description:"time in days when a repository update is considered old"`
}

func (c *updaterCmd) Execute(args []string) error {
	c.init()

	b := core.Broker()
	defer b.Close()
	q, err := b.Queue(c.Queue)
	if err != nil {

	}

	t := time.Duration(c.Time * float32(time.Minute))
	o := time.Duration(c.OldJobs * float32(time.Hour*24))

	db := core.Database()
	updater := borges.NewUpdater(log, db, q, t, c.MaxJobs, o)
	updater.Start()

	return nil
}
