package main

import (
	"context"
	"database/sql"
	"runtime"

	bcli "github.com/src-d/borges/cli"
	"github.com/src-d/borges/tool"

	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-cli.v0"
	queue "gopkg.in/src-d/go-queue.v1"
	_ "gopkg.in/src-d/go-queue.v1/amqp"
)

func init() {
	app.AddCommand(&queueCmd{})
}

type queueCmd struct {
	cli.Command `name:"queue" short-description:"queue repositories for download"`
	bcli.DatabaseOpts
	bcli.QueueOpts

	fs   billy.Basic
	db   *tool.Database
	r    *tool.Repository
	q    queue.Queue
	list []string

	Dry        bool `long:"dry" description:"do not perform modifications in database or filesystem"`
	SkipErrors bool `long:"skip-errors" description:"do not stop on errors"`
	Workers    int  `long:"workers" description:"specify the number of threads to use, 0 means all cores" default:"1"`

	queueArgs `positional-args:"true" required:"yes"`
}

type queueArgs struct {
	RepoList string `positional-arg-name:"list" description:"file with the list of repository ids" required:"yes"`
}

func (d *queueCmd) init() error {
	var err error

	if d.Database != "" {
		var db *sql.DB
		db, err = d.OpenDatabase()
		if err != nil {
			return err
		}
		d.db = tool.NewDatabase(db)
	}

	if d.Workers == 0 {
		d.Workers = runtime.NumCPU()
	}

	r := tool.NewRepository(d.db, d.q)
	r.Dry(d.Dry)
	r.Workers(d.Workers)
	r.DefaultErrors("error deleting siva", d.SkipErrors)
	d.r = r

	d.list, err = tool.LoadList(d.RepoList)
	return err
}

func (d *queueCmd) Execute(args []string) error {
	err := d.init()
	if err != nil {
		return err
	}

	err = d.r.Queue(context.Background(), d.list)
	if err != nil {
		return err
	}

	return nil
}
