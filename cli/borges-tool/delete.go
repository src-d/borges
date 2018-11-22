package main

import (
	"context"
	"database/sql"
	"os"
	"runtime"
	"sort"

	bcli "github.com/src-d/borges/cli"
	"github.com/src-d/borges/tool"

	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-cli.v0"
	_ "gopkg.in/src-d/go-queue.v1/amqp"
)

func init() {
	app.AddCommand(&deleteCmd{})
}

type deleteCmd struct {
	cli.Command `name:"delete" short-description:"delete siva files and references"`
	bcli.DatabaseOpts

	fs   billy.Basic
	db   *tool.Database
	siva *tool.Siva
	list []string

	FSString   string `long:"fs" description:"filesystem connection string, ex: file:///mnt/rooted-repos, gluster://host/volume/rooted-repos"`
	Bucket     int    `long:"bucket" description:"bucket level"`
	Dry        bool   `long:"dry" description:"do not perform modifications in database or filesystem"`
	SkipErrors bool   `long:"skip-errors" description:"do not stop on errors"`
	Workers    int    `long:"workers" description:"specify the number of threads to use, 0 means all cores" default:"1"`

	deleteArgs `positional-args:"true" required:"yes"`
}

type deleteArgs struct {
	SivaList string `positional-arg-name:"list" description:"file with the list of sivas to change bucketing" required:"yes"`
}

func (d *deleteCmd) init() error {
	var err error

	if d.Database != "" {
		var db *sql.DB
		db, err = d.OpenDatabase()
		if err != nil {
			return err
		}
		d.db = tool.NewDatabase(db)
	}

	if d.FSString != "" {
		d.fs, err = tool.OpenFS(d.FSString)
		if err != nil {
			return err
		}
	}

	if d.Workers == 0 {
		d.Workers = runtime.NumCPU()
	}

	s := tool.NewSiva(d.db, d.fs)
	s.Bucket(d.Bucket)
	s.Dry(d.Dry)
	s.Workers(d.Workers)
	s.WriteQueue(os.Stdout)
	s.DefaultErrors("error deleting siva", d.SkipErrors)
	d.siva = s

	d.list, err = tool.LoadHashes(d.SivaList)
	return err
}

func (d *deleteCmd) Execute(args []string) error {
	err := d.init()
	if err != nil {
		return err
	}

	sort.Strings(d.list)

	err = d.siva.Delete(context.Background(), d.list)
	if err != nil {
		return err
	}

	return nil
}
