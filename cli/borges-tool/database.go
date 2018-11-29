package main

import (
	"fmt"

	bcli "github.com/src-d/borges/cli"
	"github.com/src-d/borges/tool"

	"gopkg.in/src-d/go-cli.v0"
	_ "gopkg.in/src-d/go-queue.v1/amqp"
)

func init() {
	app.AddCommand(&databaseCmd{})
}

type databaseCmd struct {
	cli.Command `name:"database" short-description:"retrieve database data"`
	bcli.DatabaseOpts

	db *tool.Database
}

func (d *databaseCmd) init() error {
	db, err := d.OpenDatabase()
	if err != nil {
		return err
	}

	d.db = tool.NewDatabase(db)

	return nil
}

func (d *databaseCmd) Execute(args []string) error {
	err := d.init()
	if err != nil {
		return err
	}

	list, err := d.db.Siva()
	if err != nil {
		return err
	}

	for _, s := range list {
		fmt.Println(s)
	}

	return nil
}
