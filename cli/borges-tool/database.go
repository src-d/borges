package main

import (
	"database/sql"
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

	database *sql.DB
}

func (d *databaseCmd) init() error {
	var err error
	d.database, err = d.OpenDatabase()
	if err != nil {
		return err
	}

	return nil
}

func (d *databaseCmd) Execute(args []string) error {
	err := d.init()
	if err != nil {
		return err
	}

	list, err := tool.DatabaseSiva(d.database)
	if err != nil {
		return err
	}

	for _, s := range list {
		fmt.Println(s)
	}

	return nil
}
