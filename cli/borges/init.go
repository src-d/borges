package main

import (
	"fmt"

	bcli "github.com/src-d/borges/cli"
	"gopkg.in/src-d/core-retrieval.v0/schema"
	"gopkg.in/src-d/go-cli.v0"
	"gopkg.in/src-d/go-log.v1"
)

func init() {
	app.AddCommand(&initCmd{})
}

type initCmd struct {
	cli.Command `name:"init" short-description:"initialize the database schema" long-description:"Connects to the database and initializes the schema."`
	bcli.DatabaseOpts
}

func (c *initCmd) Execute(args []string) error {
	db, err := c.OpenDatabase()
	if err != nil {
		return fmt.Errorf("unable to get database: %s", err)
	}

	if err := schema.Create(db); err != nil {
		return fmt.Errorf("unable to create database schema: %s", err)
	}

	log.Infof("database was successfully initialized")
	return nil
}
