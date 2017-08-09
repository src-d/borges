package main

import (
	"fmt"

	"github.com/inconshreveable/log15"

	"gopkg.in/src-d/core-retrieval.v0/schema"
	"gopkg.in/src-d/framework.v0/database"
)

const (
	initCmdName      = "init"
	initCmdShortDesc = "initialize the database schema"
	initCmdLongDesc  = ""
)

type initCmd struct {
	loggerCmd
}

func (c *initCmd) Execute(args []string) error {
	c.ChangeLogLevel()

	db, err := database.Default()
	if err != nil {
		return fmt.Errorf("unable to get database: %s", err)
	}

	if err := schema.Create(db); err != nil {
		return fmt.Errorf("unable to create database schema: %s", err)
	}

	log15.Info("database was successfully initialized")
	return nil
}
