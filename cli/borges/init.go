package main

import (
	"fmt"

	"gopkg.in/src-d/core-retrieval.v0/schema"
	"gopkg.in/src-d/framework.v0/database"
)

const (
	initCmdName      = "init"
	initCmdShortDesc = "initialize the database schema"
	initCmdLongDesc  = ""
)

var initCommand = &initCmd{simpleCommand: newSimpleCommand(
	initCmdName,
	initCmdShortDesc,
	initCmdLongDesc,
)}

type initCmd struct {
	simpleCommand
	loggerOpts
}

func (c *initCmd) Execute(args []string) error {
	c.init()

	db, err := database.Default()
	if err != nil {
		return fmt.Errorf("unable to get database: %s", err)
	}

	if err := schema.Create(db); err != nil {
		return fmt.Errorf("unable to create database schema: %s", err)
	}

	log.WithField("command", initCmdName).Info("database was successfully initialized")
	return nil
}

func init() {
	_, err := parser.AddCommand(
		initCommand.Name(),
		initCommand.ShortDescription(),
		initCommand.LongDescription(),
		initCommand)

	if err != nil {
		panic(err)
	}
}
