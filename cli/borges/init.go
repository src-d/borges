package main

import (
	"fmt"

	log "github.com/sirupsen/logrus"
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
	c.init()

	db, err := database.Default()
	if err != nil {
		return fmt.Errorf("unable to get database: %s", err)
	}

	if err := schema.Create(db); err != nil {
		return fmt.Errorf("unable to create database schema: %s", err)
	}

	log.Info("database was successfully initialized")
	return nil
}
