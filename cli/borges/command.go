package main

import (
	"database/sql"

	_ "github.com/lib/pq"
)

type databaseOpts struct {
	Database string `long:"database" env:"BORGES_DATABASE" default:"postgres://testing:testing@0.0.0.0:5432/testing?application_name=borges&sslmode=disable&connect_timeout=30" description:"database connection string"`
}

func (c *databaseOpts) openDatabase() (*sql.DB, error) {
	return sql.Open("postgres", c.Database)
}

type queueOpts struct {
	Queue  string `long:"queue" env:"BORGES_QUEUE" default:"borges" description:"queue name"`
	Broker string `long:"broker" env:"BORGES_BROKER" default:"amqp://localhost:5672" description:"broker service URI"`
}
