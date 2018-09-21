package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/src-d/borges/metrics"
	log "gopkg.in/src-d/go-log.v1"
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

type metricsOpts struct {
	Metrics     bool `long:"metrics" env:"BORGES_METRICS" description:"expose a metrics endpoint using an HTTP server"`
	MetricsPort int  `long:"metrics-port" env:"BORGES_METRICS_PORT" description:"port to bind metrics to" default:"6062"`
}

func (c *metricsOpts) maybeStartMetrics() {
	if c.Metrics {
		addr := fmt.Sprintf("0.0.0.0:%d", c.MetricsPort)
		go func() {
			logger := log.New(log.Fields{"address": addr})
			logger.Debugf("started metrics service")
			if err := metrics.Start(addr); err != nil {
				logger.With(log.Fields{
					"error": err,
				}).Warningf("metrics service stopped")
			}
		}()
	}
}
