package cli

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // load postgresql driver
	"github.com/src-d/borges/metrics"
	log "gopkg.in/src-d/go-log.v1"
)

// DatabaseOpts holds cli configuration for the database connection.
type DatabaseOpts struct {
	Database string `long:"database" env:"BORGES_DATABASE" default:"postgres://testing:testing@0.0.0.0:5432/testing?application_name=borges&sslmode=disable&connect_timeout=30" description:"database connection string"`
}

// OpenDatabase creates a database connection with the provided configuration.
func (c *DatabaseOpts) OpenDatabase() (*sql.DB, error) {
	return sql.Open("postgres", c.Database)
}

// QueueOpts holds cli configuration for the queue.
type QueueOpts struct {
	Queue  string `long:"queue" env:"BORGES_QUEUE" default:"borges" description:"queue name"`
	Broker string `long:"broker" env:"BORGES_BROKER" default:"amqp://localhost:5672" description:"broker service URI"`
}

// MetricsOps holds cli configuration to expose metrics.
type MetricsOpts struct {
	Metrics     bool `long:"metrics" env:"BORGES_METRICS" description:"expose a metrics endpoint using an HTTP server"`
	MetricsPort int  `long:"metrics-port" env:"BORGES_METRICS_PORT" description:"port to bind metrics to" default:"6062"`
}

// MaybeStartMetrics starts the metrics server if configured.
func (c *MetricsOpts) MaybeStartMetrics() {
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
