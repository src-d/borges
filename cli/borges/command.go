package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"github.com/src-d/borges/metrics"
	"gopkg.in/src-d/go-log.v1"
)

// ExecutableCommand holds a command and executable function.
type ExecutableCommand interface {
	Command
	Execute(args []string) error
}

// Command contains the information about a command.
type Command interface {
	Name() string
	ShortDescription() string
	LongDescription() string
}

type simpleCommand struct {
	name             string
	shortDescription string
	longDescription  string
}

func newSimpleCommand(name, short, long string) simpleCommand {
	return simpleCommand{
		name:             name,
		shortDescription: short,
		longDescription:  long,
	}
}

func (c *simpleCommand) Name() string { return c.name }

func (c *simpleCommand) ShortDescription() string { return c.shortDescription }

func (c *simpleCommand) LongDescription() string { return c.longDescription }

type command struct {
	simpleCommand
	queueOpts
	metricsOpts
	profilerOpts
}

func newCommand(name, short, long string) command {
	return command{
		simpleCommand: newSimpleCommand(
			name,
			short,
			long,
		),
	}
}

func (c *command) init() {
	c.profilerOpts.maybeStartProfiler()
	c.metricsOpts.maybeStartMetrics()
}

type queueOpts struct {
	Queue  string `long:"queue" default:"borges" description:"queue name"`
	Broker string `long:"broker" env:"CONFIG_BROKER" default:"amqp://localhost:5672" description:"broker URL service"`
}

type metricsOpts struct {
	Metrics     bool `long:"metrics" description:"expose a metrics endpoint using an HTTP server"`
	MetricsPort int  `long:"metrics-port" description:"port to bind metrics to" default:"6062"`
}

func (c *metricsOpts) maybeStartMetrics() {
	if c.Metrics {
		addr := fmt.Sprintf("0.0.0.0:%d", c.MetricsPort)
		go func() {
			logger := log.New(log.Fields{"address": addr})
			logger.Debugf("Started metrics service")
			if err := metrics.Start(addr); err != nil {
				logger.With(log.Fields{
					"error": err,
				}).Warningf("metrics service stopped")
			}
		}()
	}
}

type profilerOpts struct {
	Profiler     bool `long:"profiler" description:"start CPU, memory and block profilers"`
	ProfilerPort int  `long:"profiler-port" description:"port to bind profiler to" default:"6061"`
}

func (c *profilerOpts) maybeStartProfiler() {
	if c.Profiler {
		addr := fmt.Sprintf("0.0.0.0:%d", c.ProfilerPort)
		go func() {
			logger := log.New(log.Fields{"address": addr})
			logger.Debugf("Started CPU, memory and block profilers")
			err := http.ListenAndServe(addr, nil)
			if err != nil {
				logger.With(log.Fields{
					"error": err,
				}).Warningf("profiler failed to listen and serve")
			}
		}()
	}
}
