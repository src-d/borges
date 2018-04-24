package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"github.com/src-d/borges/metrics"
	"gopkg.in/src-d/go-log.v0"
)

type ExecutableCommand interface {
	Command
	Execute(args []string) error
}

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
	loggerOpts
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
	c.loggerOpts.init()
	c.profilerOpts.maybeStartProfiler()
	c.metricsOpts.maybeStartMetrics()
}

type queueOpts struct {
	Queue string `long:"queue" default:"borges" description:"queue name"`
}

type loggerOpts struct {
	LogLevel  string `short:"" long:"loglevel" description:"max log level enabled (debug, info, warning, error)" default:"info"`
	LogFormat string `short:"" long:"logformat" description:"format used to output the logs (json or text)" default:"text"`
}

func (c *loggerOpts) init() {
	loggerFactory.Level = c.LogLevel
	loggerFactory.Format = c.LogFormat
}

type metricsOpts struct {
	Metrics     bool `long:"metrics" description:"expose a metrics endpoint using an HTTP server"`
	MetricsPort int  `long:"metrics-port" description:"port to bind metrics to" default:"6062"`
}

func (c *metricsOpts) maybeStartMetrics() {
	if c.Metrics {
		addr := fmt.Sprintf("0.0.0.0:%d", c.MetricsPort)
		go func() {
			log.Debugf("Started metrics service at", "address", addr)
			if err := metrics.Start(addr); err != nil {
				log.Warningf("metrics service stopped", "err", err)
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
			l, _ := log.New()
			l.New(log.Fields{"address": addr}).Debugf("Started CPU, memory and block profilers at")
			err := http.ListenAndServe(addr, nil)
			if err != nil {
				l.New(log.Fields{
					"address": addr,
					"error":   err,
				}).Warningf("Profiler failed to listen and serve at")
			}
		}()
	}
}
