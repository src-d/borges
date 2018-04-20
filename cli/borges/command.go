package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/inconshreveable/log15"
	"github.com/src-d/borges/metrics"
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
	LogLevel  string `short:"" long:"loglevel" description:"max log level enabled" default:"info"`
	LogFile   string `short:"" long:"logfile" description:"path to file where logs will be stored" default:""`
	LogFormat string `short:"" long:"logformat" description:"format used to output the logs (json or text)" default:"text"`
}

func (c *loggerOpts) init() {
	lvl, err := log15.LvlFromString(c.LogLevel)
	if err != nil {
		panic(fmt.Sprintf("unknown level name %q", c.LogLevel))
	}

	var handlers []log15.Handler
	var format log15.Format
	if c.LogFormat == "json" {
		format = log15.JsonFormat()
		handlers = append(
			handlers,
			log15.CallerFileHandler(log15.Must.FileHandler(os.Stdout.Name(), format)),
		)
	} else {
		format = log15.LogfmtFormat()
		handlers = append(
			handlers,
			log15.CallerFileHandler(log15.StdoutHandler),
		)
	}

	if c.LogFile != "" {
		handlers = append(
			handlers,
			log15.CallerFileHandler(log15.Must.FileHandler(c.LogFile, format)),
		)
	}

	log15.Root().SetHandler(log15.LvlFilterHandler(lvl, log15.MultiHandler(handlers...)))
}

type metricsOpts struct {
	Metrics     bool `long:"metrics" description:"expose a metrics endpoint using an HTTP server"`
	MetricsPort int  `long:"metrics-port" description:"port to bind metrics to" default:"6062"`
}

func (c *metricsOpts) maybeStartMetrics() {
	if c.Metrics {
		addr := fmt.Sprintf("0.0.0.0:%d", c.MetricsPort)
		go func() {
			log.Debug("Started metrics service at", "address", addr)
			if err := metrics.Start(addr); err != nil {
				log.Warn("metrics service stopped", "err", err)
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
			log.Debug("Started CPU, memory and block profilers at", "address", addr)
			err := http.ListenAndServe(addr, nil)
			if err != nil {
				log.Warn("Profiler failed to listen and serve at", "address", addr, "error", err)
			}
		}()
	}
}
