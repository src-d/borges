package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/src-d/borges/metrics"

	"github.com/onrik/logrus/filename"
	"github.com/sirupsen/logrus"
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
	LogLevel      string `short:"" long:"loglevel" description:"max log level enabled (debug, info, warn, error, fatal, panic)" default:"info"`
	LogFile       string `short:"" long:"logfile" description:"path to file where logs will be stored" default:""`
	LogFormat     string `short:"" long:"logformat" description:"format used to output the logs (json or text)" default:"text"`
	LogTimeFormat string `short:"" long:"logtimeformat" description:"format used for marshaling timestamps" default:"Jan _2 15:04:05.000000"`
}

func (c *loggerOpts) init() {
	logrus.AddHook(filename.NewHook(
		logrus.DebugLevel,
		logrus.InfoLevel,
		logrus.WarnLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel),
	)

	switch strings.ToLower(c.LogLevel) {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
		break
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
		break
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
		break
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
		break
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
		break
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
		break
	default:
		panic(fmt.Sprintf("unknown level name %q", c.LogLevel))
	}

	if c.LogTimeFormat == "" {
		c.LogTimeFormat = time.StampMicro
	}

	switch strings.ToLower(c.LogFormat) {
	case "json":
		logrus.SetFormatter(&logrus.JSONFormatter{TimestampFormat: c.LogTimeFormat})
		break
	case "text", "txt":
		logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: c.LogTimeFormat, FullTimestamp: true})
		break
	default:
		panic(fmt.Sprintf("unknown log format %q", c.LogFormat))
	}

	logrus.SetOutput(os.Stdout)
	if c.LogFile != "" {
		if f, err := os.OpenFile(c.LogFile, os.O_CREATE|os.O_WRONLY, 0666); err != nil {
			logrus.Errorf("Failed to log to file (%s), using default stdout", c.LogFile)
		} else {
			logrus.SetOutput(f)
		}
	}
}

type metricsOpts struct {
	Metrics     bool `long:"metrics" description:"expose a metrics endpoint using an HTTP server"`
	MetricsPort int  `long:"metrics-port" description:"port to bind metrics to" default:"6062"`
}

func (c *metricsOpts) maybeStartMetrics() {
	if c.Metrics {
		addr := fmt.Sprintf("0.0.0.0:%d", c.MetricsPort)
		go func() {
			logrus.Debug("Started metrics service at", "address", addr)
			if err := metrics.Start(addr); err != nil {
				logrus.Warn("metrics service stopped", "err", err)
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
			logrus.WithField("address", addr).Debug("Started CPU, memory and block profilers at")
			err := http.ListenAndServe(addr, nil)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"address": addr,
					"error":   err,
				}).Warn("Profiler failed to listen and serve at")
			}
		}()
	}
}
