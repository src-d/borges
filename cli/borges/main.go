package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/onrik/logrus/filename"
	log "github.com/sirupsen/logrus"
	"github.com/src-d/borges/metrics"
)

const (
	name string = "borges"
	desc string = "Fetches, organizes and stores repositories."
)

var (
	version string
	build   string
)

type loggerCmd struct {
	LogLevel      string `short:"" long:"loglevel" description:"max log level enabled (debug, info, warn, error, fatal, panic)" default:"info"`
	LogFile       string `short:"" long:"logfile" description:"path to file where logs will be stored" default:""`
	LogFormat     string `short:"" long:"logformat" description:"format used to output the logs (json or text)" default:"text"`
	LogTimeFormat string `short:"" long:"logtimeformat" description:"format used for marshaling timestamps" default:"Jan _2 15:04:05.000000"`
}

type cmd struct {
	loggerCmd
	Queue        string `long:"queue" default:"borges" description:"queue name"`
	Metrics      bool   `long:"metrics" description:"expose a metrics endpoint using an HTTP server"`
	MetricsPort  int    `long:"metrics-port" description:"port to bind metrics to" default:"6062"`
	Profiler     bool   `long:"profiler" description:"start CPU, memory and block profilers"`
	ProfilerPort int    `long:"profiler-port" description:"port to bind profiler to" default:"6061"`
}

func (c *loggerCmd) init() {
	log.AddHook(filename.NewHook(
		log.DebugLevel,
		log.InfoLevel,
		log.WarnLevel,
		log.ErrorLevel,
		log.FatalLevel,
		log.PanicLevel),
	)

	switch strings.ToLower(c.LogLevel) {
	case "debug":
		log.SetLevel(log.DebugLevel)
		break
	case "info":
		log.SetLevel(log.InfoLevel)
		break
	case "warn":
		log.SetLevel(log.WarnLevel)
		break
	case "error":
		log.SetLevel(log.ErrorLevel)
		break
	case "fatal":
		log.SetLevel(log.FatalLevel)
		break
	case "panic":
		log.SetLevel(log.PanicLevel)
		break
	default:
		panic(fmt.Sprintf("unknown level name %q", c.LogLevel))
	}

	if c.LogTimeFormat == "" {
		c.LogTimeFormat = time.StampMicro
	}

	switch strings.ToLower(c.LogFormat) {
	case "json":
		log.SetFormatter(&log.JSONFormatter{TimestampFormat: c.LogTimeFormat})
		break
	case "text", "txt":
		log.SetFormatter(&log.TextFormatter{TimestampFormat: c.LogTimeFormat, FullTimestamp: true})
		break
	default:
		panic(fmt.Sprintf("unknown log format %q", c.LogFormat))
	}

	log.SetOutput(os.Stdout)
	if c.LogFile != "" {
		if f, err := os.OpenFile(c.LogFile, os.O_CREATE|os.O_WRONLY, 0666); err != nil {
			log.Errorf("Failed to log to file (%s), using default stdout", c.LogFile)
		} else {
			log.SetOutput(f)
		}
	}
}

func (c *cmd) init() {
	c.loggerCmd.init()
	c.maybeStartProfiler()
	c.maybeStartMetrics()
}

func (c *cmd) maybeStartProfiler() {
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

func (c *cmd) maybeStartMetrics() {
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

func main() {
	parser := flags.NewParser(nil, flags.Default)
	parser.LongDescription = desc

	if _, err := parser.AddCommand(versionCmdName, versionCmdShortDesc,
		versionCmdLongDesc, &versionCmd{}); err != nil {
		panic(err)
	}

	if _, err := parser.AddCommand(consumerCmdName, consumerCmdShortDesc,
		consumerCmdLongDesc, &consumerCmd{}); err != nil {
		panic(err)
	}

	c, err := parser.AddCommand(producerCmdName, producerCmdShortDesc,
		producerCmdLongDesc, &producerCmd{})
	if err != nil {
		panic(err)
	}

	setPrioritySettings(c)

	if _, err := parser.AddCommand(initCmdName, initCmdShortDesc,
		initCmdLongDesc, new(initCmd)); err != nil {
		panic(err)
	}

	if _, err := parser.AddCommand(packerCmdName, packerCmdShortDesc,
		packerCmdLongDesc, new(packerCmd)); err != nil {
		panic(err)
	}

	if _, err := parser.Parse(); err != nil {
		if err, ok := err.(*flags.Error); ok {
			if err.Type == flags.ErrHelp {
				os.Exit(0)
			}

			parser.WriteHelp(os.Stdout)
		}

		os.Exit(1)
	}

}
