package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/inconshreveable/log15"
	"github.com/jessevdk/go-flags"
	"github.com/src-d/borges/metrics"
)

const (
	name string = "borges"
	desc string = "Fetches, organizes and stores repositories."
)

var (
	version string
	build   string
	log     log15.Logger
)

type loggerCmd struct {
	LogLevel  string `short:"" long:"loglevel" description:"max log level enabled" default:"info"`
	LogFile   string `short:"" long:"logfile" description:"path to file where logs will be stored" default:""`
	LogFormat string `short:"" long:"logformat" description:"format used to output the logs (json or text)" default:"text"`
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

func init() {
	log = log15.New("module", name)
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
