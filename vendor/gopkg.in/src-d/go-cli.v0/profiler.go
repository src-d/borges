package cli

import (
	"net"
	"net/http"
	"runtime"

	"gopkg.in/src-d/go-log.v1"
)

// ProfilerOptions defines profiling flags. It is meant to be embedded in a
// command struct.
type ProfilerOptions struct {
	ProfilerHTTP          bool   `long:"profiler-http" env:"PROFILER_HTTP" description:"start HTTP profiler endpoint"`
	ProfilerBlockRate     int    `long:"profiler-block-rate" env:"PROFILER_BLOCK_RATE" default:"0" description:"runtime.SetBlockProfileRate parameter"`
	ProfilerMutexFraction int    `long:"profiler-mutex-rate" env:"PROFILER_MUTEX_FRACTION" default:"0" description:"runtime.SetMutexProfileFraction parameter"`
	ProfilerEndpoint      string `long:"profiler-endpoint" env:"PROFILER_endpoint" description:"address to bind HTTP pprof endpoint to" default:"0.0.0.0:6061"`
}

// Init initializes the profiler.
func (c ProfilerOptions) init(a *App) error {
	runtime.SetBlockProfileRate(c.ProfilerBlockRate)
	runtime.SetMutexProfileFraction(c.ProfilerMutexFraction)

	if c.ProfilerHTTP {
		log.With(log.Fields{"address": c.ProfilerEndpoint}).
			Debugf("starting http pprof endpoint")
		registerPprof(a.DebugServeMux)
		lis, err := net.Listen("tcp", c.ProfilerEndpoint)
		if err != nil {
			return err
		}

		go func() {
			err := http.Serve(lis, a.DebugServeMux)
			if err != nil {
				log.Errorf(err, "failed to serve http pprof endpoint")
			}
		}()
	}

	return nil
}
