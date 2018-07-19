package cli

import (
	"fmt"
	"net/http"
	"os"
	"reflect"

	"github.com/jessevdk/go-flags"
)

// App defines the CLI application that will be run.
type App struct {
	Parser *flags.Parser

	// DebugServeMux is serves debug endpoints. It used to attach the http/pprof
	// endpoint if enabled, and can be used to handle other debug endpoints.
	DebugServeMux *http.ServeMux
}

// New creates a new App, including default values.
func New(name, version, build, description string) *App {
	parser := flags.NewNamedParser(name, flags.Default)
	parser.LongDescription = description
	app := &App{
		Parser:        parser,
		DebugServeMux: http.NewServeMux(),
	}

	app.Parser.CommandHandler = app.commandHandler

	app.AddCommand(&VersionCommand{
		Name:    name,
		Version: version,
		Build:   build,
	})

	return app
}

// Run runs the app with the given command line arguments. In order to reduce
// boilerplate, RunMain should be used instead.
func (a *App) Run(args []string) error {
	if _, err := a.Parser.ParseArgs(args[1:]); err != nil {
		if err, ok := err.(*flags.Error); ok {
			if err.Type == flags.ErrHelp {
				return nil
			}

			a.Parser.WriteHelp(os.Stderr)
		}

		return err
	}

	return nil
}

// RunMain runs the application with os.Args and if there is any error, it
// exits with error code 1.
func (a *App) RunMain() {
	if err := a.Run(os.Args); err != nil {
		os.Exit(1)
	}
}

func (a *App) commandHandler(cmd flags.Commander, args []string) error {
	if v, ok := cmd.(initializer); ok {
		if err := v.init(a); err != nil {
			return err
		}
	}

	if v, ok := cmd.(ContextCommander); ok {
		ctx, cancel := setupContext()
		defer cancel()
		return v.ExecuteContext(ctx, args)
	}

	return cmd.Execute(args)
}

func getStructType(data interface{}) (reflect.Type, error) {
	typ := reflect.TypeOf(data)
	if typ == nil {
		return nil, fmt.Errorf("expected struct or struct ptr: got nil")
	}

	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct or struct ptr: %s", typ.Kind())
	}

	return typ, nil
}

type initializer interface {
	init(*App) error
}

// PlainCommand should be embedded in a struct to indicate that it implements a
// command. See package documentation for its usage.
type PlainCommand struct{}

func (c PlainCommand) Execute(args []string) error {
	return nil
}

// Command implements the default group flags. It is meant to be embedded into
// other application commands to provide default behavior for logging,
// profiling, etc.
type Command struct {
	PlainCommand
	LogOptions      `group:"Log Options"`
	ProfilerOptions `group:"Profiler Options"`
}

// Init initializes the command.
func (c Command) init(a *App) error {
	if err := c.LogOptions.init(a); err != nil {
		return err
	}

	if err := c.ProfilerOptions.init(a); err != nil {
		return err
	}

	return nil
}
