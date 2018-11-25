package main

import (
	"context"
	"io"
	"os"
	"runtime"
	"sort"

	"github.com/src-d/borges/tool"

	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-cli.v0"
	_ "gopkg.in/src-d/go-queue.v1/amqp"
)

func init() {
	app.AddCommand(&rebucketCmd{})
}

type rebucketCmd struct {
	cli.Command `name:"rebucket" short-description:"change siva bucket level"`
	fs          billy.Basic
	siva        *tool.Siva
	list        []string
	out         io.WriteCloser

	Dry        bool   `long:"dry" description:"do not perform modifications to filesystem"`
	SkipErrors bool   `long:"skip-errors" description:"do not stop on errors"`
	Workers    int    `long:"workers" description:"specify the number of threads to use, 0 means all cores" default:"1"`
	Output     string `long:"output" short:"o" description:"file where to save siva files with error, if not specified the list will be output to stdout"`

	rebucketArgs `positional-args:"true" required:"yes"`
}

type rebucketArgs struct {
	FSString string `positional-arg-name:"fs" description:"filesystem connection string, ex: file:///mnt/rooted-repos, gluster://host/volume/rooted-repos" required:"yes"`
	From     int    `positional-arg-name:"from" description:"original bucket level" required:"yes"`
	To       int    `positional-arg-name:"to" description:"new bucket level" required:"yes"`
	SivaList string `positional-arg-name:"list" description:"file with the list of sivas to change bucketing" required:"yes"`
}

func (r *rebucketCmd) init() error {
	var err error
	r.fs, err = tool.OpenFS(r.FSString)
	if err != nil {
		return err
	}

	if r.Workers == 0 {
		r.Workers = runtime.NumCPU()
	}

	r.out = os.Stdout
	if r.Output != "" {
		r.out, err = os.Create(r.Output)
		if err != nil {
			return err
		}
	}

	s := tool.NewSiva(nil, r.fs)
	s.Bucket(r.From)
	s.Dry(r.Dry)
	s.Workers(r.Workers)
	s.WriteFailed(r.out)
	s.DefaultErrors("error rebucketing siva", r.SkipErrors)
	r.siva = s

	r.list, err = tool.LoadHashes(r.SivaList)
	return err
}

func (d *rebucketCmd) Execute(args []string) error {
	err := d.init()
	if err != nil {
		return err
	}

	sort.Strings(d.list)

	err = d.siva.Rebucket(context.Background(), d.list, d.To)
	if err != nil {
		return err
	}

	return nil
}
