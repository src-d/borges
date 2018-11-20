package main

import (
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
	list        []string

	Dry bool `long:"dry" description:"do not perform modifications in database or filesystem"`

	rebucketArgs `positional-args:"true" required:"yes"`
}

type rebucketArgs struct {
	FSString string `positional-arg-name:"fs" description:"filesystem connection string, ex: file:///mnt/rooted-repos, gluster://host/volume/rooted-repos" required:"yes"`
	From     int    `positional-arg-name:"from" description:"original bucket level" required:"yes"`
	To       int    `positional-arg-name:"to" description:"new bucket level" required:"yes"`
	SivaList string `positional-arg-name:"list" description:"file with the list of sivas to change bucketing" required:"yes"`
}

func (d *rebucketCmd) init() error {
	var err error
	d.fs, err = tool.OpenFS(d.FSString)
	if err != nil {
		return err
	}

	d.list, err = tool.LoadHashes(d.SivaList)
	return err
}

func (d *rebucketCmd) Execute(args []string) error {
	err := d.init()
	if err != nil {
		return err
	}

	err = tool.Rebucket(d.fs, d.list, d.From, d.To, d.Dry)
	if err != nil {
		return err
	}

	return nil
}
