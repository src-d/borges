// Package gfapi provides a wrapper around gfapi, the GlusterFS api, which is
// used to access files/directories on a Gluster volume.
// The design tries to follow the default go file handling functions provided
// by the os package as much as possible.
//
// To use gfapi, a virtual volume must be initialized and mounted first.
// 	vol := &gfapi.Volume{}
//	e := vol.Init("hostname", "volume")
//	e := vol.Mount()
//
// Once the virtual volume is mounted, the vol object can be used like the os package to perform file operations.
//	f, e := vol.Create("testfile")
//	defer f.Close()
//	e := vol.Unlink("somefile")
//
// The gfapi.File implements the same interfaces as os.File, and can be used wherever os.File is used.
// XXX: Acutally verify this.
package gfapi //import "github.com/gluster/gogfapi/gfapi"
