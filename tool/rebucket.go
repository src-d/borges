package tool

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/src-d/go-billy-gluster.v0"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/helper/chroot"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-log.v1"
)

// OpenFS creates a billy filesystem from a connection string. Only osfs
// and gluster are supported. gluster connection string has this syntax:
//
//     gluster://<host>/<volume>[/<path>]
//
// If path is provided a chroot is returned to that path.
func OpenFS(conn string) (billy.Basic, error) {
	u, err := url.Parse(conn)
	if err != nil {
		return nil, err
	}

	var fs billy.Basic
	switch u.Scheme {
	case "file":
		return osfs.New(u.Path), nil
	case "gluster":
		path := strings.TrimPrefix(u.Path, "/")
		if len(path) == 0 {
			return nil, fmt.Errorf("volume not provided")
		}

		s := strings.Split(path, string(os.PathSeparator))
		volume := s[0]

		fs, err = gluster.New(u.Hostname(), volume)
		if err != nil {
			return nil, err
		}

		if len(s) > 1 {
			path := filepath.Join(s[1:]...)
			fs = chroot.New(fs, path)
		}

		return fs, nil
	}

	return nil, fmt.Errorf("invalid scheme %s", u.Scheme)
}

func bucketPath(name string, sz int) string {
	if sz > len(name) {
		sz = len(name)
	}

	return filepath.Join(name[:sz], name)
}

// Rebucket moves files from one bucketing depth to another. If a siva in the
// list has less characters than the max bucketing level it is skipped. If
// dry is true no actual rename is done.
func Rebucket(fs billy.Basic, list []string, from, to int, dry bool) error {
	if to == from {
		return nil
	}

	max := from
	if to > max {
		max = to
	}

	for _, f := range list {
		if len(f)-1 < max {
			// not enough characters to create bucket directory
			continue
		}

		s := fmt.Sprintf("%s.siva", f)

		a := bucketPath(s, from)
		b := bucketPath(s, to)

		log.Debugf("move %s -> %s\n", a, b)
		if !dry {
			err := fs.Rename(a, b)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
