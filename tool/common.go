package tool

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	gluster "gopkg.in/src-d/go-billy-gluster.v0"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/helper/chroot"
	"gopkg.in/src-d/go-billy.v4/osfs"
)

// LoadHashes loads siva hashes from a file and generates a list. The lines
// from the file are parsed so it accepts file lists with bucketing. This list
// is filtered so it does not contain repetitions and is sorted
// n lexicographic order.
func LoadHashes(file string) ([]string, error) {
	return LoadListFilter(file, func(s string) string {
		t := strings.TrimSpace(s)
		if t == "" {
			return ""
		}

		t = strings.ToLower(t)
		t = strings.TrimSuffix(t, ".siva")
		t = filepath.Base(t)

		return t
	})
}

// LoadList calls LoadListFilter with an empty filter.
func LoadList(file string) ([]string, error) {
	return LoadListFilter(file, nil)
}

// LoadListFilter loads a list of strings, applies a filter function to each
// line, removes duplicates and returns it sorted lexicographically.
func LoadListFilter(file string, filter func(string) string) ([]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	hashes := make(map[string]struct{})

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		t := strings.TrimSpace(scanner.Text())
		if filter != nil {
			t = filter(t)
		}

		if t != "" {
			hashes[t] = struct{}{}
		}
	}

	list := make([]string, 0, len(hashes))
	for s := range hashes {
		list = append(list, s)
	}

	return list, nil
}

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
