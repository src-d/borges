package borges

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

type lineJobIter struct {
	storer RepositoryStore
	*bufio.Scanner
	r io.ReadCloser
}

// NewLineJobIter returns a JobIter that returns jobs generated from a reader
// with a list of repository URLs, one per line.
func NewLineJobIter(r io.ReadCloser, storer RepositoryStore) JobIter {
	return &lineJobIter{
		storer:  storer,
		Scanner: bufio.NewScanner(r),
		r:       r,
	}
}

func (i *lineJobIter) Next() (*Job, error) {
	if !i.Scan() {
		if err := i.Err(); err != nil {
			return nil, err
		}

		return nil, io.EOF
	}

	line := string(i.Bytes())
	// check if the line is an absolute path to a directory.
	// If the path is a directory we can look for the .git directory to try
	// to guess if it's a git repo or a bare repo.
	// If .git does not exist it will be treated as a bare repo (even if it's
	// not).
	if path.IsAbs(line) {
		dotGit := filepath.Join(line, ".git")
		if _, err := os.Stat(dotGit); os.IsNotExist(err) {
			line = fmt.Sprintf("file://%s", line)
		} else if err != nil {
			return nil, fmt.Errorf("expecting remote or local repository, instead %q was found", line)
		} else {
			line = fmt.Sprintf("file://%s", dotGit)
		}
	}

	u, err := url.Parse(line)
	if err != nil {
		return nil, err
	}

	if !u.IsAbs() {
		return nil, fmt.Errorf("expected absolute URL: %s", line)
	}

	ID, err := RepositoryID([]string{line}, nil, i.storer)
	if err != nil {
		return nil, err
	}

	return &Job{RepositoryID: ID}, nil
}

// Close closes the underlying reader.
func (i *lineJobIter) Close() error {
	return i.r.Close()
}
