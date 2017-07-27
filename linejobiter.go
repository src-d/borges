package borges

import (
	"bufio"
	"fmt"
	"io"
	"net/url"

	"gopkg.in/src-d/core-retrieval.v0/model"
)

type lineJobIter struct {
	storer *model.RepositoryStore
	*bufio.Scanner
	r io.ReadCloser
}

// NewLineJobIter returns a JobIter that returns jobs generated from a reader
// with a list of repository URLs, one per line.
func NewLineJobIter(r io.ReadCloser, storer *model.RepositoryStore) JobIter {
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
