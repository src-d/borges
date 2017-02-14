package borges

import (
	"bufio"
	"fmt"
	"io"
	"net/url"

	"srcd.works/core.v0/model"
	"github.com/satori/go.uuid"
)

type lineJobIter struct {
	*baseJobIter
	*bufio.Scanner
	r io.ReadCloser
}

// NewLineJobIter returns a JobIter that returns jobs generated from a reader
// with a list of repository URLs, one per line.
func NewLineJobIter(r io.ReadCloser, storer *model.RepositoryStore) JobIter {
	return &lineJobIter{
		baseJobIter: &baseJobIter{storer},
		Scanner:     bufio.NewScanner(r),
		r:           r,
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

	ID, err := i.getRepositoryID(line)
	if err != nil {
		return nil, err
	}

	return &Job{RepositoryID: ID}, nil
}

// Close closes the underlying reader.
func (i *lineJobIter) Close() error {
	return i.r.Close()
}
