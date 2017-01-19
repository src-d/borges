package borges

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
)

type lineJobIter struct {
	*bufio.Scanner
}

// NewLineJobIter returns a JobIter that returns jobs generated from a reader
// with a list of repository URLs, one per line.
func NewLineJobIter(r io.Reader) JobIter {
	return &lineJobIter{bufio.NewScanner(r)}
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

	return &Job{RepositoryID: 0, URL: line}, nil
}
