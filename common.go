package borges

import (
	"io"

	"srcd.works/go-errors.v0"
)

var (
	// ErrAlreadyStopped signals that an operation cannot be done because
	// the entity is already sopped.
	ErrAlreadyStopped = errors.NewKind("already stopped: %s")

	ErrWaitForJobs = errors.NewKind("no more jobs at the moment")
)

// Job represents a borges job to fetch and archive a repository.
type Job struct {
	RepositoryID uint64
	URL          string
}

// JobIter is an iterator of Job.
type JobIter interface {
	io.Closer
	// Next returns the next job. It returns io.EOF if there are no more
	// jobs. If there are no more jobs at the moment, but there can be
	// in the future, it returns an error of kind ErrWaitForJobs.
	Next() (*Job, error)
}
