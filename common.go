package borges

import (
	"fmt"
	"io"
	"strings"

	"github.com/satori/go.uuid"
	"srcd.works/core.v0"
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
	RepositoryID uuid.UUID
}

// JobIter is an iterator of Job.
type JobIter interface {
	io.Closer
	// Next returns the next job. It returns io.EOF if there are no more
	// jobs. If there are no more jobs at the moment, but there can be
	// in the future, it returns an error of kind ErrWaitForJobs.
	Next() (*Job, error)
}

// TODO temporal
func DropTables(names ...string) {
	smt := fmt.Sprintf("DROP TABLE IF EXISTS %s;", strings.Join(names, ", "))
	if _, err := core.Database().Exec(smt); err != nil {
		panic(err)
	}
}

// TODO temporal delete when kallax implements it
func CreateRepositoryTable() {
	_, err := core.Database().Exec(`CREATE TABLE IF NOT EXISTS repositories (
	id uuid PRIMARY KEY,
	created_at timestamptz,
	updated_at timestamptz,
	endpoints text[],
	status varchar(20),
	fetched_at timestamptz,
	fetch_error_at timestamptz,
	last_commit_at timestamptz,
	_references jsonb
	)`)

	if err != nil {
		panic(err)
	}
}
