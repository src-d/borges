package borges

import (
	"encoding/hex"
	"io"
	"time"

	"gopkg.in/src-d/go-git.v4/plumbing"
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

// Repository represents a remote repository found on the Internet.
type Repository struct {
	// ID is a unique identifier.
	ID uint64
	// Endpoints is a slice of valid git endpoints to reach this repository.
	// For example, git://host/my/repo.git and https://host/my/repo.git.
	// They are meant to be endpoints of the same exact repository, and not
	// mirrors.
	Endpoints []string
	// Status is the fetch status of tge repository in our repository storage.
	Status FetchStatus
	// CreatedAt is the timestamp of the creation of this record.
	CreatedAt time.Time
	// FetchedAt is the timestamp of the last time this repository was
	// fetched and archived in our repository storage successfully.
	FetchedAt time.Time
	// FetchErrorAt is the timestamp of the last fetch error, if any.
	FetchErrorAt time.Time
	// LastCommitAt is the last commit time found in this repository.
	LastCommitAat time.Time
	// References is the current slice of references as present in our
	// repository storage.
	References []*Reference
}

// FetchStatus represents the fetch status of this repository.
type FetchStatus string

const (
	// NotFound means that the remote repository was not found at the given
	// endpoints.
	NotFound FetchStatus = "not_found"
	// Fetched means that the remote repository was found, fetched and
	// successfully stored.
	Fetched = "fetched"
	// Pending is the default value, meaning that the repository has not
	// been fetched yet.
	Pending = "pending"
)

// Reference is a reference of a repository as present in our repository storage.
type Reference struct {
	// Name is the full reference name.
	Name string
	// Hash is the hash of the reference.
	Hash SHA1
	// Init is the hash of the init commit reached from this reference.
	Init SHA1
	// Roots is a slice of the hashes of all root commits reachable from
	// this reference.
	Roots []SHA1
	// UpdatedAt is the timestamp of the last time we updated this reference.
	UpdatedAt time.Time
	// FirstSeenAt is the timestamp of the first time we saw this reference.
	FirstSeenAt time.Time
}

func (r *Reference) GitReference() *plumbing.Reference {
	return plumbing.NewHashReference(
		plumbing.ReferenceName(r.Name),
		plumbing.Hash(r.Hash),
	)
}

// SHA1 is a SHA-1 hash.
type SHA1 [20]byte

func NewSHA1(s string) SHA1 {
	b, _ := hex.DecodeString(s)

	var h SHA1
	copy(h[:], b)

	return h
}

// String representation from this SHA1
func (h SHA1) String() string {
	return hex.EncodeToString(h[:])
}
