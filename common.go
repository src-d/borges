package borges

import (
	"io"
	"time"

	"github.com/satori/go.uuid"
	"gopkg.in/src-d/core-retrieval.v0/model"
	kallax "gopkg.in/src-d/go-kallax.v1"
)

// Job represents a borges job to fetch and archive a repository.
type Job struct {
	RepositoryID uuid.UUID
}

// JobIter is an iterator of Job.
type JobIter interface {
	io.Closer
	// Next returns the next job. It returns io.EOF if there are no more jobs.
	Next() (*Job, error)
}

// RepositoryStore is the access layer to the storage of repositories.
type RepositoryStore interface {
	// Create inserts a new Repository in the store.
	Create(repo *model.Repository) error
	// Get returns a Repository given its ID.
	Get(id kallax.ULID) (*model.Repository, error)
	// GetByEndpoints returns the Repositories that have common endpoints with the
	// list of endpoints passed.
	GetByEndpoints(endpoints ...string) ([]*model.Repository, error)
	// SetStatus changes the status of the given repository.
	SetStatus(repo *model.Repository, status model.FetchStatus) error
	// SetEndpoints updates the endpoints of the repository.
	SetEndpoints(repo *model.Repository, endpoints ...string) error
	// UpdateFailed updates the given repository as failed with the given
	// status. No modifications are performed to the repository itself
	// other than setting its status, all the modification to the repo
	// fields must be done before calling this method. That is, changing
	// FetchErrorAt and so on should be done manually before. Refer to the
	// concrete implementation to know what is being updated.
	UpdateFailed(repo *model.Repository, status model.FetchStatus) error
	// Update updates the given repository as successfully fetched.
	// No modifications are performed to the repository other than setting
	// the Fetched status and the time when it was fetched, all other changes
	// should be done to the repo before calling this method. Refer to the
	// concrete implementation to know what is being updated.
	UpdateFetched(repo *model.Repository, fetchedAt time.Time) error
}

// RepositoryID tries to find a repository by the endpoint into the database.
// If no repository is found, it creates a new one and returns the ID.
func RepositoryID(endpoints []string, isFork *bool, storer RepositoryStore) (uuid.UUID, error) {
	repositories, err := storer.GetByEndpoints(endpoints...)
	if err != nil {
		return uuid.Nil, err
	}

	if len(repositories) == 0 {
		r := model.NewRepository()
		r.Endpoints = endpoints
		r.IsFork = isFork
		if err := storer.Create(r); err != nil {
			return uuid.Nil, err
		}

		return uuid.UUID(r.ID), nil
	}

	// TODO log error printing the ids and the endpoint

	r := repositories[0]

	// check if the existing repository has all the aliases
	allEndpoints, update := getUniqueEndpoints(r.Endpoints, endpoints)

	if update {
		if err := storer.SetEndpoints(r, allEndpoints...); err != nil {
			return uuid.Nil, err
		}
	}

	return uuid.UUID(repositories[0].ID), nil
}

func getUniqueEndpoints(re, ne []string) ([]string, bool) {
	actualSet := make(map[string]bool)
	outputSet := make(map[string]bool)

	for _, e := range re {
		actualSet[e] = true
		outputSet[e] = true
	}

	eEq := 0
	for _, e := range ne {
		if _, ok := actualSet[e]; ok {
			eEq++
		}

		outputSet[e] = true
	}

	if eEq == len(outputSet) {
		return nil, false
	}

	var result []string
	for e := range outputSet {
		result = append(result, e)
	}

	return result, true
}

// Referencer can retrieve reference models (*model.Reference).
type Referencer interface {
	// References retrieves a slice of *model.Reference or an error.
	References() ([]*model.Reference, error)
}
