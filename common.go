package borges

import (
	"io"

	"github.com/src-d/borges/storage"

	"github.com/satori/go.uuid"
	"gopkg.in/src-d/core-retrieval.v0/model"
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

// RepositoryID tries to find a repository by the endpoint into the database.
// If no repository is found, it creates a new one and returns the ID.
func RepositoryID(endpoints []string, isFork *bool, storer storage.RepositoryStore) (uuid.UUID, error) {
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
