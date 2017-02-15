package borges

import (
	"github.com/satori/go.uuid"
	"github.com/src-d/go-kallax"
	"srcd.works/core.v0/model"
)

type repositoryStore struct {
	storer *model.RepositoryStore
}

// RepositoryID tries to find a repository by the endpoint into the database.
// If no repository is found, it creates a new one and returns the ID.
func (s repositoryStore) RepositoryID(endpoint string) (uuid.UUID, error) {
	rs, err := s.storer.Find(
		model.NewRepositoryQuery().
			Where(kallax.And(kallax.ArrayContains(
				model.Schema.Repository.Endpoints, endpoint,
			))),
	)
	if err != nil {
		return uuid.Nil, err
	}

	repositories, err := rs.All()
	if err != nil {
		return uuid.Nil, err
	}

	l := len(repositories)
	switch {
	case l == 0:
		r := model.NewRepository()
		r.Endpoints = []string{endpoint}
		if _, err := s.storer.Save(r); err != nil {
			return uuid.Nil, err
		}

		return uuid.UUID(r.ID), nil
	case l > 1:
		// TODO log error printing the ids and the endpoint
	}

	return uuid.UUID(repositories[0].ID), nil
}
