package borges

import (
	"github.com/satori/go.uuid"
	"github.com/src-d/go-kallax"
	"srcd.works/core.v0/model"
)

type baseJobIter struct {
	storer *model.RepositoryStore
}

// getRepositoryID tries to find a repository by the endpoint into the database.
// If no repository is found, it creates a new one and returns the ID.
func (i *baseJobIter) getRepositoryID(endpoint string) (uuid.UUID, error) {
	rs, err := i.storer.Find(
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

	var ID uuid.UUID
	if l == 0 {
		r := model.NewRepository()
		r.Endpoints = []string{endpoint}
		if _, err := i.storer.Save(r); err != nil {
			return uuid.Nil, err
		}
		ID = uuid.UUID(r.ID)
	} else {
		if l > 1 {
			// TODO log error printing the ids and the endpoint
		}

		ID = uuid.UUID(repositories[0].ID)
	}

	return ID, nil
}
