package storage

import (
	"database/sql"
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-kallax.v1"
)

// DatabaseStore implements a borges.RepositoryStorage based on a database.
type DatabaseStore struct {
	*model.RepositoryStore
}

// FromDatabase returns a new repository store that interacts with a PostgreSQL
// FromDatabase to store all the data.
func FromDatabase(db *sql.DB) *DatabaseStore {
	return &DatabaseStore{model.NewRepositoryStore(db)}
}

// Create honors the borges.RepositoryStore interface.
func (s *DatabaseStore) Create(repo *model.Repository) error {
	_, err := s.Save(repo)
	return err
}

// Get honors the borges.RepositoryStore interface.
func (s *DatabaseStore) Get(id kallax.ULID) (*model.Repository, error) {
	q := model.NewRepositoryQuery().WithReferences(nil).FindByID(id)
	return s.FindOne(q)
}

// GetByEndpoints honors the borges.RepositoryStore interface.
func (s *DatabaseStore) GetByEndpoints(endpoints ...string) ([]*model.Repository, error) {
	q := make([]interface{}, len(endpoints))
	for _, ep := range endpoints {
		q = append(q, ep)
	}

	rs, err := s.Find(
		model.NewRepositoryQuery().
			WithReferences(nil).
			Where(kallax.And(kallax.ArrayOverlap(
				model.Schema.Repository.Endpoints, q...,
			))),
	)
	if err != nil {
		return nil, err
	}

	repositories, err := rs.All()
	if err != nil {
		return nil, err
	}

	return repositories, nil
}

// SetStatus honors the borges.RepositoryStore interface.
func (s *DatabaseStore) SetStatus(repo *model.Repository, status model.FetchStatus) error {
	repo.Status = status
	return s.updateWithRefsChanged(
		repo,
		model.Schema.Repository.Status,
	)
}

// SetEndpoints honors the borges.RepositoryStore interface.
func (s *DatabaseStore) SetEndpoints(repo *model.Repository, endpoints ...string) error {
	repo.Endpoints = endpoints
	return s.updateWithRefsChanged(repo, model.Schema.Repository.Endpoints)
}

// UpdateFailed honors the borges.RepositoryStore interface.
func (s *DatabaseStore) UpdateFailed(repo *model.Repository, status model.FetchStatus) error {
	repo.Status = status
	return s.updateWithRefsChanged(repo,
		model.Schema.Repository.UpdatedAt,
		model.Schema.Repository.FetchErrorAt,
		model.Schema.Repository.Status,
	)
}

// UpdateFetched honors the borges.RepositoryStore interface.
func (s *DatabaseStore) UpdateFetched(repo *model.Repository, fetchedAt time.Time) error {
	repo.Status = model.Fetched
	repo.FetchedAt = &fetchedAt
	repo.LastCommitAt = lastCommitTime(repo.References)

	return s.updateWithRefsChanged(repo,
		model.Schema.Repository.UpdatedAt,
		model.Schema.Repository.FetchedAt,
		model.Schema.Repository.LastCommitAt,
		model.Schema.Repository.Status,
	)
}

func (s *DatabaseStore) updateWithRefsChanged(repo *model.Repository, fields ...kallax.SchemaField) error {
	return s.Transaction(func(store *model.RepositoryStore) error {
		var refStore model.ReferenceStore
		kallax.StoreFrom(&refStore, store)

		refs, err := refStore.FindAll(model.NewReferenceQuery().FindByRepository(repo.ID))
		if err != nil {
			return err
		}

		for _, ref := range refs {
			if err := refStore.Delete(ref); err != nil {
				return err
			}
		}

		for _, ref := range repo.References {
			// Some references may come from the database, so they can't be inserted
			// because they are marked as already persisted. Can't be updated either
			// because we just deleted them all.
			var emptyModel kallax.Model
			ref.Model = emptyModel
			// Can't use refStore.Insert(ref) because that would trigger an update on
			// the repository, which causes an error because there are no affected rows
			// so we have to resort to the generic store Insert, which does not perform
			// updates/inserts of any relationships.
			err := store.GenericStore().Insert(model.Schema.Reference.BaseSchema, ref)
			if err != nil {
				return err
			}
		}

		_, err = store.Update(repo, fields...)
		return err
	})
}

func lastCommitTime(refs []*model.Reference) *time.Time {
	if len(refs) == 0 {
		return nil
	}

	var last time.Time
	for _, ref := range refs {
		if last.Before(ref.Time) {
			last = ref.Time
		}
	}

	return &last
}
