package storage

import (
	"database/sql"
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-kallax.v1"
	"gopkg.in/src-d/go-log.v1"
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
	start := time.Now()
	_, err := s.Save(repo)

	logger := log.With(log.Fields{
		"duration": time.Since(start),
		"endpoint": repo.Endpoints,
	})

	if err != nil {
		logger.Errorf(err, "could not create repository")
		return err
	}

	logger.Debugf("create repository finished")
	return nil
}

// Get honors the borges.RepositoryStore interface.
func (s *DatabaseStore) Get(id kallax.ULID) (*model.Repository, error) {
	start := time.Now()

	q := model.NewRepositoryQuery().WithReferences(nil).FindByID(id)
	r, err := s.FindOne(q)

	logger := log.With(log.Fields{
		"duration": time.Since(start),
		"id":       id,
	})

	if err != nil {
		logger.Errorf(err, "could not get repository")
		return nil, err
	}

	logger.Debugf("get repository finished")
	return r, nil
}

// GetByEndpoints honors the borges.RepositoryStore interface.
func (s *DatabaseStore) GetByEndpoints(
	endpoints ...string,
) ([]*model.Repository, error) {
	start := time.Now()

	q := make([]interface{}, len(endpoints))
	for _, ep := range endpoints {
		q = append(q, ep)
	}

	rs, err := s.Find(
		model.NewRepositoryQuery().
			WithReferences(nil).
			Where(kallax.And(kallax.ArrayOverlap(
				model.Schema.Repository.Endpoints, q...,
			))).
			Order(kallax.Asc(model.Schema.Repository.Endpoints)),
	)
	if err != nil {
		return nil, err
	}

	repositories, err := rs.All()

	logger := log.With(log.Fields{
		"duration":  time.Since(start),
		"endpoints": endpoints,
	})

	if err != nil {
		logger.Errorf(err, "could not get repository by endpoints")
		return nil, err
	}

	logger.Debugf("get repository by endpoints finished")
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

func (s *DatabaseStore) updateWithRefsChanged(
	repo *model.Repository,
	fields ...kallax.SchemaField,
) error {
	start := time.Now()

	err := s.Transaction(func(store *model.RepositoryStore) error {
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

	logger := log.With(log.Fields{
		"duration": time.Since(start),
		"id":       repo.ID,
	})

	if err != nil {
		logger.Errorf(err, "could not update with references")
		return err
	}

	logger.Debugf("update with references finished")
	return nil
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
