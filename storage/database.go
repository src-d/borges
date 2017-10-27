package storage

import (
	"database/sql"
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	kallax "gopkg.in/src-d/go-kallax.v1"
)

type dbRepoStore struct {
	*model.RepositoryStore
}

// FromDatabase returns a new repository store that interacts with a PostgreSQL
// FromDatabase to store all the data.
func FromDatabase(db *sql.DB) RepoStore {
	return &dbRepoStore{model.NewRepositoryStore(db)}
}

func (s *dbRepoStore) Create(repo *model.Repository) error {
	_, err := s.Save(repo)
	return err
}

func (s *dbRepoStore) Get(id kallax.ULID) (*model.Repository, error) {
	q := model.NewRepositoryQuery().FindByID(id)
	return s.FindOne(q)
}

func (s *dbRepoStore) GetByEndpoints(endpoints ...string) ([]*model.Repository, error) {
	q := make([]interface{}, len(endpoints))
	for _, ep := range endpoints {
		q = append(q, ep)
	}

	rs, err := s.Find(
		model.NewRepositoryQuery().
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

func (s *dbRepoStore) SetStatus(repo *model.Repository, status model.FetchStatus) error {
	repo.Status = status
	_, err := s.RepositoryStore.Update(
		repo,
		model.Schema.Repository.Status,
	)
	return err
}

func (s *dbRepoStore) SetEndpoints(repo *model.Repository, endpoints ...string) error {
	repo.Endpoints = endpoints
	_, err := s.Update(repo, model.Schema.Repository.Endpoints)
	return err
}

func (s *dbRepoStore) UpdateFailed(repo *model.Repository, status model.FetchStatus) error {
	repo.Status = status
	_, err := s.Update(repo,
		model.Schema.Repository.UpdatedAt,
		model.Schema.Repository.FetchErrorAt,
		model.Schema.Repository.References,
		model.Schema.Repository.Status,
	)

	return err
}

func (s *dbRepoStore) UpdateFetched(repo *model.Repository, fetchedAt time.Time) error {
	repo.Status = model.Fetched
	repo.FetchedAt = &fetchedAt
	repo.LastCommitAt = lastCommitTime(repo.References)

	_, err := s.Update(repo,
		model.Schema.Repository.UpdatedAt,
		model.Schema.Repository.FetchedAt,
		model.Schema.Repository.LastCommitAt,
		model.Schema.Repository.Status,
		model.Schema.Repository.References,
	)

	return err
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
