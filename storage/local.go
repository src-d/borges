package storage

import (
	"fmt"
	"sync"
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-kallax.v1"
)

type localRepo struct {
	ID       kallax.ULID
	Endpoint string
	Status   model.FetchStatus
}

func (r *localRepo) toRepo() *model.Repository {
	return &model.Repository{
		ID:        r.ID,
		Status:    r.Status,
		Endpoints: []string{r.Endpoint},
	}
}

type localRepoStore struct {
	sync.RWMutex
	repos map[kallax.ULID]*localRepo
}

// Local creates a new local repository store that needs no database connection.
func Local() RepositoryStore {
	return &localRepoStore{
		repos: make(map[kallax.ULID]*localRepo),
	}
}

func (s *localRepoStore) Create(repo *model.Repository) error {
	s.Lock()
	defer s.Unlock()

	if len(repo.Endpoints) != 1 {
		return fmt.Errorf("expecting only 1 endpoint for repo %q, got %d", repo.ID, len(repo.Endpoints))
	}

	s.repos[repo.ID] = &localRepo{
		ID:       repo.ID,
		Endpoint: repo.Endpoints[0],
		Status:   repo.Status,
	}
	return nil
}

func (s *localRepoStore) Get(id kallax.ULID) (*model.Repository, error) {
	s.RLock()
	defer s.RUnlock()
	repo, ok := s.repos[id]
	if !ok {
		return nil, kallax.ErrNotFound
	}

	return repo.toRepo(), nil
}

func (s *localRepoStore) GetByEndpoints(endpoints ...string) ([]*model.Repository, error) {
	if len(endpoints) == 0 {
		return nil, nil
	}

	s.RLock()
	defer s.RUnlock()

	var repos []*model.Repository
	for _, repo := range s.repos {
		if containsString(endpoints, repo.Endpoint) {
			repos = append(repos, repo.toRepo())
		}
	}

	return repos, nil
}

func (s *localRepoStore) SetStatus(repo *model.Repository, status model.FetchStatus) error {
	s.Lock()
	defer s.Unlock()

	repo.Status = status
	localRepo, ok := s.repos[repo.ID]
	if !ok {
		return kallax.ErrNotFound
	}

	localRepo.Status = status
	return nil
}

func (s *localRepoStore) SetEndpoints(repo *model.Repository, endpoints ...string) error {
	if len(endpoints) != 1 {
		return fmt.Errorf("expecting only 1 endpoint for repo %q, got %d", repo.ID, len(endpoints))
	}

	s.Lock()
	defer s.Unlock()

	repo.Endpoints = endpoints
	localRepo, ok := s.repos[repo.ID]
	if !ok {
		return kallax.ErrNotFound
	}

	localRepo.Endpoint = endpoints[0]
	return nil
}

func (s *localRepoStore) UpdateFailed(repo *model.Repository, status model.FetchStatus) error {
	return s.SetStatus(repo, status)
}

func (s *localRepoStore) UpdateFetched(repo *model.Repository, fetchedAt time.Time) error {
	repo.FetchedAt = &fetchedAt
	return s.SetStatus(repo, model.Fetched)
}

func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
