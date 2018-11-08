package storage

import (
	"fmt"
	"sync"
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-kallax.v1"
)

type localRepository struct {
	ID         kallax.ULID
	Endpoint   string
	Status     model.FetchStatus
	References []*localReference
}

func (r *localRepository) toRepo() *model.Repository {
	repo := &model.Repository{
		ID:        r.ID,
		Status:    r.Status,
		Endpoints: []string{r.Endpoint},
	}

	var refs []*model.Reference
	for _, ref := range r.References {
		refs = append(refs, ref.toReference(repo))
	}

	repo.References = refs
	return repo
}

type localReference struct {
	Init model.SHA1
}

func (r *localReference) toReference(repo *model.Repository) *model.Reference {
	ref := model.NewReference()
	ref.Init = r.Init
	ref.Repository = repo

	return ref
}

// LocalStore represents a borges.RepositoryStore that isn't backed by any
// database.
type LocalStore struct {
	sync.RWMutex
	repos map[kallax.ULID]*localRepository
}

// Local creates a new local repository store that needs no database connection.
func Local() *LocalStore {
	return &LocalStore{
		repos: make(map[kallax.ULID]*localRepository),
	}
}

// Create honors the borges.RepositoryStore interface.
func (s *LocalStore) Create(r *model.Repository) error {
	s.Lock()
	defer s.Unlock()

	if len(r.Endpoints) != 1 {
		return fmt.Errorf("expecting only 1 endpoint for repository %q, got %d", r.ID, len(r.Endpoints))
	}

	s.repos[r.ID] = &localRepository{
		ID:       r.ID,
		Endpoint: r.Endpoints[0],
		Status:   r.Status,
	}
	return nil
}

// Get honors the borges.RepositoryStore interface.
func (s *LocalStore) Get(id kallax.ULID) (*model.Repository, error) {
	s.RLock()
	defer s.RUnlock()
	repo, ok := s.repos[id]
	if !ok {
		return nil, kallax.ErrNotFound
	}

	return repo.toRepo(), nil
}

// GetByEndpoints honors the borges.RepositoryStore interface.
func (s *LocalStore) GetByEndpoints(endpoints ...string) ([]*model.Repository, error) {
	if len(endpoints) == 0 {
		return nil, nil
	}

	s.RLock()
	defer s.RUnlock()

	var repos []*model.Repository
	for _, r := range s.repos {
		if containsString(endpoints, r.Endpoint) {
			repos = append(repos, r.toRepo())
		}
	}

	return repos, nil
}

// GetRefsByInit honors the borges.RepositoryStore interface.
func (s *LocalStore) GetRefsByInit(
	init model.SHA1,
) ([]*model.Reference, error) {
	s.RLock()
	defer s.RUnlock()

	sha1 := model.SHA1(init)

	var refs []*model.Reference
	for _, r := range s.repos {
		repo := r.toRepo()
		for _, ref := range r.References {
			if ref.Init == sha1 {
				refs = append(refs, ref.toReference(repo))
			}
		}
	}

	return refs, nil
}

// InitHasRefs honors the borges.RepositoryStore interface.
func (s *LocalStore) InitHasRefs(
	init model.SHA1,
) (bool, error) {
	s.RLock()
	defer s.RUnlock()

	sha1 := model.SHA1(init)

	for _, r := range s.repos {
		for _, ref := range r.References {
			if ref.Init == sha1 {
				return true, nil
			}
		}
	}

	return false, nil
}

// SetStatus honors the borges.RepositoryStore interface.
func (s *LocalStore) SetStatus(r *model.Repository, status model.FetchStatus) error {
	s.Lock()
	defer s.Unlock()

	r.Status = status
	localRepo, ok := s.repos[r.ID]
	if !ok {
		return kallax.ErrNotFound
	}

	localRepo.Status = status
	return nil
}

// SetEndpoints honors the borges.RepositoryStore interface.
func (s *LocalStore) SetEndpoints(r *model.Repository, endpoints ...string) error {
	if len(endpoints) != 1 {
		return fmt.Errorf("expecting only 1 endpoint for repo %q, got %d", r.ID, len(endpoints))
	}

	s.Lock()
	defer s.Unlock()

	r.Endpoints = endpoints
	localRepo, ok := s.repos[r.ID]
	if !ok {
		return kallax.ErrNotFound
	}

	localRepo.Endpoint = endpoints[0]
	return nil
}

// UpdateFailed honors the borges.RepositoryStore interface.
func (s *LocalStore) UpdateFailed(r *model.Repository, status model.FetchStatus) error {
	return s.SetStatus(r, status)
}

// UpdateFetched honors the borges.RepositoryStore interface.
func (s *LocalStore) UpdateFetched(r *model.Repository, fetchedAt time.Time) error {
	r.FetchedAt = &fetchedAt
	return s.SetStatus(r, model.Fetched)
}

func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
