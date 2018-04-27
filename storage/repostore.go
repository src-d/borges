package storage

import (
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-kallax.v1"
)

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
