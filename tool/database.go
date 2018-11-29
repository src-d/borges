package tool

import (
	"context"
	"database/sql"
	"runtime"
	"sort"
	"time"

	"github.com/src-d/borges/storage"

	"gopkg.in/src-d/core-retrieval.v0/model"
	kallax "gopkg.in/src-d/go-kallax.v1"
	"gopkg.in/src-d/go-log.v1"
)

const (
	logCount = 1000000

	dbSivaSQL      = "select init from repository_references"
	dbRefRemoveSQL = "delete from repository_references where init = $1"
)

// Database has the db functionality used by the tool.
type Database struct {
	db    *sql.DB
	store *storage.DatabaseStore
}

// NewDatabase creates and initializes a new Database struct.
func NewDatabase(db *sql.DB) *Database {
	d := &Database{
		db:    db,
		store: storage.FromDatabase(db),
	}

	return d
}

// Siva returns all siva files used by references.
func (d *Database) Siva() ([]string, error) {
	log.Infof("querying database")
	start := time.Now()

	rows, err := d.db.Query(dbSivaSQL)
	if err != nil {
		return nil, err
	}

	log.With(log.Fields{"duration": time.Since(start)}).
		Infof("database query ended")

	log.Infof("getting results")
	start = time.Now()
	partial := time.Now()

	m := make(map[string]struct{})
	var init string
	var counter uint64
	for rows.Next() {
		err = rows.Scan(&init)
		if err != nil {
			return nil, err
		}

		m[init] = struct{}{}

		if counter != 0 && counter%logCount == 0 {
			log.With(log.Fields{
				"counter":  counter,
				"duration": time.Since(start),
				"partial":  time.Since(partial),
				"sivas":    len(m),
			}).Infof("still working")

			partial = time.Now()
		}
		counter++
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	log.With(log.Fields{
		"duration":     time.Since(start),
		"memory":       mem.Alloc / 1024 / 1024,
		"total_memory": mem.TotalAlloc / 1024 / 1024,
		"references":   counter,
		"sivas":        len(m),
	}).Infof("finished getting results")

	start = time.Now()

	list := make([]string, len(m))
	var i int
	for k := range m {
		list[i] = k
		i++
	}

	sort.Strings(list)

	runtime.ReadMemStats(&mem)
	log.With(log.Fields{
		"duration":     time.Since(start),
		"memory":       mem.Alloc / 1024 / 1024,
		"total_memory": mem.TotalAlloc / 1024 / 1024,
	}).Infof("finished preparing siva list")

	return list, nil
}

// ReferencesWithInit returns all references with an specific init.
func (d *Database) ReferencesWithInit(init string) ([]*model.Reference, error) {
	sha1 := model.NewSHA1(init)
	return d.store.GetRefsByInit(sha1)
}

// RepositoriesWithInit returns all repositories with references to a
// specific init.
func (d *Database) RepositoriesWithInit(init string) ([]string, error) {
	refs, err := d.ReferencesWithInit(init)
	if err != nil {
		return nil, err
	}

	set := NewSet(false)
	for _, r := range refs {
		set.Add(r.Repository.ID.String())
	}

	return set.List(), nil
}

// DeleteReferences removes all references hold in an init.
func (d *Database) DeleteReferences(ctx context.Context, init string) error {
	_, err := d.db.ExecContext(ctx, dbRefRemoveSQL, init)
	if err != nil {
		return err
	}

	return nil
}

// Repository finds and returns a repo by ID.
func (d *Database) Repository(id string) (*model.Repository, error) {
	ulid, err := kallax.NewULIDFromText(id)
	if err != nil {
		return nil, err
	}

	return d.store.Get(ulid)
}
