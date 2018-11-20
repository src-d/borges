package tool

import (
	"context"
	"database/sql"
	"runtime"
	"sort"
	"time"

	"github.com/src-d/borges/storage"

	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-log.v1"
)

const (
	logCount = 1000000

	dbSivaSQL      = "select init from repository_references"
	dbRefRemoveSQL = "delete from repository_references where init = ?"
)

type Database struct {
	db    *sql.DB
	store *storage.DatabaseStore
}

func NewDatabase(db *sql.DB) *Database {
	d := &Database{
		db:    db,
		store: storage.FromDatabase(db),
	}

	return d
}

// DatabaseSiva returns all siva files used by references.
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

func (d *Database) ReferencesWithInit(init string) ([]*model.Reference, error) {
	sha1 := model.NewSHA1(init)
	return d.store.GetRefsByInit(sha1)
}

func (d *Database) RepositoriesWithInit(init string) ([]string, error) {
	sha1 := model.NewSHA1(init)
	refs, err := d.store.GetRefsByInit(sha1)
	if err != nil {
		return nil, err
	}

	set := NewSet(false)
	for _, r := range refs {
		set.Add(r.Repository.ID.String())
	}

	return set.List(), nil
}

func (d *Database) DeleteReferences(ctx context.Context, init string) error {
	_, err := d.db.ExecContext(ctx, dbRefRemoveSQL, init)
	if err != nil {
		return err
	}

	return nil
}
