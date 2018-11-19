package tool

import (
	"database/sql"
	"runtime"
	"sort"
	"time"

	"gopkg.in/src-d/go-log.v1"
)

const (
	logCount        = 1000000
	databaseSivaSQL = "select init from repository_references"
)

// DatabaseSiva returns all siva files used by references.
func DatabaseSiva(db *sql.DB) ([]string, error) {
	log.Infof("querying database")
	start := time.Now()

	rows, err := db.Query(databaseSivaSQL)
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

		counter++
		if counter%logCount == 0 {
			log.With(log.Fields{
				"counter":  counter,
				"duration": time.Since(start),
				"partial":  time.Since(partial),
				"sivas":    len(m),
			}).Infof("still working")

			partial = time.Now()
		}
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
