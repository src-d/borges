package borges

import (
	"database/sql"
	"time"

	"github.com/inconshreveable/log15"
	uuid "github.com/satori/go.uuid"
	"github.com/src-d/borges/storage"
	core "gopkg.in/src-d/core-retrieval.v0"
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/framework.v0/queue"
	kallax "gopkg.in/src-d/go-kallax.v1"
)

const (
	queryByUpdate = `select id from repositories where status='fetched' and
		updated_at < $1 order by updated_at limit $2`
	queryByLastCommit = `select id from repositories where status='fetched'
		order by age(updated_at, last_commit_at) limit $1`
)

type updaterSearchFunction struct {
	name     string
	function func(int) ([]kallax.ULID, error)
	priority queue.Priority
}

// Updater finds fetched repositories to be updated
type Updater struct {
	log log15.Logger

	db      *sql.DB
	storage storage.RepoStore
	queue   queue.Queue

	// Time between update actions
	cadence time.Duration

	// Maximum number of jobs to submit per update
	maxJobs uint

	// Age when we consider a repository update to be old
	oldJobs time.Duration
}

// NewUpdater creates a new Updater
func NewUpdater(
	log log15.Logger,
	db *sql.DB,
	queue queue.Queue,
	cadence time.Duration,
	maxJobs uint,
	oldJobs time.Duration,
) *Updater {
	return &Updater{
		log:     log,
		db:      db,
		storage: storage.FromDatabase(core.Database()),
		queue:   queue,
		cadence: cadence,
		maxJobs: maxJobs,
		oldJobs: oldJobs,
	}
}

// Start initializes the update process
func (u *Updater) Start() {
	ticker := time.NewTicker(u.cadence)

	for t := range ticker.C {
		u.execute(t)
	}
}

func (u *Updater) searchFunctions() []updaterSearchFunction {
	return []updaterSearchFunction{
		{"old repositories", u.reposOld, queue.PriorityUrgent},
		{"almost old repositories", u.reposAlmostOld, queue.PriorityNormal},
		{"best effort", u.reposBestEffort, queue.PriorityLow},
	}
}

func (u *Updater) execute(t time.Time) {
	executeStart := time.Now()
	jobsLeft := int(u.maxJobs)

	for _, f := range u.searchFunctions() {
		log := u.log.New("function", f.name)

		log.Debug("search start", "priority", f.priority)
		start := time.Now()

		num, err := u.executeFunction(log, f, jobsLeft)
		if err != nil {
			continue
		}

		log.Debug("search end", "duration", time.Since(start),
			"jobs", num)

		jobsLeft -= num

		if jobsLeft <= 0 {
			log.Debug("maximum number of jobs")
			return
		}
	}

	u.log.Info("Update finished", "jobs", int(u.maxJobs)-jobsLeft,
		"duration", time.Since(executeStart))
}

func (u *Updater) executeFunction(
	log log15.Logger,
	f updaterSearchFunction,
	limit int,
) (int, error) {
	log = log.New("function", f.name)

	res, err := f.function(limit)
	if err != nil {
		log.Error("Error executing search function", "query", err)
		return 0, err
	}

	for _, s := range res {
		log = log.New("uuid", s)

		id, err := uuid.FromString(s.String())
		if err != nil {
			log.Error("Could not parse uuid", "error", err)
			continue
		}

		job, err := queue.NewJob()
		if err != nil {
			log.Error("Could not create job", "error", err)
			continue
		}

		payload := Job{RepositoryID: id}
		if err := job.Encode(payload); err != nil {
			log.Error("Could not encode job", "error", err)
			continue
		}

		job.SetPriority(f.priority)

		repo, err := u.storage.Get(s)
		if err != nil {
			log.Error("Could not get repository from database", "error", err)
			continue
		}

		err = u.storage.SetStatus(repo, model.Pending)
		if err != nil {
			log.Error("Could not change repository status in the database",
				"error", err)
			continue
		}

		if err := u.queue.Publish(job); err != nil {
			log.Error("Could not submit job", "error", err)
			continue
		}
	}

	return len(res), nil
}

func (u *Updater) reposBestEffort(limit int) ([]kallax.ULID, error) {
	return u.transformResult(u.db.Query(queryByLastCommit, limit))
}

func (u *Updater) reposOld(limit int) ([]kallax.ULID, error) {
	date := time.Now().Add(-1 * u.oldJobs)
	return u.transformResult(u.db.Query(queryByUpdate, date, limit))
}

func (u *Updater) reposAlmostOld(limit int) ([]kallax.ULID, error) {
	date := time.Now().Add(-1 * u.oldJobs / 2)
	return u.transformResult(u.db.Query(queryByUpdate, date, limit))
}

func (u *Updater) transformResult(
	rows *sql.Rows,
	err error,
) ([]kallax.ULID, error) {
	if err != nil {
		return nil, err
	}

	result := make([]kallax.ULID, 0, u.maxJobs)
	for rows.Next() {
		var r kallax.ULID
		err := rows.Scan(&r)
		if err != nil {
			u.log.Error("Error retrieving row", err)
			continue
		}

		result = append(result, r)
	}

	return result, rows.Err()
}
