package tool

import (
	"context"

	"github.com/satori/go.uuid"
	"github.com/src-d/borges"
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-log.v1"
	"gopkg.in/src-d/go-queue.v1"
)

const (
	defaultRetries  int32 = 5
	defaultPriority       = queue.PriorityNormal
)

var (
	queueableStates = []model.FetchStatus{
		model.Fetched,
		model.Fetching,
	}
)

// Repository has the information to manage repositories in the database
// and message queue.
type Repository struct {
	worker
	db *Database
	q  queue.Queue

	dry             bool
	retries         int32
	priority        queue.Priority
	queueableStates []model.FetchStatus
}

// NewRepository creates and initializes a new Repository struct.
func NewRepository(db *Database, q queue.Queue) *Repository {
	return &Repository{
		db:              db,
		q:               q,
		dry:             false,
		retries:         defaultRetries,
		priority:        defaultPriority,
		queueableStates: queueableStates,
	}
}

// Retries sets the retries value used in new queued jobs.
func (r *Repository) Retries(c int32) {
	r.retries = c
}

// Priority sets the priority level for new queued jobs.
func (r *Repository) Priority(p queue.Priority) {
	r.priority = p
}

func (r *Repository) queueWorker(ctx context.Context, c chan string) {
	for init := range c {
		err := r.QueueOne(init)
		if err != nil {
			r.error(err)
		}
	}
}

// Queue creates new jobs for the repositories in the provided list. It
// skips repositories that are not in a queueable state, by default
// Fetched or Fetching. After submitting the job the repository is set to
// Pending state.
func (r *Repository) Queue(ctx context.Context, list []string) error {
	chn := make(chan string)
	wctx := r.setupWorker(ctx, func(c context.Context) {
		r.queueWorker(c, chn)
	})

	for i, h := range list {
		if i != 0 && i%logDeleteCount == 0 {
			log.With(log.Fields{"count": i}).Infof("queuing repositories")
		}

		select {
		case <-wctx.Done():
			return wctx.Err()
		default:
			chn <- h
		}
	}

	close(chn)
	r.wait()

	return nil
}

// QueueOne queues one repository.
func (r *Repository) QueueOne(id string) error {
	l := log.With(log.Fields{"id": id})

	repo, err := r.db.Repository(id)
	if err != nil {
		l.Errorf(err, "error getting repository")
		return err
	}

	l = l.With(log.Fields{"status": repo.Status})

	if !r.queueable(repo) {
		l.Debugf("repository not in a queueable status, skipping")
		return nil
	}

	job, err := r.createJob(id)
	if err != nil {
		return err
	}

	l.With(log.Fields{"status": repo.Status}).
		Debugf("repository not in a queueable status, skipping")

	if !r.dry {
		err = r.q.Publish(job)
		if err != nil {
			return err
		}
	}

	if !r.dry {
		err = r.db.store.SetStatus(repo, model.Pending)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Repository) queueable(repo *model.Repository) bool {
	if repo == nil {
		return false
	}

	for _, s := range r.queueableStates {
		if repo.Status == s {
			return true
		}
	}

	return false
}

func (r *Repository) createJob(id string) (*queue.Job, error) {
	u, err := uuid.FromString(id)
	if err != nil {
		return nil, err
	}
	job := &borges.Job{RepositoryID: u}

	qj, err := queue.NewJob()
	if err != nil {
		return nil, err
	}

	if err := qj.Encode(job); err != nil {
		return nil, err
	}

	qj.Retries = r.retries
	qj.SetPriority(r.priority)

	return qj, nil
}
