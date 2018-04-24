package borges

import (
	"io"
	"time"

	"github.com/src-d/borges/storage"
	"gopkg.in/src-d/framework.v0/queue"
	"gopkg.in/src-d/go-log.v0"
)

// Executor retrieves jobs from an job iterator and passes them to a worker
// pool to be executed.
// Executor acts as a producer-consumer in a single component.
type Executor struct {
	log   log.Logger
	wp    *WorkerPool
	q     queue.Queue
	store storage.RepoStore
	iter  JobIter
}

// NewExecutor creates a new job executor.
func NewExecutor(
	log log.Logger,
	q queue.Queue,
	pool *WorkerPool,
	store storage.RepoStore,
	iter JobIter,
) *Executor {
	return &Executor{
		log:   log,
		wp:    pool,
		q:     q,
		store: store,
		iter:  iter,
	}
}

// Execute will queue all jobs and distribute them across the worker pool
// for them to be executed.
func (p *Executor) Execute() error {
	if err := p.queueJobs(); err != nil {
		return err
	}

	var errCh = make(chan error)
	go func() {
		errCh <- p.start()
	}()

	return <-errCh
}

func (p *Executor) start() error {
	for {
		if err := p.consumeJobs(); err == io.EOF {
			return p.wp.Close()
		} else if err != nil {
			p.log.Error(err, "error consuming jobs")
		}

		<-time.After(5 * time.Second)
	}
}

func (p *Executor) queueJobs() error {
	p.log.Debugf("queueing jobs")
	var n int
	for {
		job, err := p.iter.Next()
		if err == io.EOF {
			p.log.New(log.Fields{"jobs": n}).Debugf("jobs queued")
			return nil
		}

		if err != nil {
			p.logError(err)
			continue
		}

		qj, err := queue.NewJob()
		if err != nil {
			return err
		}

		if err = qj.Encode(&job); err != nil {
			return err
		}

		if err := p.q.Publish(qj); err != nil {
			return err
		}

		n++
	}
}

func (p *Executor) consumeJobs() error {
	iter, err := p.q.Consume(p.wp.Len())
	if err != nil {
		return err
	}

	for {
		j, err := iter.Next()
		if err == queue.ErrEmptyJob {
			p.logError(err)
			continue
		}

		if err == queue.ErrAlreadyClosed {
			return io.EOF
		}

		if err != nil {
			return err
		}

		if j == nil {
			_ = iter.Close()
			return io.EOF
		}

		var job Job
		if err := j.Decode(&job); err != nil {
			p.logError(err)
			if err := j.Reject(false); err != nil {
				p.logError(err)
			}
		} else {
			p.wp.Do(&WorkerJob{&job, j, p.q})
		}
	}
}

func (p *Executor) logError(err error) {
	p.log.Error(err, "error occurred")
}
