package borges

import (
	"github.com/inconshreveable/log15"
)

// Worker is a worker that processes jobs from a channel.
type Worker struct {
	log        log15.Logger
	do         func(log15.Logger, *Job) error
	jobChannel chan *WorkerJob
	quit       chan struct{}
	running    bool
}

// NewWorker creates a new Worker. The first parameter is a WorkerContext that
// will be passed to the processing function on every call. The second parameter
// is the processing function itself that will be called for every job. The
// third parameter is a channel that the worker will consume jobs from.
func NewWorker(log log15.Logger, do func(log15.Logger, *Job) error, ch chan *WorkerJob) *Worker {
	return &Worker{
		log:        log,
		do:         do,
		jobChannel: ch,
		quit:       make(chan struct{}),
	}
}

// Start processes jobs from the input channel until it is stopped. Start blocks
// until the worker is stopped or the channel is closed.
func (w *Worker) Start() {
	log := w.log

	w.running = true
	defer func() { w.running = false }()

	log.Info("starting")
	for {
		select {
		case job, ok := <-w.jobChannel:
			if !ok {
				return
			}

			if err := w.do(log, job.Job); err != nil {
				if err := job.Reject(false); err != nil {
					log.Error("error rejecting job", "error", err)
				}

				log.Error("error on job", "error", err)

				continue
			}

			if err := job.Ack(); err != nil {
				log.Error("error acking job", "error", err)
			}
		case <-w.quit:
			return
		}
	}
}

// Stop stops the worker. It blocks until it is actually stopped. If it is
// currently processing a job, it will finish before stopping.
func (w *Worker) Stop() {
	w.quit <- struct{}{}
}

// IsRunning returns true if the worker is running.
func (w *Worker) IsRunning() bool {
	return w.running
}
