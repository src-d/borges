package borges

import (
	"github.com/sirupsen/logrus"
)

const TemporaryError = "temporary"

// Worker is a worker that processes jobs from a channel.
type Worker struct {
	logentry   *logrus.Entry
	do         func(*logrus.Entry, *Job) error
	jobChannel chan *WorkerJob
	quit       chan struct{}
	running    bool
}

// NewWorker creates a new Worker. The first parameter is a WorkerContext that
// will be passed to the processing function on every call. The second parameter
// is the processing function itself that will be called for every job. The
// third parameter is a channel that the worker will consume jobs from.
func NewWorker(logentry *logrus.Entry, do func(*logrus.Entry, *Job) error, ch chan *WorkerJob) *Worker {
	return &Worker{
		logentry:   logentry,
		do:         do,
		jobChannel: ch,
		quit:       make(chan struct{}),
	}
}

// Start processes jobs from the input channel until it is stopped. Start blocks
// until the worker is stopped or the channel is closed.
func (w *Worker) Start() {
	w.running = true
	defer func() { w.running = false }()

	log := w.logentry
	log.Info("starting")
	for {
		select {
		case job, ok := <-w.jobChannel:
			if !ok {
				return
			}

			var requeue bool
			if err := w.do(log, job.Job); err != nil {
				// when a previous job which failed with a temporary error
				// panics, it's sent to the buried queue without the retries
				// header or with this header set to a value greater than zero.
				if ErrFatal.Is(err) || job.queueJob.Retries == 0 {
					if err := job.queueJob.Reject(false); err != nil {
						log.WithField("error", err).Error("error rejecting job")
					}

					log.WithField("error", err).Error("error on job")
					continue
				}

				requeue = true
			}

			if requeue {
				job.queueJob.Retries--
				job.queueJob.ErrorType = TemporaryError
				if err := job.source.Publish(job.queueJob); err != nil {
					log.Error("error publishing job back to the main queue", "error", err)
					if err := job.queueJob.Reject(false); err != nil {
						log.Error("error rejecting job", "error", err)
					}

					continue
				}
			}

			if err := job.queueJob.Ack(); err != nil {
				log.WithField("error", err).Error("error acking job")
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
