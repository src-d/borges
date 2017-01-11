package borges

// Worker is a worker that processes jobs from a channel.
type Worker struct {
	ctx        *WorkerContext
	do         func(*WorkerContext, *Job) error
	jobChannel chan *WorkerJob
	quit       chan struct{}
	running    bool
}

// NewWorker creates a new Worker. The first parameter is a WorkerContext that
// will be passed to the processing function on every call. The second parameter
// is the processing function itself that will be called for every job. The
// third parameter is a channel that the worker will consume jobs from.
func NewWorker(ctx *WorkerContext, do func(*WorkerContext, *Job) error, ch chan *WorkerJob) *Worker {
	return &Worker{
		ctx:        ctx,
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

	for {
		select {
		case job, ok := <-w.jobChannel:
			if !ok {
				return
			}

			if err := w.do(w.ctx, job.Job); err != nil {
				if err := job.Reject(true); err != nil {
					//TODO: do something about this
				}
				continue
			}

			if err := job.Ack(); err != nil {
				//TODO: do something about this
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
