package borges

import (
	"io"
	"sync"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/src-d/borges/metrics"
	"gopkg.in/src-d/framework.v0/queue"
)

// Producer is a service to generate jobs and put them to the queue.
type Producer struct {
	log       log15.Logger
	jobIter   JobIter
	queue     queue.Queue
	running   bool
	startOnce *sync.Once
	stopOnce  *sync.Once
	priority  queue.Priority

	// used by Stop() to wait until Start() has finished
	startIsRunning chan struct{}
}

// NewProducer creates a new producer.
func NewProducer(
	log log15.Logger,
	jobIter JobIter,
	queue queue.Queue,
	prio queue.Priority,
) *Producer {
	return &Producer{
		log:       log.New("mode", "producer"),
		jobIter:   jobIter,
		queue:     queue,
		startOnce: &sync.Once{},
		stopOnce:  &sync.Once{},
		priority:  prio,
	}
}

// Start starts the producer services. It blocks until Stop is called.
func (p *Producer) Start() {
	p.startOnce.Do(p.start)
}

// Stop stops the producer.
func (p *Producer) Stop() {
	p.stopOnce.Do(p.stop)
}

func (p *Producer) start() {
	log := p.log
	log.Info("starting up")

	p.running = true
	p.startIsRunning = make(chan struct{})
	defer func() { close(p.startIsRunning) }()

	for {
		if !p.running {
			break
		}

		j, err := p.jobIter.Next()
		if err == io.EOF {
			log.Info("no more jobs in the queue")
			break
		}

		if ErrWaitForJobs.Is(err) {
			time.Sleep(time.Millisecond * 500)
			continue
		}

		if err != nil {
			log.Error("error obtaining next job", "error", err)
			continue
		}

		if err := p.add(j); err != nil {
			metrics.RepoProduceFailed()
			log.Error("error adding job to the queue", "job", j.RepositoryID, "error", err)
		} else {
			metrics.RepoProduced()
			log.Info("job queued", "job", j.RepositoryID)
		}
	}

	log.Info("stopping")
}

func (p *Producer) add(j *Job) error {
	qj, err := queue.NewJob()
	qj.Retries = maxJobRetries
	if err != nil {
		return err
	}

	if err := qj.Encode(j); err != nil {
		return err
	}

	qj.SetPriority(p.priority)

	return p.queue.Publish(qj)
}

func (p *Producer) stop() {
	p.running = false
	p.closeIter()
	<-p.startIsRunning
}

func (p *Producer) closeIter() {
	if p.jobIter == nil {
		return
	}

	if err := p.jobIter.Close(); err != nil {
		p.log.Error("error closing queue iterator", "error", err)
	}

	p.jobIter = nil
}
