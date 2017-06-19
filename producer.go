package borges

import (
	"io"
	"sync"
	"time"

	"srcd.works/framework.v0/queue"
)

// Producer is a service to generate jobs and put them to the queue.
type Producer struct {
	Notifiers struct {
		Done       func(*Job, error)
		QueueError func(error)
	}

	jobIter   JobIter
	queue     queue.Queue
	running   bool
	startOnce *sync.Once
	stopOnce  *sync.Once

	// used by Stop() to wait until Start() has finished
	startIsRunning chan struct{}
}

// NewProducer creates a new producer.
func NewProducer(jobIter JobIter, queue queue.Queue) *Producer {
	return &Producer{
		jobIter:   jobIter,
		queue:     queue,
		startOnce: &sync.Once{},
		stopOnce:  &sync.Once{},
	}
}

// IsRunning returns true if the producer is running.
func (p *Producer) IsRunning() bool {
	return p.running
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
	log := log.New("module", "producer")
	p.running = true
	p.startIsRunning = make(chan struct{})
	defer func() { close(p.startIsRunning) }()

	log.Debug("starting")
	for {
		if !p.running {
			break
		}

		j, err := p.jobIter.Next()
		if err == io.EOF {
			break
		}

		if ErrWaitForJobs.Is(err) {
			time.Sleep(time.Millisecond * 500)
			continue
		}

		if err != nil {
			log.Error("error obtaining next job", "err", err)
			p.notifyQueueError(err)
			continue
		}

		err = p.add(j)
		p.notifyDone(j, err)
	}

	log.Debug("stopping")
}

func (p *Producer) add(j *Job) error {
	qj := queue.NewJob()
	if err := qj.Encode(j); err != nil {
		return err
	}

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
		p.notifyQueueError(err)
	}

	p.jobIter = nil
}

func (p *Producer) notifyQueueError(err error) {
	if p.Notifiers.QueueError == nil {
		return
	}

	p.Notifiers.QueueError(err)
}

func (p *Producer) notifyDone(j *Job, err error) {
	if p.Notifiers.Done == nil {
		return
	}

	p.Notifiers.Done(j, err)
}
