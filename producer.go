package borges

import (
	"sync"
	"time"

	"srcd.works/framework.v0/queue"
)

// Producer is a service to generate jobs and put them to the queue.
type Producer struct {
	Notifiers struct {
		Done func(*Job, error)
	}

	queue     queue.Queue
	running   bool
	startOnce *sync.Once
	stopOnce  *sync.Once
	wg        *sync.WaitGroup
}

// NewProducer creates a new producer.
func NewProducer(queue queue.Queue) *Producer {
	return &Producer{
		queue:     queue,
		startOnce: &sync.Once{},
		stopOnce:  &sync.Once{},
		wg:        &sync.WaitGroup{},
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
	p.running = true
	p.wg.Add(1)
	defer p.wg.Done()
	for {
		if !p.running {
			break
		}

		j, err := p.next()
		if err != nil {
			//TODO: error handling
			continue
		}

		err = p.add(j)
		p.notifyDone(j, err)
	}
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
	p.wg.Wait()
}

var n uint64

func (p *Producer) next() (*Job, error) {
	//TODO: Add logic.
	n++
	time.Sleep(time.Millisecond * 500)
	return &Job{RepositoryID: n}, nil
}

func (p *Producer) notifyDone(j *Job, err error) {
	if p.Notifiers.Done == nil {
		return
	}

	p.Notifiers.Done(j, err)
}
