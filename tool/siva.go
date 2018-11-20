package tool

import (
	"context"
	"fmt"
	"io"
	"sync"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-log.v1"
)

const (
	logDeleteCount = 100000
)

type Siva struct {
	db        *Database
	fs        billy.Basic
	bucket    int
	queueChan chan string
	errorChan chan error
	workers   int
	cancel    context.CancelFunc

	skipErrors bool
	dry        bool
}

func NewSiva(db *Database, fs billy.Basic) *Siva {
	return &Siva{
		db: db,
		fs: fs,
	}
}

func (s *Siva) Bucket(b int) *Siva {
	s.bucket = b
	return s
}

func (s *Siva) Queue(req func(string) error) *Siva {
	chn := make(chan string)
	go func() {
		for r := range chn {
			err := req(r)
			if err != nil {
				s.errorChan <- err
			}
		}
	}()

	s.queueChan = chn
	return s
}

func (s *Siva) WriteQueue(writer io.Writer) *Siva {
	return s.Queue(func(s string) error {
		_, err := fmt.Fprintln(writer, s)
		return err
	})
}

func (s *Siva) Errors(f func(error)) *Siva {
	chn := make(chan error)
	go func() {
		for r := range chn {
			f(r)
		}
	}()

	s.errorChan = chn
	return s
}

func (s *Siva) DefaultErrors(msg string, skipErrors bool) *Siva {
	return s.Errors(func(err error) {
		log.Errorf(err, msg)
		if !skipErrors {
			s.Cancel()
		}
	})
}

func (s *Siva) Dry(d bool) *Siva {
	s.dry = d
	return s
}

func (s *Siva) SkipErrors(e bool) *Siva {
	s.skipErrors = e
	return s
}

func (s *Siva) Workers(w int) *Siva {
	s.workers = w
	return s
}

func (s *Siva) Cancel() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Siva) deleteWorker(ctx context.Context, c chan string) {
	for init := range c {
		s.DeleteSiva(ctx, init)
	}
}

func (s *Siva) Delete(ctx context.Context, list []string) error {
	w := s.workers
	if w < 1 {
		w = 1
	}

	ctx, s.cancel = context.WithCancel(ctx)

	var wg sync.WaitGroup
	chn := make(chan string)
	wg.Add(w)

	for i := 0; i < w; i++ {
		go func() {
			s.deleteWorker(ctx, chn)
			wg.Done()
		}()
	}

	for i, h := range list {
		if i != 0 && i%logDeleteCount == 0 {
			log.With(log.Fields{"count": i}).Infof("deleting sivas")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			chn <- h
		}
	}

	close(chn)
	wg.Wait()

	return nil
}

func (s *Siva) DeleteSiva(ctx context.Context, init string) {
	if s.db != nil {
		err := s.deleteDatabase(ctx, init)
		if err != nil {
			s.error(err)
		}
	}

	if s.fs != nil {
		err := s.deleteFilesystem(ctx, init)
		if err != nil {
			s.error(err)
		}
	}
}

func (s *Siva) deleteDatabase(ctx context.Context, init string) error {
	repos, err := s.db.RepositoriesWithInit(init)
	if err != nil {
		return err
	}

	log.With(log.Fields{
		"siva":  init,
		"repos": len(repos),
	}).Debugf("queuing repositories")

	for _, r := range repos {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			s.queue(r)
		}
	}

	return s.db.DeleteReferences(ctx, init)
}

func (s *Siva) deleteFilesystem(ctx context.Context, init string) error {
	f := fmt.Sprintf("%s.siva", bucketPath(init, s.bucket))
	log.With(log.Fields{"file": f}).Debugf("deleting siva file")

	if !s.dry {
		err := s.fs.Remove(f)
		if err != nil {
			s.error(err)
		}
	}

	return nil
}

func (s *Siva) queue(repo string) {
	if s.queueChan != nil {
		s.queueChan <- repo
	}
}

func (s *Siva) error(e error) {
	if s.errorChan != nil {
		s.errorChan <- e
	}
}
