package tool

import (
	"context"
	"fmt"
	"io"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-log.v1"
)

const (
	logDeleteCount = 100000
)

// Siva deals with siva files and its database usage.
type Siva struct {
	worker

	db        *Database
	fs        billy.Basic
	bucket    int
	queueChan chan string
}

// NewSiva creates and initializes a new Siva struct.
func NewSiva(db *Database, fs billy.Basic) *Siva {
	return &Siva{
		db: db,
		fs: fs,
	}
}

// Bucket sets the bucket size for siva files.
func (s *Siva) Bucket(b int) {
	s.bucket = b
}

// Queue sets the function used to queue repositories.
func (s *Siva) Queue(req func(string) error) {
	chn := make(chan string)
	go func() {
		for r := range chn {
			err := req(r)
			if err != nil {
				s.error(err)
			}
		}
	}()

	s.queueChan = chn
}

// WriteQueue sets a default function that writes to writer each repository
// id that has to be queued.
func (s *Siva) WriteQueue(writer io.Writer) {
	s.Queue(func(s string) error {
		_, err := fmt.Fprintln(writer, s)
		return err
	})
}

func (s *Siva) deleteWorker(ctx context.Context, c chan string) {
	for init := range c {
		err := s.DeleteOne(ctx, init)
		if err != nil {
			s.error(err)
		}
	}
}

// Delete removes all references to the siva files in a list, deletes its
// siva file and calls queue function for its repositories.
func (s *Siva) Delete(ctx context.Context, list []string) error {
	chn := make(chan string)
	wctx := s.setupWorker(ctx, func(c context.Context) {
		s.deleteWorker(c, chn)
	})

	for i, h := range list {
		if i != 0 && i%logDeleteCount == 0 {
			log.With(log.Fields{"count": i}).Infof("deleting sivas")
		}

		select {
		case <-wctx.Done():
			return wctx.Err()
		default:
			chn <- h
		}
	}

	close(chn)
	s.wait()

	return nil
}

// DeleteOne deletes one siva file, its references and calls queue function
// for the related repositories.
func (s *Siva) DeleteOne(ctx context.Context, init string) error {
	if s.db != nil {
		err := s.deleteDatabase(ctx, init)
		if err != nil {
			return err
		}
	}

	if s.fs != nil {
		err := s.deleteFilesystem(ctx, init)
		if err != nil {
			return err
		}
	}

	return nil
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
