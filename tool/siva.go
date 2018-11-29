package tool

import (
	"context"
	"fmt"
	"io"
	"time"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-log.v1"
)

const (
	logDeleteCount   = 10000
	logRebucketCount = 100000
)

// Siva deals with siva files and its database usage.
type Siva struct {
	worker

	db         *Database
	fs         billy.Basic
	bucket     int
	queueChan  chan string
	failedChan chan string
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

// Failed sets the function used to save failed jobs.
func (s *Siva) Failed(req func(string) error) {
	chn := make(chan string)
	go func() {
		for r := range chn {
			err := req(r)
			if err != nil {
				s.error(err)
			}
		}
	}()

	s.failedChan = chn
}

// WriteFailed sets a default function that writes to writer each failed job.
func (s *Siva) WriteFailed(writer io.Writer) {
	s.Failed(func(s string) error {
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
			log.With(log.Fields{
				"count": i,
				"siva":  h,
			}).Infof("deleting sivas")
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

	if !s.dry {
		return s.db.DeleteReferences(ctx, init)
	}

	return nil
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

func (s *Siva) rebucketWorker(ctx context.Context, to int, c chan string) {
	for init := range c {
		err := s.RebucketOne(ctx, init, to)
		if err != nil {
			s.error(err)
		}
	}
}

// Rebucket changes bucketing level from a list of siva files.
func (s *Siva) Rebucket(ctx context.Context, list []string, to int) error {
	if to == s.bucket {
		return nil
	}

	chn := make(chan string)
	wctx := s.setupWorker(ctx, func(c context.Context) {
		s.rebucketWorker(c, to, chn)
	})

	for i, h := range list {
		if i != 0 && i%logRebucketCount == 0 {
			log.With(log.Fields{
				"count": i,
				"siva":  h,
			}).Infof("changing siva bucket level")
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

// RebucketOne changes one siva file bucketing level.
func (s *Siva) RebucketOne(ctx context.Context, f string, to int) error {
	max := s.bucket
	if to > max {
		max = to
	}
	if len(f)-1 < max {
		// not enough characters to create bucket directory
		s.failed(f)
		return fmt.Errorf("siva file name too small for bucketing level: %s", f)
	}

	siva := fmt.Sprintf("%s.siva", f)

	a := bucketPath(siva, s.bucket)
	b := bucketPath(siva, to)

	l := log.With(log.Fields{
		"from": a,
		"to":   b,
	})
	start := time.Now()

	if !s.dry {
		err := s.fs.Rename(a, b)
		if err != nil {
			l.With(log.Fields{
				"duration": time.Since(start),
			}).Errorf(err, "moving siva file")

			s.failed(f)
			return err
		}
	}

	l.With(log.Fields{
		"duration": time.Since(start),
	}).Debugf("moved siva file")

	return nil
}

func (s *Siva) failed(job string) {
	if s.failedChan != nil {
		s.failedChan <- job
	}
}
