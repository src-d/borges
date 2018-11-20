package tool

import (
	"context"
	"sync"

	log "gopkg.in/src-d/go-log.v0"
)

type worker struct {
	errorChan chan error
	workers   int
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	ctx       context.Context

	skipErrors bool
	dry        bool
}

func (w *worker) Errors(f func(error)) {
	chn := make(chan error)
	go func() {
		for r := range chn {
			f(r)
		}
	}()

	w.errorChan = chn
}

func (w *worker) DefaultErrors(msg string, skipErrors bool) {
	w.Errors(func(err error) {
		log.Errorf(err, msg)
		if !skipErrors {
			w.Cancel()
		}
	})
}

func (w *worker) Dry(d bool) {
	w.dry = d
}

func (w *worker) SkipErrors(e bool) {
	w.skipErrors = e
}

func (w *worker) Workers(n int) {
	w.workers = n
}

func (w *worker) Cancel() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *worker) error(e error) {
	if w.errorChan != nil {
		w.errorChan <- e
	}
}

func (w *worker) setupWorker(
	ctx context.Context,
	f func(c context.Context),
) context.Context {
	n := w.workers
	if n < 1 {
		n = 1
	}

	w.ctx, w.cancel = context.WithCancel(ctx)

	w.wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			f(w.ctx)
			w.wg.Done()
		}()
	}

	return w.ctx
}

func (w *worker) wait() {
	w.wg.Wait()
}
