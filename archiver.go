package borges

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/src-d/borges/lock"
	"github.com/src-d/borges/metrics"

	"github.com/jpillora/backoff"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/core-retrieval.v0/repository"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	"gopkg.in/src-d/go-kallax.v1"
)

var (
	ErrCleanRepositoryDir     = errors.NewKind("cleaning up local repo dir failed")
	ErrClone                  = errors.NewKind("cloning %s failed")
	ErrPushToRootedRepository = errors.NewKind("push to rooted repo %s failed")
	ErrArchivingRoots         = errors.NewKind("archiving %d out of %d roots failed: %s")
	ErrEndpointsEmpty         = errors.NewKind("endpoints is empty")
	ErrRepositoryIDNotFound   = errors.NewKind("repository id not found: %s")
	ErrChanges                = errors.NewKind("error computing changes")
	ErrAlreadyFetching        = errors.NewKind("repository %s was already in a fetching status")
	ErrSetStatus              = errors.NewKind("unable to set repository to status: %s")
	ErrFatal                  = errors.NewKind("fatal, %v: stacktrace: %s")
)

// Archiver archives repositories. Archiver instances are thread-safe and can
// be reused.
//
// See borges documentation for more details about the archiving rules.
type Archiver struct {
	log     *logrus.Entry
	backoff *backoff.Backoff

	// TemporaryCloner is used to clone repositories into temporary storage.
	TemporaryCloner TemporaryCloner
	// Timeout is the deadline to cancel a job.
	Timeout time.Duration
	// Store is the component where repository models are stored.
	Store RepositoryStore
	// RootedTransactioner is used to push new references to our repository
	// storage.
	RootedTransactioner repository.RootedTransactioner
	// LockSession is a locker service to prevent concurrent access to the same
	// rooted reporitories.
	LockSession lock.Session
}

func NewArchiver(log *logrus.Entry, r RepositoryStore, tx repository.RootedTransactioner, tc TemporaryCloner,
	ls lock.Session, to time.Duration) *Archiver {
	return &Archiver{
		log:                 log,
		backoff:             newBackoff(),
		TemporaryCloner:     tc,
		Timeout:             to,
		Store:               r,
		RootedTransactioner: tx,
		LockSession:         ls,
	}
}

const maxRetries = 5

func newBackoff() *backoff.Backoff {
	const (
		minDuration = 100 * time.Millisecond
		maxDuration = 30 * time.Second
		factor      = 4
	)

	return &backoff.Backoff{
		Min:    minDuration,
		Max:    maxDuration,
		Factor: factor,
		Jitter: true,
	}
}

// Do archives a repository according to a job.
func (a *Archiver) Do(ctx context.Context, j *Job) error {
	log := a.log.WithField("job", j.RepositoryID)
	log.Info("job started")
	if err := a.do(ctx, log, j); err != nil {
		log.WithField("error", err).Error("job finished with error")
		return err
	}

	log.Info("job finished successfully")
	return nil
}

func (a *Archiver) do(ctx context.Context, log *logrus.Entry, j *Job) (err error) {
	now := time.Now()
	ctx, cancel := context.WithTimeout(ctx, a.Timeout)
	defer cancel()

	r, err := a.getRepositoryModel(j)
	if err != nil {
		return err
	}

	defer func() {
		switch r.Status {
		case model.Fetched:
			metrics.RepoProcessed(time.Since(now))
		case model.NotFound:
			metrics.RepoNotFound()
		case model.AuthRequired:
			metrics.RepoAuthRequired()
		default:
			metrics.RepoFailed()
		}
	}()

	log.WithFields(logrus.Fields{
		"status":     r.Status,
		"last-fetch": r.FetchedAt,
		"references": len(r.References),
	}).Debug("repository model obtained")

	defer func() {
		if rcv := recover(); rcv != nil {
			log.WithField("error", rcv).Error("panic while processing repository")

			r.FetchErrorAt = &now
			if sErr := a.Store.UpdateFailed(r, model.Pending); sErr != nil {
				log.WithFields(logrus.Fields{
					"err": err,
					"id":  r.ID,
				}).Error("error setting repo as failed")
			}

			err = ErrFatal.New(rcv, debug.Stack())
		}
	}()

	if err := a.canProcessRepository(r, &now); err != nil {
		log.WithFields(logrus.Fields{
			"id":         r.ID.String(),
			"last-fetch": r.FetchedAt,
			"reason":     err,
		}).Warn("cannot process repository")

		return err
	}

	if err := a.Store.SetStatus(r, model.Fetching); err != nil {
		return ErrSetStatus.Wrap(err, model.Fetching)
	}

	endpoint, err := selectEndpoint(r.Endpoints)
	if err != nil {
		if err := a.Store.UpdateFailed(r, model.Pending); err != nil {
			log.WithFields(logrus.Fields{
				"id": r.ID, "err": err,
			}).Error("error setting repo as failed")
		}
		return err
	}

	log = log.WithField("endpoint", endpoint)

	gr, err := a.TemporaryCloner.Clone(
		ctx,
		j.RepositoryID.String(),
		endpoint)
	if err != nil {
		var finalErr error
		if err != transport.ErrEmptyUploadPackRequest {
			r.FetchErrorAt = &now
			finalErr = ErrClone.Wrap(err, endpoint)
		}

		status := model.Pending
		if err == transport.ErrRepositoryNotFound {
			status = model.NotFound
			finalErr = nil
		} else if err == transport.ErrAuthenticationRequired {
			status = model.AuthRequired
			finalErr = nil
		}

		if err := a.Store.UpdateFailed(r, status); err != nil {
			return err
		}

		log.WithField("error", err).Error("error cloning repository")
		return finalErr
	}

	defer func() {
		if cErr := gr.Close(); cErr != nil && err == nil {
			err = ErrCleanRepositoryDir.Wrap(cErr)
		}
	}()
	log.Debug("remote repository cloned")

	oldRefs := NewModelReferencer(r)
	newRefs := gr
	changes, err := NewChanges(oldRefs, newRefs)
	if err != nil {
		log.WithField("error", err).Error("error computing changes")
		if err := a.Store.UpdateFailed(r, model.Pending); err != nil {
			log.WithFields(logrus.Fields{"err": err, "id": r.ID}).Error("error setting repo as failed")
		}

		return ErrChanges.Wrap(err)
	}

	log.WithField("roots", len(changes)).Debug("changes obtained")
	if err := a.pushChangesToRootedRepositories(ctx, log, j, r, gr, changes, now); err != nil {
		log.WithField("error", err).Error("repository processed with errors")

		r.FetchErrorAt = &now
		if updateErr := a.Store.UpdateFailed(r, model.Pending); updateErr != nil {
			return ErrSetStatus.Wrap(updateErr, model.Pending)
		}

		return err
	}

	log.Debug("repository processed")
	return nil
}

func (a *Archiver) canProcessRepository(repo *model.Repository, now *time.Time) (err error) {
	defer func() {
		if err != nil {
			repo.FetchErrorAt = now
			if updateErr := a.Store.UpdateFailed(repo, model.Pending); updateErr != nil {
				err = ErrSetStatus.Wrap(updateErr, model.Pending)
			}
		}
	}()

	if repo.Status == model.Fetching {
		err = ErrAlreadyFetching.New(repo.ID)
	}

	return
}

func (a *Archiver) getRepositoryModel(j *Job) (*model.Repository, error) {
	r, err := a.Store.Get(kallax.ULID(j.RepositoryID))
	if err != nil {
		return nil, ErrRepositoryIDNotFound.Wrap(err, j.RepositoryID.String())
	}

	return r, nil
}

var endpointsOrder = []string{"git://", "https://", "http://"}

func selectEndpoint(endpoints []string) (string, error) {
	if len(endpoints) == 0 {
		return "", ErrEndpointsEmpty.New()
	}

	for _, epo := range endpointsOrder {
		for _, ep := range endpoints {
			if strings.HasPrefix(ep, epo) {
				return ep, nil
			}
		}
	}

	return endpoints[0], nil
}

func (a *Archiver) pushChangesToRootedRepositories(ctx context.Context, ctxLog *logrus.Entry,
	j *Job, r *model.Repository, tr TemporaryRepository, changes Changes,
	now time.Time) error {

	var failedInits []model.SHA1
	for ic, cs := range changes {
		log := ctxLog.WithField("root", ic.String())
		lock := a.LockSession.NewLocker(fmt.Sprintf("borges/%s", ic.String()))
		ch, err := lock.Lock()
		if err != nil {
			failedInits = append(failedInits, ic)
			log.WithFields(logrus.Fields{"root": ic.String(), "error": err}).Warn("failed to acquire lock")
			continue
		}

		log.Debug("push changes to rooted repository started")
		if err := a.pushChangesToRootedRepository(ctx, ctxLog, r, tr, ic, cs); err != nil {
			err = ErrPushToRootedRepository.Wrap(err, ic.String())
			log.WithField("error", err).Error("error pushing changes to rooted repository")
			failedInits = append(failedInits, ic)
			if err := lock.Unlock(); err != nil {
				log.WithFields(logrus.Fields{"root": ic.String(), "error": err}).Warn("failed to release lock")
			}

			continue
		}
		log.Debug("push changes to rooted repository finished")

		log.Debug("update repository references started")
		r.References = updateRepositoryReferences(r.References, cs, ic)
		for _, ref := range r.References {
			ref.Repository = r
		}

		if err := a.Store.UpdateFetched(r, now); err != nil {
			err = ErrPushToRootedRepository.Wrap(err, ic.String())
			log.WithField("error", err).Error("error updating repository in database")
			failedInits = append(failedInits, ic)
		}
		log.Debug("update repository references finished")

		select {
		case <-ch:
			log.WithField("root", ic.String()).Error("lost the lock")
		default:
		}

		if err := lock.Unlock(); err != nil {
			log.WithFields(logrus.Fields{"root": ic.String(), "error": err}).Warn("failed to release lock")
		}
	}

	if len(changes) == 0 {
		if err := a.Store.UpdateFetched(r, now); err != nil {
			ctxLog.WithField("error", err).Error("error updating repository in database")
		}
	}

	return checkFailedInits(changes, failedInits)
}

func (a *Archiver) pushChangesToRootedRepository(ctx context.Context, log *logrus.Entry, r *model.Repository, tr TemporaryRepository, ic model.SHA1, changes []*Command) error {
	var rootedRepoCpStart = time.Now()
	tx, err := a.beginTxWithRetries(ctx, log, plumbing.Hash(ic), maxRetries)
	sivaCpFromDuration := time.Now().Sub(rootedRepoCpStart)
	log.WithFields(logrus.Fields{
		"RootedRepository": ic,
		"copyFromRemote":   int64(sivaCpFromDuration / time.Second),
	}).Debug("Copy siva file from FS.")

	if err != nil {
		return err
	}

	rr, err := git.Open(tx.Storer(), nil)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return withInProcRepository(ic, rr, func(url string) error {
		if err := StoreConfig(rr, r); err != nil {
			_ = tx.Rollback()
			return err
		}

		refspecs := a.changesToPushRefSpec(r.ID, changes)
		pushStart := time.Now()
		if err := tr.Push(ctx, url, refspecs); err != nil {
			onlyPushDurationSec := int64(time.Now().Sub(pushStart) / time.Second)
			log.WithFields(logrus.Fields{"refs": refspecs, "error": err, "took": onlyPushDurationSec}).Error("error pushing 1 change for")
			_ = tx.Rollback()
			return err
		}
		onlyPushDurationSec := int64(time.Now().Sub(pushStart) / time.Second)
		log.WithField("took", onlyPushDurationSec).Debug("1 change pushed")

		var rootedRepoCpStart = time.Now()
		err = a.commitTxWithRetries(ctx, log, ic, tx, maxRetries)
		sivaCpToDuration := time.Now().Sub(rootedRepoCpStart)
		log.WithFields(logrus.Fields{
			"RootedRepository": ic,
			"copyToRemote":     int64(sivaCpToDuration / time.Second),
		}).Debug("Copy siva file to FS")
		return err
	})
}

func (a *Archiver) beginTxWithRetries(
	ctx context.Context,
	log *logrus.Entry,
	initCommit plumbing.Hash,
	numRetries float64,
) (tx repository.Tx, err error) {
	for a.backoff.Attempt() < numRetries {
		tx, err = a.RootedTransactioner.Begin(ctx, initCommit)
		if err == nil || !repository.HDFSNamenodeError.Is(err) {
			break
		}

		tts := a.backoff.Duration()
		log.WithFields(logrus.Fields{
			"RootedRepository": initCommit,
			"tx":               "begin",
			"wait":             tts,
			"error":            err,
		}).Error("Waiting for HDFS reconnection")
		time.Sleep(tts)
	}

	a.backoff.Reset()
	return
}

func (a *Archiver) commitTxWithRetries(
	ctx context.Context,
	log *logrus.Entry,
	initCommit model.SHA1,
	tx repository.Tx,
	numRetries float64,
) (err error) {
	for a.backoff.Attempt() < numRetries {
		err = tx.Commit(ctx)
		if err == nil || !repository.HDFSNamenodeError.Is(err) {
			break
		}

		tts := a.backoff.Duration()
		log.WithFields(logrus.Fields{
			"RootedRepository": initCommit,
			"tx":               "commit",
			"wait":             tts,
			"error":            err,
		}).Error("Waiting for HDFS reconnection")
		time.Sleep(tts)
	}

	a.backoff.Reset()
	return
}

func (a *Archiver) changesToPushRefSpec(id kallax.ULID, changes []*Command) []config.RefSpec {
	var rss []config.RefSpec
	for _, ch := range changes {
		var rs string
		switch ch.Action() {
		case Create, Update:
			rs = fmt.Sprintf("+%s:%s/%s", ch.New.Name, ch.New.Name, id)
		case Delete:
			rs = fmt.Sprintf(":%s/%s", ch.Old.Name, id)
		default:
			panic("not reachable")
		}

		rss = append(rss, config.RefSpec(rs))
	}

	return rss
}

// Applies all given changes to a slice of References
func updateRepositoryReferences(oldRefs []*model.Reference, commands []*Command, ic model.SHA1) []*model.Reference {
	rbn := refsByName(oldRefs)
	for _, com := range commands {
		switch com.Action() {
		case Delete:
			ref, ok := rbn[com.Old.Name]
			if !ok {
				continue
			}

			if com.Old.Init == ref.Init {
				delete(rbn, com.Old.Name)
			}
		case Create:
			rbn[com.New.Name] = com.New
		case Update:
			oldRef, ok := rbn[com.New.Name]
			if !ok {
				continue
			}

			if oldRef.Init == com.Old.Init {
				rbn[com.New.Name] = com.New
			}
		}
	}

	// Add the references that keep equals
	var result []*model.Reference
	for _, r := range rbn {
		result = append(result, r)
	}

	return result
}

func checkFailedInits(changes Changes, failed []model.SHA1) error {
	n := len(failed)
	if n == 0 {
		return nil
	}

	strs := make([]string, n)
	for i := 0; i < n; i++ {
		strs[i] = failed[i].String()
	}

	return ErrArchivingRoots.New(
		n,
		len(changes),
		strings.Join(strs, ", "),
	)
}

// NewArchiverWorkerPool creates a new WorkerPool that uses an Archiver to
// process jobs. It takes optional start, stop and warn notifier functions that
// are equal to the Archiver notifiers but with additional WorkerContext.
func NewArchiverWorkerPool(
	log *logrus.Entry,
	r RepositoryStore, tx repository.RootedTransactioner,
	tc TemporaryCloner,
	ls lock.Service,
	to time.Duration) *WorkerPool {
	do := func(ctx context.Context, logentry *logrus.Entry, j *Job) error {
		lsess, err := ls.NewSession(&lock.SessionConfig{TTL: 10 * time.Second})
		if err != nil {
			return err
		}

		defer func() {
			err := lsess.Close()
			if err != nil {
				logrus.Error("error closing locking session", "error", err)
			}
		}()

		a := NewArchiver(log, r, tx, tc, lsess, to)
		return a.Do(ctx, j)
	}

	return NewWorkerPool(log, do)
}
