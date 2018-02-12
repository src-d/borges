package borges

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/jpillora/backoff"
	"github.com/src-d/borges/metrics"
	"github.com/src-d/borges/storage"
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/core-retrieval.v0/repository"
	"gopkg.in/src-d/framework.v0/lock"
	"gopkg.in/src-d/go-errors.v0"
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
)

// Archiver archives repositories. Archiver instances are thread-safe and can
// be reused.
//
// See borges documentation for more details about the archiving rules.
type Archiver struct {
	log log15.Logger

	backoff *backoff.Backoff

	// TemporaryCloner is used to clone repositories into temporary storage.
	TemporaryCloner TemporaryCloner

	// Timeout is the deadline to cancel a job.
	Timeout time.Duration

	// Store is the component where repository models are stored.
	Store storage.RepoStore

	// RootedTransactioner is used to push new references to our repository
	// storage.
	RootedTransactioner repository.RootedTransactioner

	// LockSession is a locker service to prevent concurrent access to the same
	// rooted reporitories.
	LockSession lock.Session
}

func NewArchiver(log log15.Logger, r storage.RepoStore,
	tx repository.RootedTransactioner, tc TemporaryCloner,
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
func (a *Archiver) Do(j *Job) error {
	log := a.log.New("job", j.RepositoryID)
	log.Info("job started")
	if err := a.do(log, j); err != nil {
		log.Error("job finished with error", "error", err)
		return err
	}

	log.Info("job finished successfully")
	return nil
}

func (a *Archiver) do(log log15.Logger, j *Job) (err error) {
	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), a.Timeout)
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
		case model.Private:
			metrics.RepoPrivate()
		default:
			metrics.RepoFailed()
		}
	}()

	log.Debug("repository model obtained",
		"status", r.Status,
		"last-fetch", r.FetchedAt,
		"references", len(r.References))

	if err := a.canProcessRepository(r); err != nil {
		log.Warn("cannot process repository",
			"id", r.ID.String(),
			"last-fetch", r.FetchedAt,
			"reason", err,
		)

		metrics.RepoSkipped()
		return err
	}

	if err := a.Store.SetStatus(r, model.Fetching); err != nil {
		return ErrSetStatus.Wrap(err, model.Fetching)
	}

	endpoint, err := selectEndpoint(r.Endpoints)
	if err != nil {
		if err := a.Store.UpdateFailed(r, model.Pending); err != nil {
			log15.Error("error setting repo as failed", "id", r.ID, "err", err)
		}
		return err
	}

	log = log.New("endpoint", endpoint)

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
			status = model.Private
			finalErr = nil
		}

		if err := a.Store.UpdateFailed(r, status); err != nil {
			return err
		}

		log.Error("error cloning repository", "error", err)
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
		log.Error("error computing changes", "error", err)
		if err := a.Store.UpdateFailed(r, model.Pending); err != nil {
			log15.Error("error setting repo as failed", "id", r.ID, "err", err)
		}

		return ErrChanges.Wrap(err)
	}

	log.Debug("changes obtained", "roots", len(changes))
	if err := a.pushChangesToRootedRepositories(ctx, log, j, r, gr, changes, now); err != nil {
		log.Error("repository processed with errors", "error", err)

		r.FetchErrorAt = &now
		if updateErr := a.Store.UpdateFailed(r, model.Pending); updateErr != nil {
			return ErrSetStatus.Wrap(updateErr, model.Pending)
		}

		return err
	}

	log.Debug("repository processed")
	return nil
}

func (a *Archiver) canProcessRepository(repo *model.Repository) error {
	if repo.Status == model.Fetching {
		return ErrAlreadyFetching.New(repo.ID)
	}

	return nil
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

func (a *Archiver) pushChangesToRootedRepositories(ctx context.Context, ctxLog log15.Logger,
	j *Job, r *model.Repository, tr TemporaryRepository, changes Changes,
	now time.Time) error {

	var failedInits []model.SHA1
	for ic, cs := range changes {
		log := ctxLog.New("root", ic.String())
		lock := a.LockSession.NewLocker(fmt.Sprintf("borges/%s", ic.String()))
		ch, err := lock.Lock()
		if err != nil {
			failedInits = append(failedInits, ic)
			log.Warn("failed to acquire lock", "root", ic.String(), "error", err)
			continue
		}

		log.Debug("push changes to rooted repository started")
		if err := a.pushChangesToRootedRepository(ctx, log, r, tr, ic, cs); err != nil {
			err = ErrPushToRootedRepository.Wrap(err, ic.String())
			log.Error("error pushing changes to rooted repository", "error", err)
			failedInits = append(failedInits, ic)
			if err := lock.Unlock(); err != nil {
				log.Warn("failed to release lock", "root", ic.String(), "error", err)
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
			log.Error("error updating repository in database", "error", err)
			failedInits = append(failedInits, ic)
		}
		log.Debug("update repository references finished")

		select {
		case <-ch:
			log.Error("lost the lock", "root", ic.String())
		default:
		}

		if err := lock.Unlock(); err != nil {
			log.Warn("failed to release lock", "root", ic.String(), "error", err)
		}
	}

	if len(changes) == 0 {
		if err := a.Store.UpdateFetched(r, now); err != nil {
			ctxLog.Error("error updating repository in database", "error", err)
		}
	}

	return checkFailedInits(changes, failedInits)
}

func (a *Archiver) pushChangesToRootedRepository(ctx context.Context, log log15.Logger, r *model.Repository, tr TemporaryRepository, ic model.SHA1, changes []*Command) error {
	var rootedRepoCpStart = time.Now()
	tx, err := a.beginTxWithRetries(log, plumbing.Hash(ic), maxRetries)
	sivaCpFromDuration := time.Now().Sub(rootedRepoCpStart)
	log.Debug("Copy siva file from HDFS", "RootedRepository", ic, "copyFromRemote", int64(sivaCpFromDuration/time.Second))
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
			log.Error("error pushing 1 change for", "refs", refspecs, "error", err, "took", onlyPushDurationSec)
			_ = tx.Rollback()
			return err
		}
		onlyPushDurationSec := int64(time.Now().Sub(pushStart) / time.Second)
		log.Debug("1 change pushed", "took", onlyPushDurationSec)

		var rootedRepoCpStart = time.Now()
		err = a.commitTxWithRetries(log, ic, tx, maxRetries)
		sivaCpToDuration := time.Now().Sub(rootedRepoCpStart)
		log.Debug("Copy siva file to HDFS", "RootedRepository", ic, "copyToRemote", int64(sivaCpToDuration/time.Second))
		return err
	})
}

func (a *Archiver) beginTxWithRetries(log log15.Logger, initCommit plumbing.Hash, numRetries float64) (tx repository.Tx, err error) {
	for a.backoff.Attempt() < numRetries {
		tx, err = a.RootedTransactioner.Begin(initCommit)
		if err == nil || !repository.HDFSNamenodeError.Is(err) {
			break
		}

		tts := a.backoff.Duration()
		log.Error("Waiting for HDFS reconnection", "RootedRepository", initCommit, "tx", "begin", "wait", tts, "error", err)
		time.Sleep(tts)
	}

	a.backoff.Reset()
	return
}

func (a *Archiver) commitTxWithRetries(log log15.Logger, initCommit model.SHA1, tx repository.Tx, numRetries float64) (err error) {
	for a.backoff.Attempt() < numRetries {
		err = tx.Commit()
		if err == nil || !repository.HDFSNamenodeError.Is(err) {
			break
		}

		tts := a.backoff.Duration()
		log.Error("Waiting for HDFS reconnection", "RootedRepository", initCommit, "tx", "commit", "wait", tts, "error", err)
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
	log log15.Logger,
	r storage.RepoStore,
	tx repository.RootedTransactioner,
	tc TemporaryCloner,
	ls lock.Service,
	to time.Duration) *WorkerPool {

	do := func(log log15.Logger, j *Job) error {
		lsess, err := ls.NewSession(&lock.SessionConfig{TTL: 10 * time.Second})
		if err != nil {
			return err
		}

		defer func() {
			err := lsess.Close()
			if err != nil {
				log.Error("error closing locking session", "error", err)
			}
		}()

		a := NewArchiver(log, r, tx, tc, lsess, to)
		return a.Do(j)
	}

	return NewWorkerPool(log, do)
}
