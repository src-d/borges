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
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/core-retrieval.v0/repository"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-kallax.v1"
	"gopkg.in/src-d/go-log.v1"
)

var (
	ErrCleanRepositoryDir      = errors.NewKind("cleaning up local repo dir failed")
	ErrClone                   = errors.NewKind("cloning %s failed")
	ErrPushToRootedRepository  = errors.NewKind("push to rooted repo %s failed")
	ErrArchivingRoots          = errors.NewKind("archiving %d out of %d roots failed: %s")
	ErrEndpointsEmpty          = errors.NewKind("endpoints is empty")
	ErrRepositoryIDNotFound    = errors.NewKind("repository id not found: %s")
	ErrChanges                 = errors.NewKind("error computing changes")
	ErrAlreadyFetching         = errors.NewKind("repository %s was already in a fetching status")
	ErrSetStatus               = errors.NewKind("unable to set repository to status: %s")
	ErrFatal                   = errors.NewKind("fatal, %v: stacktrace: %s")
	ErrCannotProcessRepository = errors.NewKind("cannot process repository")
	ErrProcessedWithErrors     = errors.NewKind("repository processed with errors")

	// StrRemoveTmpFiles is the string used to log when tmp files could not
	// be deleted.
	StrRemoveTmpFiles = "could not remove tmp files"
)

// Archiver archives repositories. Archiver instances are thread-safe and can
// be reused.
//
// See borges documentation for more details about the archiving rules.
type Archiver struct {
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

func NewArchiver(
	r RepositoryStore,
	tx repository.RootedTransactioner,
	tc TemporaryCloner,
	ls lock.Session, timeout time.Duration,
) *Archiver {
	return &Archiver{
		backoff:             newBackoff(),
		TemporaryCloner:     tc,
		Timeout:             timeout,
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
	log := log.New(log.Fields{"job": j.RepositoryID})
	log.Debugf("job started")
	if err := a.do(ctx, log, j); err != nil {
		log.Errorf(err, "job finished with error")
		return err
	}

	log.Infof("job finished successfully")
	return nil
}

func (a *Archiver) do(ctx context.Context, logger log.Logger, j *Job) (err error) {
	now := time.Now()
	ctx, cancel := context.WithTimeout(ctx, a.Timeout)
	defer cancel()

	r, err := a.getRepositoryModel(j)
	if err != nil {
		return err
	}

	defer a.reportMetrics(r, now)
	defer a.recoverDo(logger, r, &now, err)
	defer func() {
		logger.With(log.Fields{"status": r.Status}).Debugf("repository processed")
	}()

	logger.With(log.Fields{
		"status":     r.Status,
		"last-fetch": r.FetchedAt,
		"references": len(r.References),
	}).Debugf("repository model obtained")

	if err := a.isProcessableRepository(r, &now); err != nil {
		return ErrCannotProcessRepository.Wrap(err)
	}

	if err := a.Store.SetStatus(r, model.Fetching); err != nil {
		return ErrSetStatus.Wrap(err, model.Fetching)
	}

	endpoint, err := selectEndpoint(r.Endpoints)
	if err != nil {
		a.updateFailed(r, model.Pending)
		return err
	}

	logger = logger.New(log.Fields{"endpoint": endpoint})
	gr, err := a.doClone(ctx, logger, &now, j, r, endpoint)
	if err != nil {
		return err
	}

	if gr == nil {
		return nil
	}

	log.Debugf("remote repository cloned")
	if err := a.doPush(ctx, logger, &now, j, r, endpoint, gr); err != nil {
		e := gr.Close()
		if e != nil {
			logger.Errorf(err, StrRemoveTmpFiles)
		}

		return err
	}

	return gr.Close()
}

func (a *Archiver) doClone(
	ctx context.Context, logger log.Logger, now *time.Time,
	j *Job, r *model.Repository, endpoint string,
) (tr TemporaryRepository, err error) {

	tr, err = a.TemporaryCloner.Clone(ctx, j.RepositoryID.String(), endpoint)
	if err == nil {
		return
	}

	status := model.Pending
	defer func() {
		a.updateFailed(r, status)
	}()

	if err == transport.ErrRepositoryNotFound {
		status = model.NotFound
		err = nil
		logger.Warningf("repository not found")
		return
	}

	if err == transport.ErrAuthenticationRequired {
		status = model.AuthRequired
		err = nil
		logger.Warningf("repository not cloned, authentication required")
		return
	}

	if err != transport.ErrEmptyUploadPackRequest {
		r.FetchErrorAt = now
		err = ErrClone.Wrap(err, endpoint)
		return
	}

	return
}

func (a *Archiver) doPush(
	ctx context.Context, logger log.Logger, now *time.Time,
	j *Job, r *model.Repository, endpoint string, gr TemporaryRepository,
) error {
	changes, err := NewChanges(NewModelReferencer(r), gr)
	if err != nil {
		a.updateFailed(r, model.Pending)
		return ErrChanges.Wrap(err)
	}

	logger.With(log.Fields{"roots": len(changes)}).Debugf("changes obtained")
	if err := a.pushChangesToRootedRepositories(ctx, logger, j, r, gr, changes, now); err != nil {
		r.FetchErrorAt = now
		a.updateFailed(r, model.Pending)
		return ErrProcessedWithErrors.Wrap(err)
	}

	return nil
}

func (a *Archiver) updateFailed(r *model.Repository, s model.FetchStatus) {
	if err := a.Store.UpdateFailed(r, s); err != nil {
		log.With(log.Fields{"job": r.ID}).Errorf(err, "error setting repository as failed")
	}
}

func (a *Archiver) reportMetrics(r *model.Repository, now time.Time) {
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
}

func (a *Archiver) recoverDo(logger log.Logger, r *model.Repository, now *time.Time, err error) {
	return
	rcv := recover()
	if rcv == nil {
		return
	}

	logger.Errorf(err, "panic while processing repository")

	r.FetchErrorAt = now
	a.updateFailed(r, model.Pending)
	err = ErrFatal.New(rcv, debug.Stack())
}

func (a *Archiver) isProcessableRepository(r *model.Repository, now *time.Time) error {
	if r.Status != model.Fetching {
		return nil
	}

	r.FetchErrorAt = now
	a.updateFailed(r, model.Pending)

	return ErrAlreadyFetching.New(r.ID)
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

func (a *Archiver) pushChangesToRootedRepositories(
	ctx context.Context,
	logger log.Logger,
	j *Job,
	r *model.Repository,
	tr TemporaryRepository,
	changes Changes,
	now *time.Time,
) error {
	var failedInits []model.SHA1
	for ic, cs := range changes {
		logger = logger.New(log.Fields{"root": ic.String()})
		lock := a.LockSession.NewLocker(fmt.Sprintf("borges/%s", ic.String()))
		ch, err := lock.Lock()
		if err != nil {
			failedInits = append(failedInits, ic)
			logger.Errorf(err, "failed to acquire lock")
			continue
		}

		logger.Debugf("push changes to rooted repository started")
		if err := a.pushChangesToRootedRepository(ctx, logger, r, tr, ic, cs); err != nil {
			err = ErrPushToRootedRepository.Wrap(err, ic.String())
			logger.Errorf(err, "error pushing changes to rooted repository")

			failedInits = append(failedInits, ic)
			if err := lock.Unlock(); err != nil {
				logger.Errorf(err, "failed to release lock")
			}
			continue
		}

		logger.Debugf("push changes to rooted repository finished")
		logger.Debugf("update repository references started")
		r.References = updateRepositoryReferences(r.References, cs, ic)
		for _, ref := range r.References {
			ref.Repository = r
		}

		if err := a.Store.UpdateFetched(r, *now); err != nil {
			err = ErrPushToRootedRepository.Wrap(err, ic.String())
			logger.Errorf(err, "error updating repository in database")
			failedInits = append(failedInits, ic)
		}

		logger.Debugf("update repository references finished")

		select {
		case <-ch:
			logger.Errorf(err, "lost the lock")
		default:
		}

		if err := lock.Unlock(); err != nil {
			logger.Errorf(err, "failed to release lock")
		}
	}

	if len(changes) == 0 {
		if err := a.Store.UpdateFetched(r, *now); err != nil {
			logger.Errorf(err, "error updating repository in database")
		}
	}

	return checkFailedInits(changes, failedInits)
}

func (a *Archiver) pushChangesToRootedRepository(ctx context.Context, logger log.Logger, r *model.Repository, tr TemporaryRepository, ic model.SHA1, changes []*Command) error {
	var rootedRepoCpStart = time.Now()
	tx, err := a.beginTxWithRetries(ctx, logger, plumbing.Hash(ic), maxRetries)

	logger = logger.With(log.Fields{
		"rooted-repository": ic.String(),
	})

	sivaCpFromDuration := time.Now().Sub(rootedRepoCpStart)
	logger.With(log.Fields{
		"duration": sivaCpFromDuration,
	}).Debugf("copy siva file from FS")

	if err != nil {
		return err
	}

	rr, err := a.openGitWithRetries(tx.Storer(), logger, maxRetries)
	if err != nil {
		if e := tx.Rollback(); e != nil {
			logger.Errorf(e, StrRemoveTmpFiles)
		}
		return err
	}

	err = withInProcRepository(ic, rr, func(url string) error {
		if err := StoreConfig(rr, r); err != nil {
			return err
		}

		refspecs := a.changesToPushRefSpec(r.ID, changes)
		pushStart := time.Now()
		if err := tr.Push(ctx, url, refspecs); err != nil {
			onlyPushDurationSec := int64(time.Now().Sub(pushStart) / time.Second)
			logger.With(log.Fields{
				"refs":     refspecs,
				"duration": onlyPushDurationSec,
			}).Errorf(err, "error pushing one change for")
			return err
		}
		onlyPushDurationSec := int64(time.Now().Sub(pushStart) / time.Second)
		logger.With(log.Fields{
			"duration": onlyPushDurationSec,
		}).Debugf("one change pushed")

		var rootedRepoCpStart = time.Now()
		err = a.commitTxWithRetries(ctx, logger, ic, tx, maxRetries)
		if err != nil {
			logger.With(log.Fields{
				"duration": time.Now().Sub(rootedRepoCpStart),
			}).Errorf(err, "could not copy siva file to FS")
			return err
		}

		logger.With(log.Fields{
			"duration": time.Now().Sub(rootedRepoCpStart),
		}).Debugf("copy siva file to FS")

		return nil
	})

	if err != nil {
		e := tx.Rollback()
		if e != nil {
			logger.Errorf(err, StrRemoveTmpFiles)
		}
	}

	return err
}

func (a *Archiver) beginTxWithRetries(
	ctx context.Context,
	logger log.Logger,
	initCommit plumbing.Hash,
	numRetries float64,
) (tx repository.Tx, err error) {
	for a.backoff.Attempt() < numRetries {
		tx, err = a.RootedTransactioner.Begin(ctx, initCommit)
		if err == nil || !repository.HDFSNamenodeError.Is(err) {
			break
		}

		tts := a.backoff.Duration()
		logger.With(log.Fields{
			"rooted-repository": initCommit,
			"tx":                "begin",
			"wait":              tts,
		}).Errorf(err, "waiting for HDFS reconnection")
		time.Sleep(tts)
	}

	a.backoff.Reset()
	return
}

func (a *Archiver) commitTxWithRetries(
	ctx context.Context,
	logger log.Logger,
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
		logger.With(log.Fields{
			"rooted-repository": initCommit,
			"tx":                "commit",
			"wait":              tts,
		}).Errorf(err, "Waiting for HDFS reconnection")
		time.Sleep(tts)
	}

	a.backoff.Reset()
	return
}

func (a *Archiver) openGitWithRetries(storage storage.Storer, logger log.Logger, numRetries float64) (*git.Repository, error) {
	var (
		repo *git.Repository
		err  error
	)
	for a.backoff.Attempt() < numRetries {
		if repo, err = git.Open(storage, nil); err == nil {
			break
		}

		tts := a.backoff.Duration()
		logger.With(log.Fields{"wait": tts}).Errorf(err, "waiting for git Open")
		time.Sleep(tts)
	}
	a.backoff.Reset()

	return repo, err
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

	return ErrArchivingRoots.New(n, len(changes), strings.Join(strs, ", "))
}

// NewArchiverWorkerPool creates a new WorkerPool that uses an Archiver to
// process jobs. It takes optional start, stop and warn notifier functions that
// are equal to the Archiver notifiers but with additional WorkerContext.
func NewArchiverWorkerPool(
	r RepositoryStore, tx repository.RootedTransactioner,
	tc TemporaryCloner,
	ls lock.Service,
	timeout time.Duration,
) *WorkerPool {

	do := func(ctx context.Context, logger log.Logger, j *Job) error {
		lsess, err := ls.NewSession(&lock.SessionConfig{TTL: 10 * time.Second})
		if err != nil {
			return err
		}

		defer func() {
			err := lsess.Close()
			if err != nil {
				logger.Errorf(err, "error closing locking session")
			}
		}()

		a := NewArchiver(r, tx, tc, lsess, timeout)
		return a.Do(ctx, j)
	}

	return NewWorkerPool(do)
}
