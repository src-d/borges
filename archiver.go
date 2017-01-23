package borges

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"srcd.works/go-errors.v0"
)

const tempDir = "/tmp/borges"

var (
	ErrNotSupported       = errors.NewKind("feature not supported: %s")
	ErrCleanRepositoryDir = errors.NewKind("cleaning up local repo dir failed")
)

// Archiver archives repositories. Archiver instances are thread-safe and can
// be reused.
//
// See borges documentation for more details about the archiving rules.
type Archiver struct {
	Notifiers struct {
		// Start function, if set, is called whenever a job is started.
		Start func(*Job)
		// Stop function, if set, is called whenever a job stops. If
		// there was an error, it is passed as second parameter,
		// otherwise, it is nil.
		Stop func(*Job, error)
		// Warn function, if set, is called whenever there is a warning
		// during the processing of a repository.
		Warn func(*Job, error)
	}

	// TempDir is the path to a temporary directory used to fetch
	// repositories to.
	TempDir string
}

// Do archives a repository according to a job.
func (a *Archiver) Do(j *Job) error {
	a.notifyStart(j)
	err := a.do(j)
	a.notifyStop(j, err)
	return err
}

func (a *Archiver) do(j *Job) error {
	r, err := a.getRepositoryModel(j)
	if err != nil {
		return err
	}

	haves, err := a.getHaves(r)
	if err != nil {
		return err
	}

	dir, err := a.newRepoDir(j)
	if err != nil {
		return err
	}
	defer a.cleanRepoDir(j, dir)

	gr, err := a.createLocalRepository(dir, j)
	if err != nil {
		return err
	}

	err = a.fetch(r, gr, haves)
	if err != nil {
		return err
	}

	changesPerFirstCommit, err := NewChanges(r.References, gr)
	if err != nil {
		return err
	}

	for fc, changes := range changesPerFirstCommit {
		//TODO: instantiate a go-git storer for the rooted repo (repository for the given first_commit).
		//TODO: instantiate go-git server (in-process)
		//TODO: try lock first_commit
		//TODO: if lock cannot be acquired after timeout, continue
		//TODO: push from local repo to rooted repo with changes for this first_commit. A refspec must be used to namespace every reference with a prefix with the repository id (refs/heads/<repo_id>/<reference>) [optimization: perform this operation without actually retrieving the repository siva file; future task]
		//TODO: update references db with the changes
		//TODO: release lock
		_ = fc
		_ = changes
	}

	//TODO: Update repository

	return nil
}

func (a *Archiver) getRepositoryModel(j *Job) (*Repository, error) {
	//TODO: if id == 0 { generate new repository with URL }
	//      else { get from DB }
	return &Repository{ID: j.RepositoryID}, nil
}

func (a *Archiver) getHaves(r *Repository) ([]plumbing.Hash, error) {
	//TODO
	return nil, nil
}

func (a *Archiver) fetch(r *Repository, gr *git.Repository, haves []plumbing.Hash) error {
	remote, err := gr.CreateRemote(&config.RemoteConfig{
		Name: git.DefaultRemoteName,
		URL:  r.Endpoints[0],
	})
	if err != nil {
		return err
	}

	//TODO

	if err := remote.Fetch(&git.FetchOptions{}); err != nil {
		return nil
	}

	return gr.Fetch(&git.FetchOptions{})
}

func (a *Archiver) newRepoDir(j *Job) (string, error) {
	dir := filepath.Join(a.TempDir, "repos",
		strconv.FormatUint(j.RepositoryID, 10),
		strconv.Itoa(rand.Int()),
	)
	return dir, os.MkdirAll(dir, os.FileMode(0755))
}

func (a *Archiver) cleanRepoDir(j *Job, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		a.Notifiers.Warn(j, ErrCleanRepositoryDir.Wrap(err))
	}
}

func (a *Archiver) createLocalRepository(dir string, j *Job) (*git.Repository, error) {
	return git.NewFilesystemRepository(dir)
}

func (a *Archiver) notifyStart(j *Job) {
	if a.Notifiers.Start == nil {
		return
	}

	a.Notifiers.Start(j)
}

func (a *Archiver) notifyStop(j *Job, err error) {
	if a.Notifiers.Stop == nil {
		return
	}

	a.Notifiers.Stop(j, err)
}

func (a *Archiver) notifyWarn(j *Job, err error) {
	if a.Notifiers.Warn == nil {
		return
	}

	a.Notifiers.Warn(j, err)
}

// NewArchiverWorkerPool creates a new WorkerPool that uses an Archiver to
// process jobs. It takes optional start, stop and warn notifier functions that
// are equal to the Archiver notifiers but with additional WorkerContext.
func NewArchiverWorkerPool(start func(*WorkerContext, *Job),
	stop func(*WorkerContext, *Job, error),
	warn func(*WorkerContext, *Job, error)) *WorkerPool {

	do := func(ctx *WorkerContext, j *Job) error {
		a := &Archiver{
			TempDir: filepath.Join(tempDir, strconv.Itoa(ctx.ID)),
		}

		if start != nil {
			a.Notifiers.Start = func(j *Job) {
				start(ctx, j)
			}
		}

		if stop != nil {
			a.Notifiers.Stop = func(j *Job, err error) {
				stop(ctx, j, err)
			}
		}

		if warn != nil {
			a.Notifiers.Warn = func(j *Job, err error) {
				warn(ctx, j, err)
			}
		}

		return a.Do(j)
	}

	return NewWorkerPool(do)
}
