package borges

import (
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strconv"
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-billy.v3"
	"gopkg.in/src-d/go-billy.v3/util"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	"gopkg.in/src-d/go-git.v4/plumbing/transport/client"
	"gopkg.in/src-d/go-git.v4/plumbing/transport/server"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
)

const (
	FetchRefSpec = config.RefSpec("refs/*:refs/*")
)

type TemporaryRepository interface {
	io.Closer
	Referencer
	Push(url string, refspecs []config.RefSpec) error
}

type TemporaryCloner interface {
	Clone(id, url string) (TemporaryRepository, error)
}

// NewGitReferencer takes a *git.Repository and returns a Referencer that
// retrieves any valid reference from it. Symbolic references and references
// that do not point to commits (possibly through a tag) are silently ignored.
// It might return an error if any operation fails in the underlying repository.
func NewGitReferencer(r *git.Repository) Referencer {
	return gitReferencer{r}
}

type gitReferencer struct {
	*git.Repository
}

func (r gitReferencer) References() ([]*model.Reference, error) {
	iter, err := r.Repository.References()
	if err != nil {
		return nil, err
	}

	var refs []*model.Reference
	return refs, iter.ForEach(func(ref *plumbing.Reference) error {
		//TODO: add tags support
		if ref.Type() != plumbing.HashReference || ref.IsRemote() {
			return nil
		}

		c, err := ResolveCommit(r.Repository, plumbing.NewHash(ref.Hash().String()))
		if err == ErrReferencedObjectTypeNotSupported {
			return nil
		}

		if err != nil {
			return err
		}

		roots, err := rootCommits(r.Repository, c.Hash)
		if err != nil {
			return err
		}

		refs = append(refs, &model.Reference{
			Name:  ref.Name().String(),
			Hash:  model.NewSHA1(ref.Hash().String()),
			Init:  roots[0],
			Roots: roots,
			Time:  c.Committer.When,
		})
		return nil
	})
}

func rootCommits(r *git.Repository, from plumbing.Hash) ([]model.SHA1, error) {
	var roots []model.SHA1

	cIter, err := r.Log(&git.LogOptions{From: from})
	if err != nil {
		return nil, err
	}

	err = cIter.ForEach(func(wc *object.Commit) error {
		if wc.NumParents() == 0 {
			roots = append(roots, model.SHA1(wc.Hash))
		}

		return nil
	})

	return roots, err
}

// ResolveCommit gets the hash of a commit that is referenced by a tag, per example.
// The only resolvable objects are Tags and Commits. If the object is not one of them,
// This method will return an ErrReferencedObjectTypeNotSupported. The output hash
// always will be a Commit hash.
func ResolveCommit(r *git.Repository, h plumbing.Hash) (*object.Commit, error) {
	obj, err := r.Object(plumbing.AnyObject, h)
	if err != nil {
		return nil, err
	}

	switch o := obj.(type) {
	case *object.Commit:
		return o, nil
	case *object.Tag:
		return ResolveCommit(r, o.Target)
	default:
		log.Warn("referenced object not supported", "type", o.Type())
		return nil, ErrReferencedObjectTypeNotSupported
	}
}

func NewTemporaryCloner(tmpFs billy.Filesystem) TemporaryCloner {
	return &temporaryRepositoryBuilder{tmpFs}
}

type temporaryRepositoryBuilder struct {
	TempFilesystem billy.Filesystem
}

type temporaryRepository struct {
	Referencer
	Repository     *git.Repository
	TempFilesystem billy.Filesystem
	TempPath       string
}

func (b *temporaryRepositoryBuilder) Clone(id, endpoint string) (TemporaryRepository, error) {
	dir := filepath.Join("local_repos", id,
		strconv.FormatInt(time.Now().UnixNano(), 10))

	tmpFs, err := b.TempFilesystem.Chroot(dir)
	if err != nil {
		return nil, err
	}

	s, err := filesystem.NewStorage(tmpFs)
	if err != nil {
		return nil, err
	}

	r, err := git.Init(s, nil)
	if err != nil {
		_ = util.RemoveAll(b.TempFilesystem, dir)
		return nil, err
	}

	remote, err := r.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URL:  endpoint,
	})
	if err != nil {
		_ = util.RemoveAll(b.TempFilesystem, dir)
		return nil, err
	}

	o := &git.FetchOptions{
		RefSpecs: []config.RefSpec{FetchRefSpec},
	}
	err = remote.Fetch(o)
	if err == git.NoErrAlreadyUpToDate || err == transport.ErrEmptyRemoteRepository {
		r, err = git.Init(memory.NewStorage(), nil)
	}

	if err != nil {
		_ = util.RemoveAll(b.TempFilesystem, dir)
		return nil, err
	}

	return &temporaryRepository{
		Referencer:     NewGitReferencer(r),
		Repository:     r,
		TempFilesystem: b.TempFilesystem,
		TempPath:       dir,
	}, nil
}

func (r *temporaryRepository) Push(url string, refspecs []config.RefSpec) error {
	const remoteName = "tmp"
	defer func() { _ = r.Repository.DeleteRemote(remoteName) }()
	remote, err := r.Repository.CreateRemote(&config.RemoteConfig{
		Name: remoteName,
		URL:  url,
	})
	if err != nil {
		return err
	}

	return remote.Push(&git.PushOptions{RefSpecs: refspecs})
}

func (r *temporaryRepository) Close() error {
	r.Repository = nil
	return util.RemoveAll(r.TempFilesystem, r.TempPath)
}

func WithInProcRepository(r *git.Repository, f func(string) error) error {
	proto := fmt.Sprintf("borges%d", rand.Uint32())
	url := fmt.Sprintf("%s://%s", proto, "repo")
	ep, err := transport.NewEndpoint(url)
	if err != nil {
		return err
	}

	loader := server.MapLoader{ep.String(): r.Storer}
	s := server.NewClient(loader)
	client.InstallProtocol(proto, s)
	defer client.InstallProtocol(proto, nil)

	return f(url)
}
