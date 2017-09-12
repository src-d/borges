package borges

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strconv"
	"time"

	"github.com/inconshreveable/log15"
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
	FetchHEAD    = config.RefSpec("HEAD:refs/heads/HEAD")
)

type TemporaryRepository interface {
	io.Closer
	Referencer
	Push(ctx context.Context, url string, refspecs []config.RefSpec) error
}

type TemporaryCloner interface {
	Clone(ctx context.Context, id, url string) (TemporaryRepository, error)
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
	var seenRoots = make(map[plumbing.Hash][]model.SHA1)
	return refs, iter.ForEach(func(ref *plumbing.Reference) error {
		//TODO: add tags support
		if ref.Type() != plumbing.HashReference || ref.Name().IsRemote() {
			return nil
		}

		c, err := ResolveCommit(r.Repository, plumbing.NewHash(ref.Hash().String()))
		if err == ErrReferencedObjectTypeNotSupported {
			return nil
		}

		if err != nil {
			return err
		}

		roots, err := rootCommits(r.Repository, c, seenRoots)
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

type commitFrame struct {
	cursor int
	hashes []plumbing.Hash
	roots  [][]model.SHA1
}

func rootCommits(
	r *git.Repository,
	start *object.Commit,
	seenRoots map[plumbing.Hash][]model.SHA1,
) ([]model.SHA1, error) {
	var seen = make(map[plumbing.Hash]bool)
	stack := []*commitFrame{
		&commitFrame{0, []plumbing.Hash{start.Hash}, make([][]model.SHA1, 1)},
	}
	store := r.Storer

	for {
		current := len(stack) - 1
		if current < 0 {
			return nil, nil
		}

		frame := stack[current]
		if len(frame.hashes) <= frame.cursor {
			if current == 0 {
				seenRoots[frame.hashes[0]] = frame.roots[0]
				return deduplicateHashes(frame.roots[0]), nil
			}

			if current > 0 {
				prevFrame := stack[current-1]
				for i, r := range frame.roots {
					prevFrame.roots[prevFrame.cursor-1] = append(prevFrame.roots[prevFrame.cursor-1], r...)
					if _, ok := seenRoots[frame.hashes[i]]; !ok {
						seenRoots[frame.hashes[i]] = r
					}
				}
			}

			stack = stack[:current]
			continue
		}

		hash := frame.hashes[frame.cursor]
		if roots, ok := seenRoots[hash]; ok {
			frame.roots[frame.cursor] = roots
			frame.cursor++
			continue
		}

		frame.cursor++

		if seen[hash] {
			continue
		}

		seen[hash] = true

		var c *object.Commit
		if hash != start.Hash {
			obj, err := store.EncodedObject(plumbing.CommitObject, hash)
			if err != nil {
				return nil, err
			}

			do, err := object.DecodeObject(store, obj)
			if err != nil {
				return nil, err
			}

			c = do.(*object.Commit)
		} else {
			c = start
		}

		if c.NumParents() > 0 {
			stack = append(stack, &commitFrame{
				0,
				c.ParentHashes,
				make([][]model.SHA1, len(c.ParentHashes)),
			})
		} else {
			frame.roots[frame.cursor-1] = append(frame.roots[frame.cursor-1], model.SHA1(c.Hash))
		}
	}
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
		log15.Warn("referenced object not supported", "hash", h.String(), "type", o.Type())
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

func (b *temporaryRepositoryBuilder) Clone(
	ctx context.Context,
	id, endpoint string,
) (TemporaryRepository, error) {
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
		URLs: []string{endpoint},
	})
	if err != nil {
		_ = util.RemoveAll(b.TempFilesystem, dir)
		return nil, err
	}

	o := &git.FetchOptions{
		RefSpecs: []config.RefSpec{FetchRefSpec, FetchHEAD},
	}
	err = remote.FetchContext(ctx, o)
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

func (r *temporaryRepository) Push(
	ctx context.Context,
	url string,
	refspecs []config.RefSpec,
) error {
	const remoteName = "tmp"
	defer func() { _ = r.Repository.DeleteRemote(remoteName) }()
	remote, err := r.Repository.CreateRemote(&config.RemoteConfig{
		Name: remoteName,
		URLs: []string{url},
	})
	if err != nil {
		return err
	}

	o := &git.PushOptions{
		RemoteName: remoteName,
		RefSpecs:   refspecs,
	}
	return remote.PushContext(ctx, o)
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

func StoreConfig(r *git.Repository, mr *model.Repository) error {
	id := mr.ID.String()
	storer := r.Storer
	c, err := storer.Config()
	if err != nil {
		return err
	}

	updated := false
	updated = updateConfigRemote(c, id, mr) || updated
	_, _ = c.Marshal()
	updated = updateConfigIsFork(c, id, mr) || updated

	if !updated {
		return nil
	}

	return storer.SetConfig(c)
}

func updateConfigRemote(c *config.Config, id string, mr *model.Repository) bool {
	remote, ok := c.Remotes[id]
	if ok {
		if stringSliceEqual(remote.URLs, mr.Endpoints) {
			return false
		}

		remote.URLs = nil
		// Force marshalling of remote back into raw config so that order of
		// endpoints is preserved.
		_, _ = c.Marshal()

		remote.URLs = mr.Endpoints
		return true
	}

	c.Remotes[id] = &config.RemoteConfig{
		Name: id,
		URLs: mr.Endpoints,
	}

	return true
}

func updateConfigIsFork(c *config.Config, id string, mr *model.Repository) bool {
	const (
		section = "remote"
		key     = "isfork"
	)

	isFork := false
	if mr.IsFork != nil {
		isFork = *mr.IsFork
	}

	val := strconv.FormatBool(isFork)
	ss := c.Raw.Section(section).Subsection(id)
	if ss.Option(key) == val {
		return false
	}

	ss.SetOption(key, val)
	return true
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func deduplicateHashes(hashes []model.SHA1) []model.SHA1 {
	var set hashSet
	for _, h := range hashes {
		set.add(h)
	}
	return []model.SHA1(set)
}

type hashSet []model.SHA1

func (hs *hashSet) add(hash model.SHA1) {
	if !hs.contains(hash) {
		*hs = append(*hs, hash)
	}
}

func (hs hashSet) contains(hash model.SHA1) bool {
	for _, h := range hs {
		if h == hash {
			return true
		}
	}
	return false
}
