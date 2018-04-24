package borges

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	"gopkg.in/src-d/go-git.v4/plumbing/transport/client"
	"gopkg.in/src-d/go-git.v4/plumbing/transport/server"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	log "gopkg.in/src-d/go-log.v0"
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

		reference := model.NewReference()
		reference.Name = ref.Name().String()
		reference.Hash = model.NewSHA1(ref.Hash().String())
		reference.Init = roots[0]
		reference.Roots = roots
		reference.Time = c.Committer.When
		refs = append(refs, reference)
		return nil
	})
}

type commitFrame struct {
	cursor int
	hashes []plumbing.Hash
}

func newCommitFrame(hashes ...plumbing.Hash) *commitFrame {
	return &commitFrame{0, hashes}
}

// lastHash returns the last visited hash, assuming one has at least been
// visited before. That is, this should not be called before incrementing
// the cursor for the first time.
func (f *commitFrame) lastHash() plumbing.Hash {
	return f.hashes[f.cursor-1]
}

// rootCommits returns the commits with no parents reachable from `start`.
// To do so, all the commits are iterated using a stack where frames are
// the parent commits of the last visited hash of the previous frame.
//
// As we go down, if a commit has parents, we add a new frame to the stack
// with these parents as hashes.
// If the commit does not have parents its a root, so we add it to the list
// found roots and keep going.
//
// If we have not visited all the hashes in the current frame it means we have
// to switch branches. That means caching the roots found so far for the last
// visited commit in the frame and reset the roots so we can find the ones
// for the new root. If these branches converge nothing happens, that
// point will be cached and we'll load them from the cache and continue.
// If we have visited all the hashes in the current frame we cache the found
// roots and move all the roots found in all the hashes in the frame to the
// last visited hash of the previous frame. The found roots now will be the
// same roots we pushed to the previous frame.
//
// After repeating this process, when we get to the root frame, we just have to
// return the roots cached for it, which will be the roots of all reachable
// commits from the start.
func rootCommits(
	r *git.Repository,
	start *object.Commit,
	seenRoots map[plumbing.Hash][]model.SHA1,
) ([]model.SHA1, error) {
	stack := []*commitFrame{
		newCommitFrame(start.Hash),
	}
	store := r.Storer
	var roots []model.SHA1

	for {
		current := len(stack) - 1
		if current < 0 {
			return nil, nil
		}

		frame := stack[current]
		if len(frame.hashes) <= frame.cursor {
			roots = deduplicateHashes(roots)
			seenRoots[frame.lastHash()] = roots

			// root frame is guaranteed to have just one hash
			if current == 0 {
				return seenRoots[frame.lastHash()], nil
			}

			// move all the roots of all the branches to the last visited
			// hash of the previous frame
			prevFrame := stack[current-1]

			prevHash := prevFrame.lastHash()
			for _, h := range frame.hashes {
				seenRoots[prevHash] = append(seenRoots[prevHash], seenRoots[h]...)
			}

			roots = deduplicateHashes(seenRoots[prevHash])
			seenRoots[prevHash] = roots

			stack = stack[:current]
			continue
		} else if frame.cursor > 0 {
			// if the frame cursor is bigger than 0 and we're not done with it
			// cache the roots of the previous hash and start anew with the
			// next branch.
			seenRoots[frame.lastHash()] = deduplicateHashes(roots)
			roots = nil
		}

		frame.cursor++
		hash := frame.lastHash()
		// use cached roots for this commit, if any
		if cachedRoots, ok := seenRoots[hash]; ok {
			roots = cachedRoots
			continue
		}

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
			stack = append(stack, newCommitFrame(c.ParentHashes...))
		} else {
			roots = append(roots, model.SHA1(c.Hash))
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
		l, _ := log.New()
		l.New(log.Fields{"hash": h.String(), "type": o.Type()}).
			Warningf("referenced object not supported")

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
	dir := filepath.Join(
		"local_repos",
		fmt.Sprintf("%s_%s",
			id,
			strconv.FormatInt(time.Now().UnixNano(), 10)))

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

// sivaLoader loads a go-git storer.Storer for the given endpoint,
// which is expected to be using the siva protocol siva://<hash>
type sivaLoader struct {
	mut   *sync.RWMutex
	repos map[model.SHA1]storer.Storer
}

// add associates a new hash to a go-git storer.
func (l *sivaLoader) add(hash model.SHA1, storer storer.Storer) {
	l.mut.Lock()
	defer l.mut.Unlock()
	l.repos[hash] = storer
}

// remove removes the association between a hash and its storer.
func (l *sivaLoader) remove(hash model.SHA1) {
	l.mut.Lock()
	defer l.mut.Unlock()
	delete(l.repos, hash)
}

// Load a storer from the endpoint received in format siva://[hash]
func (l *sivaLoader) Load(ep *transport.Endpoint) (storer.Storer, error) {
	if !strings.HasPrefix(ep.String(), "siva://") {
		return nil, transport.ErrRepositoryNotFound
	}

	l.mut.RLock()
	defer l.mut.RUnlock()

	hash := model.NewSHA1(ep.String()[len("siva://"):])
	s, ok := l.repos[hash]
	if !ok {
		return nil, transport.ErrRepositoryNotFound
	}

	return s, nil
}

// defaultLoader is a rootedRepoLoader that will be used in the siva
// protocol installation.
var defaultLoader = &sivaLoader{
	new(sync.RWMutex),
	make(map[model.SHA1]storer.Storer),
}

// protoInstalled keeps the installation of the siva protocol synchronized.
var protoInstalled sync.Once

// installProtocol will install the siva protocol, making sure it's only
// installed once.
func installProtocol() {
	protoInstalled.Do(func() {
		client.InstallProtocol("siva", server.NewClient(defaultLoader))
	})
}

func withInProcRepository(hash model.SHA1, r *git.Repository, f func(string) error) error {
	installProtocol()
	defaultLoader.add(hash, r.Storer)
	defer defaultLoader.remove(hash)

	url := fmt.Sprintf("siva://%s", hash)
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
