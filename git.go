package borges

import (
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

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

		h, err := ResolveHash(r.Repository, plumbing.NewHash(ref.Hash().String()))
		if err == ErrReferencedObjectTypeNotSupported {
			return nil
		}

		if err != nil {
			return err
		}

		roots, err := rootCommits(r.Repository, h)
		if err != nil {
			return err
		}

		refs = append(refs, &model.Reference{
			Name:  ref.Name().String(),
			Hash:  model.NewSHA1(ref.Hash().String()),
			Init:  roots[0],
			Roots: roots,
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
