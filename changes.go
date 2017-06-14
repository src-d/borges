package borges

import (
	"errors"
	"time"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-kallax.v1"
	"srcd.works/core.v0/model"
)

// Changes represents several actions to realize to our root repositories. The
// map key is the hash of a init commit, and the value is a slice of Command
// that can be add a new reference, delete a reference or update the hash a
// reference points to.
type Changes map[model.SHA1][]*Command

type Action string

const (
	Create  Action = "create"
	Update         = "update"
	Delete         = "delete"
	Invalid        = "invalid"
)

// Command is the way to represent a change into a reference. It could be:
// - Create: A new reference is created
// - Update: A previous reference is updated. This means its head changes.
// - Delete: A previous reference does not exist now.
type Command struct {
	Old *model.Reference
	New *model.Reference
}

// Action returns the action related to this command depending of his content
func (c *Command) Action() Action {
	if c.Old == nil && c.New == nil {
		return Invalid
	}

	if c.Old == nil {
		return Create
	}

	if c.New == nil {
		return Delete
	}

	return Update
}

var ErrReferencedObjectTypeNotSupported error = errors.New("referenced object type not supported")

// NewChanges returns Changes needed to obtain the current state of the
// repository from a set of old references. The Changes could be create,
// update or delete. It also checks the root commits per each reference.
// If an old reference has the same name of a new one, but the init commit
// is different, then the changes will contain a delete command and a
// create command. If a new reference has more than one init commit, at least
// one create command per init commit will be created.
//
// Here are all possible cases for up to one reference.
// We use the notation a<11,01> to refer to reference 'a', pointing to hash
// '11' with initial commit '01'.
//
// 	Old		New		Changes
//	---		---		-------
//	Ø		Ø		Ø
//	Ø		a<11,01>	01 -> c<a,11>
//	a<11,01>	Ø		01 -> d<a,11>
//	a<11,01>	a<12,01>	01 -> u<a,11,12>
//	a<11,01>	a<11,02>	01 -> d<a,11> | 02 -> c<a,11> (invalid)
//	a<11,01>	a<12,02>	01 -> d<a,11> | 02 -> c<a,12>
//
func NewChanges(oldRefs []*model.Reference, newRepo *git.Repository) (Changes, error) {
	now := time.Now()
	return newChanges(now, oldRefs, newRepo)
}

func newChanges(now time.Time, oldRefs []*model.Reference, newRepo *git.Repository) (Changes, error) {
	refIter, err := newRepo.References()
	if err != nil {
		return nil, err
	}

	refsByName := refsByName(oldRefs)
	changes := make(Changes)
	err = refIter.ForEach(func(r *plumbing.Reference) error {
		err := addChangesBetweenOldAndNewReferences(now, changes, r, refsByName, newRepo)
		if err == ErrReferencedObjectTypeNotSupported {
			// TODO log this
			return nil
		}

		return err
	})
	if err != nil {
		return nil, err
	}

	for _, r := range refsByName {
		changes.Delete(r)
	}

	return changes, nil
}

func addChangesBetweenOldAndNewReferences(
	now time.Time,
	c Changes,
	rReference *plumbing.Reference,
	oldRefs map[string]*model.Reference,
	newRepo *git.Repository) error {

	//TODO: add tags support
	if rReference.Type() != plumbing.HashReference || rReference.IsRemote() {
		return nil
	}

	roots, err := rootCommits(newRepo, rReference.Hash())
	if err != nil {
		return err
	}

	ref := oldRefs[rReference.Name().String()]

	// If we don't have the reference or the init commit has changed,
	// we will create a new reference
	if ref == nil || roots[0] != ref.Init {
		createdAt := now
		if ref != nil {
			createdAt = ref.CreatedAt
		}
		newReference := &model.Reference{
			Name:  rReference.Name().String(),
			Hash:  model.SHA1(rReference.Hash()),
			Init:  roots[0],
			Roots: roots,
			Timestamps: kallax.Timestamps{
				CreatedAt: createdAt,
				UpdatedAt: now,
			},
		}
		c.Add(newReference)

		return nil
	}

	if rReference.Hash() != plumbing.Hash(ref.Hash) {
		updateReference := &model.Reference{
			Name:  rReference.Name().String(),
			Hash:  model.SHA1(rReference.Hash()),
			Init:  roots[0],
			Roots: roots,
			Timestamps: kallax.Timestamps{
				CreatedAt: ref.CreatedAt,
				UpdatedAt: now,
			},
		}
		c.Update(ref, updateReference)
	}

	delete(oldRefs, rReference.Name().String())

	return nil
}

func (c Changes) Delete(old *model.Reference) {
	c[old.Init] = append(c[old.Init], &Command{Old: old})
}

func (c Changes) Update(old, new *model.Reference) {
	c[new.Init] = append(c[new.Init], &Command{Old: old, New: new})
}

func (c Changes) Add(new *model.Reference) {
	c[new.Init] = append(c[new.Init], &Command{New: new})
}

func rootCommits(r *git.Repository, from plumbing.Hash) ([]model.SHA1, error) {
	h, err := resolveHash(r, from)
	if err != nil {
		return nil, err
	}

	var roots []model.SHA1

	cIter, err := r.Log(&git.LogOptions{From: h})
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

func resolveHash(r *git.Repository, h plumbing.Hash) (plumbing.Hash, error) {
	obj, err := r.Object(plumbing.AnyObject, h)
	if err != nil {
		return plumbing.ZeroHash, err
	}

	switch o := obj.(type) {
	case *object.Commit:
		return o.Hash, nil
	case *object.Tag:
		return resolveHash(r, o.Target)
	default:
		return plumbing.ZeroHash, ErrReferencedObjectTypeNotSupported
	}
}

func refsByName(refs []*model.Reference) map[string]*model.Reference {
	result := make(map[string]*model.Reference)
	for _, r := range refs {
		result[r.Name] = r
	}

	return result
}
