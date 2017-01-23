package borges

import (
	"time"

	"github.com/src-d/go-kallax"
	"srcd.works/core.v0/models"
	"srcd.works/go-git.v4"
	"srcd.works/go-git.v4/plumbing"
	"srcd.works/go-git.v4/plumbing/object"
)

// Changes represents several actions to realize to our root repositories. The
// map key is the hash of a init commit, and the value is a slice of Command
// that can be add a new reference, delete a reference or update the hash a
// reference points to.
type Changes map[models.SHA1][]*Command

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
	Old *models.Reference
	New *models.Reference
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

// NewChanges returns Changes needed to obtain the current state of the
// repository from a set of old references. The Changes could be create,
// update or delete. It also checks the root commits per each reference.
// If an old reference has the same name of a new one, but the init commit
// is different, then the changes will contain a delete command and a
// create command. If a new reference has more than one init commit, at least
// one create command per init commit will be created.
func NewChanges(oldReferences []*models.Reference, repository *git.Repository) (Changes, error) {
	now := time.Now()
	return newChanges(now, oldReferences, repository)
}

func newChanges(now time.Time, oldReferences []*models.Reference, repository *git.Repository) (Changes, error) {
	refIter, err := repository.References()
	if err != nil {
		return nil, err
	}

	refsByName := refsByName(oldReferences)
	changes := make(Changes)
	err = refIter.ForEach(func(r *plumbing.Reference) error {
		return addChangesBetweenOldAndNewReferences(now, changes, r, refsByName, repository)
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
	oldRefs map[string]*models.Reference,
	r *git.Repository) error {

	if rReference.Type() != plumbing.HashReference || rReference.IsRemote() {
		return nil
	}

	roots, err := rootCommits(r, rReference.Hash())
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
		newReference := &models.Reference{
			Name:  rReference.Name().String(),
			Hash:  models.SHA1(rReference.Hash()),
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
		updateReference := &models.Reference{
			Name:  rReference.Name().String(),
			Hash:  models.SHA1(rReference.Hash()),
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

func (c Changes) Delete(old *models.Reference) {
	c[old.Init] = append(c[old.Init], &Command{Old: old})
}

func (c Changes) Update(old, new *models.Reference) {
	c[new.Init] = append(c[new.Init], &Command{Old: old, New: new})
}

func (c Changes) Add(new *models.Reference) {
	c[new.Init] = append(c[new.Init], &Command{New: new})
}

func rootCommits(r *git.Repository, from plumbing.Hash) ([]models.SHA1, error) {
	c, err := r.Commit(from)
	if err != nil {
		return nil, err
	}

	var roots []models.SHA1
	err = object.WalkCommitHistory(c, func(wc *object.Commit) error {
		if wc.NumParents() == 0 {
			roots = append(roots, models.SHA1(wc.Hash))
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return roots, nil
}

func refsByName(refs []*models.Reference) map[string]*models.Reference {
	result := make(map[string]*models.Reference)
	for _, r := range refs {
		result[r.Name] = r
	}

	return result
}
