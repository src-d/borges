package borges

import (
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-kallax.v1"
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

// NewChanges returns Changes needed to obtain the current state of the
// repository from a set of old references. The Changes could be create,
// update or delete. If an old reference has the same name of a new one, but the
// init commit is different, then the changes will contain a delete command and
// a create command. If a new reference has more than one init commit, at least
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
func NewChanges(old, new Referencer) (Changes, error) {
	now := time.Now()
	return newChanges(now, old, new)
}

func newChanges(now time.Time, old, new Referencer) (Changes, error) {
	newRefs, err := new.References()
	if err != nil {
		return nil, err
	}

	oldRefs, err := old.References()
	if err != nil {
		return nil, err
	}

	oldRefsByName := refsByName(oldRefs)
	changes := make(Changes)
	for _, newRef := range newRefs {
		err := addToChangesDfferenceBetweenOldAndNewRefs(now, changes, newRef, oldRefsByName)
		if ErrObjectTypeNotSupported.Is(err) {
			continue
		}

		if err != nil {
			return nil, err
		}
	}

	for _, r := range oldRefsByName {
		changes.Delete(r)
	}

	return changes, nil
}

// For a given rReference it:
//  - puts new Change to Changes
//  - removes rReference from oldRefs
func addToChangesDfferenceBetweenOldAndNewRefs(
	now time.Time,
	c Changes,
	newRef *model.Reference,
	oldRefs map[string]*model.Reference) error {

	oldRef, ok := oldRefs[newRef.Name]

	// If we don't have the reference or the init commit has changed,
	// we will create a new reference
	if !ok || oldRef.Init != newRef.Init {
		createdAt := now
		if oldRef != nil {
			createdAt = oldRef.CreatedAt
		}

		newReference := model.NewReference()
		newReference.Name = newRef.Name
		newReference.Hash = newRef.Hash
		newReference.Init = newRef.Init
		newReference.Roots = newRef.Roots
		newReference.Timestamps = kallax.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: now,
		}
		c.Add(newReference)

		return nil
	}

	if newRef.Hash != oldRef.Hash {
		updateReference := model.NewReference()
		updateReference.Name = newRef.Name
		updateReference.Name = newRef.Name
		updateReference.Hash = newRef.Hash
		updateReference.Init = newRef.Init
		updateReference.Roots = newRef.Roots
		updateReference.Timestamps = kallax.Timestamps{
			CreatedAt: oldRef.CreatedAt,
			UpdatedAt: now,
		}
		c.Update(oldRef, updateReference)
	}

	delete(oldRefs, newRef.Name)

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

func refsByName(refs []*model.Reference) map[string]*model.Reference {
	result := make(map[string]*model.Reference)
	for _, r := range refs {
		result[r.Name] = r
	}

	return result
}
