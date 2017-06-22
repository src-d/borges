package borges

import (
	"sort"
	"strings"
	"testing"

	"github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v3/memfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"srcd.works/core.v0/model"
)

func TestNewChanges(t *testing.T) {
	fixtures.Init()
	defer fixtures.Clean()
	for _, ct := range ChangesFixtures {
		t.Run(ct.TestName, func(t *testing.T) {
			require := require.New(t)
			sto, err := ct.NewRepository()
			require.NoError(err)
			changes, err := newChanges(timeNow, ct.OldReferences, sto)
			require.NoError(err)

			sortChanges(changes)
			sortChanges(ct.Changes)

			require.Equal(ct.Changes, changes)
		})
	}
}

func TestChanges_ReferenceToTagObject(t *testing.T) {
	fixtures.Init()
	defer fixtures.Clean()
	require := require.New(t)

	srcFs := fixtures.ByTag("tags").One().DotGit()
	sto, err := filesystem.NewStorage(srcFs)
	require.NoError(err)

	r, err := git.Open(sto, memfs.New())
	require.NoError(err)

	changes, err := newChanges(timeNow, nil, r)
	require.NoError(err)

	require.Equal(1, len(changes))
	for k, v := range changes {
		require.Equal(model.NewSHA1("f7b877701fbf855b44c0a9e86f3fdce2c298b07f"), k)
		for _, c := range v {
			require.Equal(Create, c.Action())
		}

		require.Equal(4, len(v))
	}
}

func sortChanges(c Changes) {
	for _, cmds := range c {
		sort.Sort(cmdSort(cmds))
	}
}

type cmdSort []*Command

func (s cmdSort) Len() int      { return len(s) }
func (s cmdSort) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s cmdSort) Less(i, j int) bool {
	a, b := s[i], s[j]
	switch a.Action() {
	case Update:
		switch b.Action() {
		case Update:
			return strings.Compare(a.New.Name, b.New.Name) < 0
		case Create:
			return true
		case Delete:
			return true
		}
	case Create:
		switch b.Action() {
		case Update:
			return false
		case Create:
			return strings.Compare(a.New.Name, b.New.Name) < 0
		case Delete:
			return true
		}
	case Delete:
		switch b.Action() {
		case Update:
			return false
		case Create:
			return false
		case Delete:
			return strings.Compare(a.Old.Name, b.Old.Name) < 0
		}
	}

	return false
}
