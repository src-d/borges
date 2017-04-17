package borges

import (
	"sort"
	"strings"
	"testing"

	"github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/require"
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
