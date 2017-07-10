package borges

import (
	"testing"

	"github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v3/memfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

func TestNewGitReferencer(t *testing.T) {
	fixtures.Init()
	defer fixtures.Clean()

	for _, ct := range ChangesFixtures {
		t.Run(ct.TestName, func(t *testing.T) {
			assert := assert.New(t)
			r, err := ct.NewRepository()
			assert.NoError(err)

			gitRefs := NewGitReferencer(r)
			resGitRefs, err := gitRefs.References()
			assert.NoError(err)
			assert.Equal(len(ct.NewReferences), len(resGitRefs))

			resGitRefsByName := refsByName(resGitRefs)
			expectedRefsByName := refsByName(ct.NewReferences)
			for name, expectedRef := range expectedRefsByName {
				obtainedRef, ok := resGitRefsByName[name]
				assert.True(ok)
				assert.Equal(expectedRef.Name, obtainedRef.Name)
				assert.Equal(expectedRef.Hash, obtainedRef.Hash)
				assert.Equal(expectedRef.Init, obtainedRef.Init)
				assert.Equal(expectedRef.Roots, obtainedRef.Roots)
			}
		})
	}
}

func TestNewGitReferencer_ReferenceToTagObject(t *testing.T) {
	fixtures.Init()
	defer fixtures.Clean()
	require := require.New(t)

	srcFs := fixtures.ByTag("tags").One().DotGit()
	sto, err := filesystem.NewStorage(srcFs)
	require.NoError(err)

	r, err := git.Open(sto, memfs.New())
	require.NoError(err)

	newRefs := NewGitReferencer(r)
	refs, err := newRefs.References()
	require.NoError(err)
	require.Len(refs, 4)
	for _, ref := range refs {
		require.Equal("f7b877701fbf855b44c0a9e86f3fdce2c298b07f", ref.Init.String())
	}
}
