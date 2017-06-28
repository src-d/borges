package borges

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/satori/go.uuid"
	"github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	rrepository "gopkg.in/src-d/core-retrieval.v0/repository"
	"gopkg.in/src-d/core.v0/model"
	"gopkg.in/src-d/core.v0/test"
	"gopkg.in/src-d/go-billy.v3"
	"gopkg.in/src-d/go-billy.v3/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/storage/memory"
)

func TestArchiver(t *testing.T) {
	suite.Run(t, new(ArchiverSuite))
}

type ArchiverSuite struct {
	test.Suite
}

func (s *ArchiverSuite) SetupTest() {
	fixtures.Init()
	s.Suite.Setup()
}

func (s *ArchiverSuite) TearDownTest() {
	s.Suite.TearDown()
	fixtures.Clean()
}

func (s *ArchiverSuite) TestReferenceUpdate() {
	for _, ct := range ChangesFixtures {
		if ct.FakeHashes {
			s.T().Run(ct.TestName, func(t *testing.T) {
				assert := assert.New(t)

				references := ct.OldReferences
				for _, cs := range ct.Changes { // emulate pushChangesToRootedRepositories() behaviour
					references = updateRepositoryReferences(references, cs)
				}

				assert.Equal(len(ct.NewReferences), len(references))
			})
		}
	}
}

func (s *ArchiverSuite) TestFixtures() {
	for _, ct := range ChangesFixtures {
		if ct.FakeHashes {
			continue
		}

		s.T().Run(ct.TestName, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)

			tmp, err := ioutil.TempDir(os.TempDir(),
				fmt.Sprintf("borge-tests%d", rand.Uint32()))
			require.NoError(err)
			defer func() { require.NoError(os.RemoveAll(tmp)) }()

			fs := osfs.New(tmp)

			rootedFs, err := fs.Chroot("rooted")
			require.NoError(err)
			txFs, err := fs.Chroot("tx")
			require.NoError(err)
			tmpFs, err := fs.Chroot("tmp")
			require.NoError(err)

			s := model.NewRepositoryStore(s.DB)
			tx := rrepository.NewSivaRootedTransactioner(rootedFs, txFs)
			a := NewArchiver(s, tx, tmpFs)

			a.Notifiers.Warn = func(j *Job, err error) {
				assert.NoError(err, "job: %v", j)
			}

			nr, err := ct.NewRepository()
			require.NoError(err)

			err = withInProcRepository(nr, func(url string) error {
				mr := model.NewRepository()
				mr.Endpoints = append(mr.Endpoints, url)
				mr.References = ct.OldReferences
				updated, err := s.Save(mr)
				require.NoError(err)
				require.False(updated)

				return a.Do(&Job{RepositoryID: uuid.UUID(mr.ID)})
			})
			require.NoError(err)

			checkNoFiles(t, txFs)
			checkNoFiles(t, tmpFs)

			checkReferences(t, nr, ct.NewReferences)
		})
	}
}

func checkReferences(t *testing.T, obtained *git.Repository, refs []*model.Reference) {
	require := require.New(t)
	obtainedRefs := repoToMemRefs(t, obtained)
	expectedRefs := modelToMemRefs(t, refs)
	require.Equal(expectedRefs, obtainedRefs)
}

func modelToMemRefs(t *testing.T, refs []*model.Reference) memory.ReferenceStorage {
	require := require.New(t)
	m := memory.ReferenceStorage{}
	for _, ref := range refs {
		err := m.SetReference(ref.GitReference())
		require.NoError(err)
	}

	return m
}

func repoToMemRefs(t *testing.T, r *git.Repository) memory.ReferenceStorage {
	require := require.New(t)
	m := memory.ReferenceStorage{}
	iter, err := r.References()
	require.NoError(err)

	err = iter.ForEach(func(r *plumbing.Reference) error {
		if r.Type() != plumbing.HashReference {
			return nil
			//TODO: check this does not happen
		}

		return m.SetReference(r)
	})
	require.NoError(err)
	return m
}

func checkNoFiles(t *testing.T, fs billy.Filesystem) {
	require := require.New(t)

	fis, err := fs.ReadDir("")
	if !os.IsNotExist(err) {
		require.NoError(err)
	}

	for _, fi := range fis {
		require.True(fi.IsDir(), "unexpected file: %s", fi.Name())

		fsr, err := fs.Chroot(fi.Name())
		require.NoError(err)
		checkNoFiles(t, fsr)
	}
}
