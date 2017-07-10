package borges

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/satori/go.uuid"
	"github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/core-retrieval.v0/model"
	rrepository "gopkg.in/src-d/core-retrieval.v0/repository"
	"gopkg.in/src-d/core-retrieval.v0/test"
	"gopkg.in/src-d/go-billy.v3"
	"gopkg.in/src-d/go-billy.v3/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"gopkg.in/src-d/go-kallax.v1"
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
		s.T().Run(ct.TestName, func(t *testing.T) {
			var obtainedRefs []*model.Reference = ct.OldReferences
			for ic, cs := range ct.Changes { // emulate pushChangesToRootedRepositories() behaviour
				obtainedRefs = updateRepositoryReferences(obtainedRefs, cs, ic)
			}

			s.Equal(len(ct.NewReferences), len(obtainedRefs))
		})
	}
}

func (s *ArchiverSuite) TestFixtures() {
	for _, ct := range ChangesFixtures {
		s.T().Run(ct.TestName, func(t *testing.T) {
			require := require.New(t)

			tmp, err := ioutil.TempDir(os.TempDir(),
				fmt.Sprintf("borges-tests%d", rand.Uint32()))
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
			a := NewArchiver(s, tx, NewTemporaryCloner(tmpFs))

			a.Notifiers.Warn = func(j *Job, err error) {
				require.NoError(err, "job: %v", j)
			}

			or, err := ct.OldRepository()
			var rid kallax.ULID
			// emulate initial status of a repository
			err = WithInProcRepository(or, func(url string) error {
				mr := model.NewRepository()
				rid = mr.ID
				mr.Endpoints = append(mr.Endpoints, url)
				updated, err := s.Save(mr)
				require.NoError(err)
				require.False(updated)
				return a.Do(&Job{RepositoryID: uuid.UUID(mr.ID)})
			})
			require.NoError(err)

			nr, err := ct.NewRepository()
			require.NoError(err)

			err = WithInProcRepository(nr, func(url string) error {
				mr, err := s.FindOne(model.NewRepositoryQuery().FindByID(rid))
				require.NoError(err)
				mr.Endpoints = nil
				mr.Endpoints = append(mr.Endpoints, url)
				updated, err := s.Save(mr)
				require.NoError(err)
				require.True(updated, err)
				return a.Do(&Job{RepositoryID: uuid.UUID(mr.ID)})
			})
			require.NoError(err)

			checkNoFiles(t, txFs)
			checkNoFiles(t, tmpFs)

			checkReferences(t, nr, ct.NewReferences)

			// check references in database
			mr, err := s.FindOne(model.NewRepositoryQuery().FindByID(rid))
			require.NoError(err)
			checkReferencesInDB(t, mr, ct.NewReferences)

			// check references in siva files
			fis, err := rootedFs.ReadDir(".")
			if len(ct.NewReferences) != 0 {
				require.NoError(err)
				initHashesInStorage := make(map[string]bool)

				for _, fi := range fis {
					initHashesInStorage[strings.Replace(fi.Name(), ".siva", "", -1)] = true
				}

				// we check that all the references that we have into the database exists as a rooted repository
				for _, ref := range mr.References {
					_, ok := initHashesInStorage[ref.Init.String()]
					require.True(ok)
				}
			}
		})
	}
}

func newRepository(f *fixtures.Fixture) *git.Repository {
	fs := osfs.New(f.DotGit().Root())
	st, err := filesystem.NewStorage(fs)
	if err != nil {
		panic(err)
	}

	r, err := git.Open(st, fs)
	if err != nil {
		panic(err)
	}

	return r
}

func checkReferences(t *testing.T, obtained *git.Repository, refs []*model.Reference) {
	require := require.New(t)
	obtainedRefs := repoToMemRefs(t, obtained)
	expectedRefs := modelToMemRefs(t, refs)
	require.Equal(expectedRefs, obtainedRefs)
}

func checkReferencesInDB(t *testing.T, obtained *model.Repository, refs []*model.Reference) {
	require := require.New(t)
	require.Equal(len(refs), len(obtained.References))
	obtainedRefs := modelToMemRefs(t, obtained.References)
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
	refr := NewGitReferencer(r)
	refs, err := refr.References()
	require.NoError(err)
	return modelToMemRefs(t, refs)
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
