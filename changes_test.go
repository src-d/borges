package borges

import (
	"testing"

	"github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"srcd.works/core.v0/models"
	"srcd.works/go-billy.v1/memfs"
	"srcd.works/go-git.v4"
	"srcd.works/go-git.v4/storage/filesystem"
)

func TestChangesSuite(t *testing.T) {
	suite.Run(t, new(ChangesSuite))
}

type ChangesSuite struct {
	suite.Suite
	r     *git.Repository
	aHash string
	bHash string
}

func (s *ChangesSuite) SetupTest() {
	assert := assert.New(s.T())

	fixtures.Init()

	srcFs := fixtures.ByTag("root-reference").One().DotGit()
	sto, err := filesystem.NewStorage(srcFs)
	assert.NoError(err)

	r, err := git.Open(sto, memfs.New())
	assert.NoError(err)
	assert.NotNil(r)
	s.r = r

	s.aHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	s.bHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
}

func (s *ChangesSuite) TearDownTest() {
	fixtures.Clean()
}

func (s *ChangesSuite) TestNewChanges_AllReferencesAreNew() {
	s.check(&ChangesFixture{
		Repository:      s.r,
		InitCommitCount: 7,
		OldReferences:   nil,
		Expected: []*RefFixture{
			getByName("refs/heads/master", RefFixtures).Create(),
			getByName("refs/heads/branch", RefFixtures).Create(),

			getByName("refs/heads/1", RefFixtures).Create(),
			getByName("refs/heads/2", RefFixtures).Create(),
			getByName("refs/heads/3", RefFixtures).Create(),

			getByName("refs/heads/functionalityOne", RefFixtures).Create(),
			getByName("refs/heads/functionalityTwo", RefFixtures).Create(),

			getByName("refs/heads/rootReference", RefFixtures).Create(),

			getByName("refs/tags/v1.0.0", RefFixtures).Create(),
		},
	})
}

func (s *ChangesSuite) TestNewChanges_AllReferencesAreOld() {
	s.check(&ChangesFixture{
		Repository:      s.r,
		InitCommitCount: 0,
		OldReferences: []*models.Reference{
			getByName("refs/heads/master", RefFixtures).ToRef(),
			getByName("refs/heads/branch", RefFixtures).ToRef(),

			getByName("refs/heads/1", RefFixtures).ToRef(),
			getByName("refs/heads/2", RefFixtures).ToRef(),
			getByName("refs/heads/3", RefFixtures).ToRef(),

			getByName("refs/heads/functionalityOne", RefFixtures).ToRef(),
			getByName("refs/heads/functionalityTwo", RefFixtures).ToRef(),

			getByName("refs/heads/rootReference", RefFixtures).ToRef(),

			getByName("refs/tags/v1.0.0", RefFixtures).ToRef(),
		},
		Expected: nil,
	})
}

func (s *ChangesSuite) TestNewChanges_TwoOldReferences() {
	s.check(&ChangesFixture{
		Repository:      s.r,
		InitCommitCount: 6,
		OldReferences: []*models.Reference{
			getByName("refs/heads/master", RefFixtures).ToRef(),
			getByName("refs/heads/1", RefFixtures).ToRef(),
		},
		Expected: []*RefFixture{
			getByName("refs/heads/branch", RefFixtures).Create(),

			getByName("refs/heads/2", RefFixtures).Create(),
			getByName("refs/heads/3", RefFixtures).Create(),

			getByName("refs/heads/functionalityOne", RefFixtures).Create(),
			getByName("refs/heads/functionalityTwo", RefFixtures).Create(),

			getByName("refs/heads/rootReference", RefFixtures).Create(),

			getByName("refs/tags/v1.0.0", RefFixtures).Create(),
		},
	})
}

func (s *ChangesSuite) TestNewChanges_UpdateAReference() {
	s.check(&ChangesFixture{
		Repository:      s.r,
		InitCommitCount: 7,
		OldReferences: []*models.Reference{
			getByName("refs/heads/master", RefFixtures).WithHead(s.aHash).ToRef(),
		},
		Expected: []*RefFixture{
			getByName("refs/heads/master", RefFixtures).Update(),
			getByName("refs/heads/branch", RefFixtures).Create(),

			getByName("refs/heads/1", RefFixtures).Create(),
			getByName("refs/heads/2", RefFixtures).Create(),
			getByName("refs/heads/3", RefFixtures).Create(),

			getByName("refs/heads/functionalityOne", RefFixtures).Create(),
			getByName("refs/heads/functionalityTwo", RefFixtures).Create(),

			getByName("refs/heads/rootReference", RefFixtures).Create(),

			getByName("refs/tags/v1.0.0", RefFixtures).Create(),
		},
	})
}
func (s *ChangesSuite) TestNewChanges_RootCommitChanges() {
	refRootChange := getByName("refs/heads/master", RefFixtures).WithRoots(s.aHash)

	s.check(&ChangesFixture{
		Repository:      s.r,
		InitCommitCount: 8,
		OldReferences: []*models.Reference{
			refRootChange.ToRef(),
		},
		Expected: []*RefFixture{
			refRootChange.Delete(),

			getByName("refs/heads/master", RefFixtures).Create(),
			getByName("refs/heads/branch", RefFixtures).Create(),

			getByName("refs/heads/1", RefFixtures).Create(),
			getByName("refs/heads/2", RefFixtures).Create(),
			getByName("refs/heads/3", RefFixtures).Create(),

			getByName("refs/heads/functionalityOne", RefFixtures).Create(),
			getByName("refs/heads/functionalityTwo", RefFixtures).Create(),

			getByName("refs/heads/rootReference", RefFixtures).Create(),

			getByName("refs/tags/v1.0.0", RefFixtures).Create(),
		},
	})
}

func (s *ChangesSuite) TestNewChanges_RootCommitsChangeFromTwoToOne() {
	refRootChange := getByName("refs/heads/master", RefFixtures).
		WithRoots(s.aHash, s.bHash)

	s.check(&ChangesFixture{
		Repository:      s.r,
		InitCommitCount: 8,
		OldReferences: []*models.Reference{
			refRootChange.ToRef(),
		},
		Expected: []*RefFixture{
			getByName("refs/heads/master", RefFixtures).WithRoots(s.aHash, s.bHash).Delete(),

			getByName("refs/heads/master", RefFixtures).Create(),
			getByName("refs/heads/branch", RefFixtures).Create(),

			getByName("refs/heads/1", RefFixtures).Create(),
			getByName("refs/heads/2", RefFixtures).Create(),
			getByName("refs/heads/3", RefFixtures).Create(),

			getByName("refs/heads/functionalityOne", RefFixtures).Create(),
			getByName("refs/heads/functionalityTwo", RefFixtures).Create(),

			getByName("refs/heads/rootReference", RefFixtures).Create(),

			getByName("refs/tags/v1.0.0", RefFixtures).Create(),
		},
	})
}

func (s *ChangesSuite) TestNewChanges_EmptyRepository() {
	s.check(&ChangesFixture{
		Repository:      newEmptyRepository(),
		InitCommitCount: 0,
		OldReferences:   nil,
		Expected:        nil,
	})
}

func (s *ChangesSuite) TestNewChanges_EmptyRepositoryPreviousReferences() {
	s.check(&ChangesFixture{
		Repository:      newEmptyRepository(),
		InitCommitCount: 1,
		OldReferences: []*models.Reference{
			getByName("refs/heads/master", RefFixtures).ToRef(),
		},
		Expected: []*RefFixture{
			getByName("refs/heads/master", RefFixtures).Delete(),
		},
	})
}

func (s *ChangesSuite) check(ct *ChangesFixture) {
	assert := assert.New(s.T())
	require := require.New(s.T())

	require.NotNil(ct.Repository)

	changes, err := NewChanges(ct.OldReferences, ct.Repository)
	assert.Nil(err)
	assert.Equal(ct.InitCommitCount, len(changes))

	for fc, commands := range changes {
		s.T().Log("InitCommit: ", fc)
		sbi := withInitCommit(fc, ct.Expected)
		assert.Equal(len(sbi), len(commands))

		for _, com := range commands {
			s.T().Log("Command: ", com)
			rs := getByName(getName(com), sbi)
			s.T().Log("RefSpec: ", rs)
			assert.NotNil(rs)
			assert.Equal(rs.Action, com.Action())
			switch com.Action() {
			case Create:
				assert.Nil(com.Old)
				assert.Equal(rs.Head, com.New.Hash.String())
			case Update:
				assert.Equal(com.Old.Hash, getRefByName(rs.Name, ct.OldReferences).Hash)
				assert.NotEqual(rs.Head, com.Old.Hash.String())
				assert.Equal(rs.Head, com.New.Hash.String())
			case Delete:
				assert.Nil(com.New)
				assert.NotNil(com.Old)
				assert.Equal(rs.Head, com.Old.Hash.String())
			default:
				assert.Fail("Unexpected Action value")
			}
		}
	}
}
