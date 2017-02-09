package borges

import (
	"encoding/hex"
	"testing"

	"github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"srcd.works/core.v0/models"
	"srcd.works/go-billy.v1/memfs"
	"srcd.works/go-git.v4"
	"srcd.works/go-git.v4/storage/filesystem"
	"srcd.works/go-git.v4/storage/memory"
)

func TestChangesSuite(t *testing.T) {
	suite.Run(t, new(ChangesSuite))
}

type ChangesSuite struct {
	suite.Suite
	r     *git.Repository
	specs []*refFixture
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

	s.specs = []*refFixture{
		{
			Name: "refs/heads/master",
			Head: "6ecf0ef2c2dffb796033e5a02219af86ec6584e5",
			Roots: []string{
				"b029517f6300c2da0f4b651b8642506cd6aaf45d",
			},
		}, {
			Name: "refs/heads/branch",
			Head: "e8d3ffab552895c19b9fcf7aa264d277cde33881",
			Roots: []string{
				"b029517f6300c2da0f4b651b8642506cd6aaf45d",
			},
		}, {
			Name: "refs/heads/1",
			Head: "caf05fe371a5a6feab588a73ebd9ac73abdd072c",
			Roots: []string{
				"8ec19d64748c54c6d047f30c81b4c444a8232b41",
				"04fffad6eacd4512554cb22ca3a0d6b8a38a96cc",
				"058cec4b81e8f0a9c3763e0671bbfba0666a4b33",
			},
		}, {
			Name: "refs/heads/2",
			Head: "04fffad6eacd4512554cb22ca3a0d6b8a38a96cc",
			Roots: []string{
				"04fffad6eacd4512554cb22ca3a0d6b8a38a96cc",
			},
		}, {
			Name: "refs/heads/3",
			Head: "058cec4b81e8f0a9c3763e0671bbfba0666a4b33",
			Roots: []string{
				"058cec4b81e8f0a9c3763e0671bbfba0666a4b33",
			},
		}, {
			Name: "refs/heads/functionalityOne",
			Head: "ca858bfd043ac70bf532d53a4031be0cdf7483b4",
			Roots: []string{
				"5e4661353b435315edb0aab7a472bd43c82fed5c",
			},
		}, {
			Name: "refs/heads/functionalityTwo",
			Head: "79b3db5091672bcb9da2704a2d7b269bcd1ef36f",
			Roots: []string{
				"8829746417d76e7a64e540e906abcb7970679e47",
				"5e4661353b435315edb0aab7a472bd43c82fed5c",
			},
		}, {
			Name: "refs/heads/rootReference",
			Head: "a135c3e77219a8eaf166a643f6ce3192e97b7e5e",
			Roots: []string{
				"a135c3e77219a8eaf166a643f6ce3192e97b7e5e",
			},
		}, {
			Name: "refs/tags/v1.0.0",
			Head: "6ecf0ef2c2dffb796033e5a02219af86ec6584e5",
			Roots: []string{
				"b029517f6300c2da0f4b651b8642506cd6aaf45d",
			},
		},
	}
}

func (s *ChangesSuite) TearDownTest() {
	fixtures.Clean()
}

func (s *ChangesSuite) TestNewChanges_AllReferencesAreNew() {
	s.check(&changesTest{
		Repository:      s.r,
		InitCommitCount: 7,
		OldReferences:   nil,
		Expected: []*refFixture{
			getByName("refs/heads/master", s.specs).Create(),
			getByName("refs/heads/branch", s.specs).Create(),

			getByName("refs/heads/1", s.specs).Create(),
			getByName("refs/heads/2", s.specs).Create(),
			getByName("refs/heads/3", s.specs).Create(),

			getByName("refs/heads/functionalityOne", s.specs).Create(),
			getByName("refs/heads/functionalityTwo", s.specs).Create(),

			getByName("refs/heads/rootReference", s.specs).Create(),

			getByName("refs/tags/v1.0.0", s.specs).Create(),
		},
	})
}

func (s *ChangesSuite) TestNewChanges_AllReferencesAreOld() {
	s.check(&changesTest{
		Repository:      s.r,
		InitCommitCount: 0,
		OldReferences: []*models.Reference{
			getByName("refs/heads/master", s.specs).ToRef(),
			getByName("refs/heads/branch", s.specs).ToRef(),

			getByName("refs/heads/1", s.specs).ToRef(),
			getByName("refs/heads/2", s.specs).ToRef(),
			getByName("refs/heads/3", s.specs).ToRef(),

			getByName("refs/heads/functionalityOne", s.specs).ToRef(),
			getByName("refs/heads/functionalityTwo", s.specs).ToRef(),

			getByName("refs/heads/rootReference", s.specs).ToRef(),

			getByName("refs/tags/v1.0.0", s.specs).ToRef(),
		},
		Expected: nil,
	})
}

func (s *ChangesSuite) TestNewChanges_TwoOldReferences() {
	s.check(&changesTest{
		Repository:      s.r,
		InitCommitCount: 6,
		OldReferences: []*models.Reference{
			getByName("refs/heads/master", s.specs).ToRef(),
			getByName("refs/heads/1", s.specs).ToRef(),
		},
		Expected: []*refFixture{
			getByName("refs/heads/branch", s.specs).Create(),

			getByName("refs/heads/2", s.specs).Create(),
			getByName("refs/heads/3", s.specs).Create(),

			getByName("refs/heads/functionalityOne", s.specs).Create(),
			getByName("refs/heads/functionalityTwo", s.specs).Create(),

			getByName("refs/heads/rootReference", s.specs).Create(),

			getByName("refs/tags/v1.0.0", s.specs).Create(),
		},
	})
}

func (s *ChangesSuite) TestNewChanges_UpdateAReference() {
	s.check(&changesTest{
		Repository:      s.r,
		InitCommitCount: 7,
		OldReferences: []*models.Reference{
			getByName("refs/heads/master", s.specs).WithHead(s.aHash).ToRef(),
		},
		Expected: []*refFixture{
			getByName("refs/heads/master", s.specs).Update(),
			getByName("refs/heads/branch", s.specs).Create(),

			getByName("refs/heads/1", s.specs).Create(),
			getByName("refs/heads/2", s.specs).Create(),
			getByName("refs/heads/3", s.specs).Create(),

			getByName("refs/heads/functionalityOne", s.specs).Create(),
			getByName("refs/heads/functionalityTwo", s.specs).Create(),

			getByName("refs/heads/rootReference", s.specs).Create(),

			getByName("refs/tags/v1.0.0", s.specs).Create(),
		},
	})
}
func (s *ChangesSuite) TestNewChanges_RootCommitChanges() {
	refRootChange := getByName("refs/heads/master", s.specs).WithRoots(s.aHash)

	s.check(&changesTest{
		Repository:      s.r,
		InitCommitCount: 8,
		OldReferences: []*models.Reference{
			refRootChange.ToRef(),
		},
		Expected: []*refFixture{
			refRootChange.Delete(),

			getByName("refs/heads/master", s.specs).Create(),
			getByName("refs/heads/branch", s.specs).Create(),

			getByName("refs/heads/1", s.specs).Create(),
			getByName("refs/heads/2", s.specs).Create(),
			getByName("refs/heads/3", s.specs).Create(),

			getByName("refs/heads/functionalityOne", s.specs).Create(),
			getByName("refs/heads/functionalityTwo", s.specs).Create(),

			getByName("refs/heads/rootReference", s.specs).Create(),

			getByName("refs/tags/v1.0.0", s.specs).Create(),
		},
	})
}

func (s *ChangesSuite) TestNewChanges_RootCommitsChangeFromTwoToOne() {
	refRootChange := getByName("refs/heads/master", s.specs).
		WithRoots(s.aHash, s.bHash)

	s.check(&changesTest{
		Repository:      s.r,
		InitCommitCount: 8,
		OldReferences: []*models.Reference{
			refRootChange.ToRef(),
		},
		Expected: []*refFixture{
			getByName("refs/heads/master", s.specs).WithRoots(s.aHash, s.bHash).Delete(),

			getByName("refs/heads/master", s.specs).Create(),
			getByName("refs/heads/branch", s.specs).Create(),

			getByName("refs/heads/1", s.specs).Create(),
			getByName("refs/heads/2", s.specs).Create(),
			getByName("refs/heads/3", s.specs).Create(),

			getByName("refs/heads/functionalityOne", s.specs).Create(),
			getByName("refs/heads/functionalityTwo", s.specs).Create(),

			getByName("refs/heads/rootReference", s.specs).Create(),

			getByName("refs/tags/v1.0.0", s.specs).Create(),
		},
	})
}

func (s *ChangesSuite) TestNewChanges_EmptyRepository() {
	s.check(&changesTest{
		Repository:      newEmptyRepository(),
		InitCommitCount: 0,
		OldReferences:   nil,
		Expected:        nil,
	})
}

func (s *ChangesSuite) TestNewChanges_EmptyRepositoryPreviousReferences() {
	s.check(&changesTest{
		Repository:      newEmptyRepository(),
		InitCommitCount: 1,
		OldReferences: []*models.Reference{
			getByName("refs/heads/master", s.specs).ToRef(),
		},
		Expected: []*refFixture{
			getByName("refs/heads/master", s.specs).Delete(),
		},
	})
}

func (s *ChangesSuite) check(ct *changesTest) {
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

type changesTest struct {
	Repository      *git.Repository
	InitCommitCount int
	OldReferences   []*models.Reference
	Expected        []*refFixture
}

type refFixture struct {
	Name   string
	Head   string
	Roots  []string
	Action Action
}

func (rs *refFixture) Create() *refFixture {
	return rs.withAction(Create)
}

func (rs *refFixture) Update() *refFixture {
	return rs.withAction(Update)
}

func (rs *refFixture) Delete() *refFixture {
	return rs.withAction(Delete)
}

func (rs *refFixture) withAction(a Action) *refFixture {
	return &refFixture{
		Head:   rs.Head,
		Name:   rs.Name,
		Action: a,
		Roots:  rs.Roots,
	}
}

func (rs *refFixture) WithHead(hash string) *refFixture {
	return &refFixture{
		Head:   hash,
		Name:   rs.Name,
		Action: rs.Action,
		Roots:  rs.Roots,
	}
}

func (rs *refFixture) WithRoots(roots ...string) *refFixture {
	return &refFixture{
		Head:   rs.Head,
		Name:   rs.Name,
		Action: rs.Action,
		Roots:  roots,
	}
}

func (rs *refFixture) ToRef() *models.Reference {
	roots := rs.toHash(rs.Roots...)
	return &models.Reference{
		Roots: roots,
		Init:  roots[0],
		Hash:  rs.toHash(rs.Head)[0],
		Name:  rs.Name,
	}
}

func (rs *refFixture) toHash(hs ...string) []models.SHA1 {
	var result []models.SHA1
	for _, h := range hs {
		b, _ := hex.DecodeString(h)
		var h models.SHA1
		copy(h[:], b)

		result = append(result, h)
	}

	return result
}

func withInitCommit(initCommit models.SHA1, r []*refFixture) []*refFixture {
	ic := initCommit.String()

	var result []*refFixture
	for _, sr := range r {
		if sr.Roots[0] == ic {
			result = append(result, sr)
		}
	}

	return result
}

func getName(c *Command) string {
	var name string
	switch c.Action() {
	case Create, Update:
		name = c.New.Name
	case Delete:
		name = c.Old.Name
	}

	return name
}

func getByName(name string, r []*refFixture) *refFixture {
	for _, sr := range r {
		if sr.Name == name {
			return sr
		}
	}

	panic("not found: " + name)
}

func getRefByName(name string, r []*models.Reference) *models.Reference {
	for _, sr := range r {
		if sr.Name == name {
			return sr
		}
	}

	panic("not found: " + name)
}

func newEmptyRepository() *git.Repository {
	r, err := git.Init(memory.NewStorage(), nil)
	if err != nil {
		panic(err)
	}

	return r
}
