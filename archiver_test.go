package borges

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"srcd.works/core.v0/model"
	"srcd.works/go-git.v4"
	"srcd.works/go-git.v4/plumbing"
	"srcd.works/go-git.v4/plumbing/object"
)

func TestArchiverSuite(t *testing.T) {
	suite.Run(t, new(ArchiverSuite))
}

type ArchiverSuite struct {
	suite.Suite
	tmpDir    string
	endpoints []string

	lastCommit plumbing.Hash
}

func (s *ArchiverSuite) SetupSuite() {
	assert := assert.New(s.T())
	fixtures.Init()

	s.tmpDir = filepath.Join(os.TempDir(), "test_data")
	err := os.RemoveAll(s.tmpDir)
	assert.NoError(err)

	s.lastCommit = plumbing.NewHash("6ecf0ef2c2dffb796033e5a02219af86ec6584e5")

	s.endpoints = []string{fmt.Sprintf("file://%s", fixtures.Basic().One().DotGit().Base())}
}

func (s *ArchiverSuite) TearDownSuite() {
	assert := assert.New(s.T())

	err := fixtures.Clean()
	assert.NoError(err)

	err = os.RemoveAll(s.tmpDir)
	assert.NoError(err)
}

func (s *ArchiverSuite) TestCreateLocalRepository() {
	assert := assert.New(s.T())

	repo, err := createLocalRepository(s.tmpDir, s.endpoints, []*model.Reference{
		{
			Hash: model.NewSHA1("918c48b83bd081e863dbe1b80f8998f058cd8294"),
			Name: "refs/remotes/origin/master",
			Init: model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
		}, {
			// branch is up to date
			Hash: model.NewSHA1("e8d3ffab552895c19b9fcf7aa264d277cde33881"),
			Name: "refs/remotes/origin/branch",
			Init: model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
		},
	})
	assert.Nil(err)

	c, err := repo.Commit(s.lastCommit)
	assert.Nil(c)
	assert.Error(err)

	err = repo.Fetch(&git.FetchOptions{})
	assert.NoError(err)

	c, err = repo.Commit(s.lastCommit)
	assert.NoError(err)
	assert.NotNil(c)

	iter, err := repo.Objects()
	assert.NoError(err)
	assert.NotNil(iter)

	count := 0
	iter.ForEach(func(o object.Object) error {
		count++
		return nil
	})

	// 1- last commit into master
	// 2,3 - trees
	// 4 - file added into commit
	assert.Equal(4, count)
}
