package borges

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

func TestArchiverSuite(t *testing.T) {
	suite.Run(t, new(ArchiverSuite))
}

type ArchiverSuite struct {
	suite.Suite
	a      *Archiver
	tmpDir string
	j      *Job

	lastCommit plumbing.Hash
}

func (s *ArchiverSuite) SetupSuite() {
	s.tmpDir = filepath.Join(os.TempDir(), "test_data")

	os.RemoveAll(s.tmpDir)

	s.lastCommit = plumbing.NewHash("6ecf0ef2c2dffb796033e5a02219af86ec6584e5")

	s.a = &Archiver{}

	s.j = &Job{
		URL: "git@github.com:git-fixtures/basic.git",
	}
}

func (s *ArchiverSuite) TestArchiver_CreateLocalRepository() {
	assert := assert.New(s.T())

	repo, err := s.a.createLocalRepository(s.tmpDir, s.j, []*Reference{
		{
			Hash: NewSHA1("918c48b83bd081e863dbe1b80f8998f058cd8294"),
			Name: "refs/remotes/origin/master",
			Init: NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
		}, {
			// branch is up to date
			Hash: NewSHA1("e8d3ffab552895c19b9fcf7aa264d277cde33881"),
			Name: "refs/remotes/origin/branch",
			Init: NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
		},
	})
	assert.Nil(err)

	c, err := repo.Commit(s.lastCommit)
	assert.Nil(c)
	assert.NotNil(err)

	err = repo.Fetch(&git.FetchOptions{})
	assert.Nil(err)

	c, err = repo.Commit(s.lastCommit)
	assert.Nil(err)
	assert.NotNil(c)

	iter, err := repo.Objects()
	assert.Nil(err)
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
