package tool

import (
	"testing"

	"github.com/stretchr/testify/suite"
	billy "gopkg.in/src-d/go-billy.v4"
)

func TestDatabase(t *testing.T) {
	suite.Run(t, new(DatabaseSuite))
}

type DatabaseSuite struct {
	ToolSuite
	database *Database
	FS       billy.Basic
}

func (s *DatabaseSuite) SetupTest() {
	s.ToolSuite.FS = s.FS
	s.ToolSuite.SetupTest()
	s.database = NewDatabase(s.DB)
}

func (s *DatabaseSuite) TearDownTest() {
	s.ToolSuite.TearDownTest()
}

func (s *DatabaseSuite) TestSiva() {
	siva, err := s.database.Siva()
	s.NoError(err)
	s.Len(siva, len(inits))
	s.ElementsMatch(siva, inits)
}

func (s *DatabaseSuite) TestRepositoriesWithInit() {
	repos, err := s.database.RepositoriesWithInit(inits[0])
	s.NoError(err)
	s.Len(repos, len(testRepos))
	s.ElementsMatch(repos, ulid)

	repos, err = s.database.RepositoriesWithInit(inits[1])
	s.NoError(err)
	s.Len(repos, 1)
	s.ElementsMatch(repos, ulid[1:2])
}
