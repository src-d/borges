package borges

import (
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/core.v0/model"
	"gopkg.in/src-d/core.v0/test"
	"gopkg.in/src-d/go-kallax.v1"
)

func TestLineJobIter(t *testing.T) {
	suite.Run(t, new(LineJobIterSuite))
}

type LineJobIterSuite struct {
	test.Suite
}

func (s *LineJobIterSuite) SetupTest() {
	s.Suite.Setup()
}

func (s *LineJobIterSuite) TearDownTest() {
	s.Suite.TearDown()
}

func (s *LineJobIterSuite) TestGetJobsWithTwoRepos() {
	text := `git://foo/bar.git
https://foo/baz.git`
	r := ioutil.NopCloser(strings.NewReader(text))

	storer := model.NewRepositoryStore(s.DB)

	iter := NewLineJobIter(r, storer)

	j, err := iter.Next()
	s.NoError(err)
	ID, err := getIDByEndpoint("git://foo/bar.git", storer)
	s.NoError(err)
	s.Equal(&Job{RepositoryID: ID}, j)

	j, err = iter.Next()
	s.NoError(err)
	ID, err = getIDByEndpoint("https://foo/baz.git", storer)
	s.NoError(err)
	s.Equal(&Job{RepositoryID: ID}, j)

	j, err = iter.Next()
	s.Equal(io.EOF, err)
	s.Nil(j)

	j, err = iter.Next()
	s.Equal(io.EOF, err)
	s.Nil(j)
}

func (s *LineJobIterSuite) TestEmpty() {
	r := ioutil.NopCloser(strings.NewReader(""))
	iter := NewLineJobIter(r, nil)

	j, err := iter.Next()
	s.Equal(io.EOF, err)
	s.Nil(j)

	j, err = iter.Next()
	s.Equal(io.EOF, err)
	s.Nil(j)
}

func (s *LineJobIterSuite) TestNonAbsoluteURL() {
	text := "foo"
	r := ioutil.NopCloser(strings.NewReader(text))

	storer := model.NewRepositoryStore(s.DB)

	iter := NewLineJobIter(r, storer)

	j, err := iter.Next()
	s.Error(err)
	s.NotEqual(io.EOF, err)

	j, err = iter.Next()
	s.Equal(io.EOF, err)
	s.Nil(j)

	j, err = iter.Next()
	s.Equal(io.EOF, err)
	s.Nil(j)
}

func (s *LineJobIterSuite) TestBadURL() {
	text := "://"
	r := ioutil.NopCloser(strings.NewReader(text))

	storer := model.NewRepositoryStore(s.DB)

	iter := NewLineJobIter(r, storer)

	j, err := iter.Next()
	s.Error(err)
	s.NotEqual(io.EOF, err)

	j, err = iter.Next()
	s.Equal(io.EOF, err)
	s.Nil(j)

	j, err = iter.Next()
	s.Equal(io.EOF, err)
	s.Nil(j)
}

func getIDByEndpoint(endpoint string, store *model.RepositoryStore) (uuid.UUID, error) {
	q := model.NewRepositoryQuery().
		Where(kallax.ArrayContains(model.Schema.Repository.Endpoints, endpoint))
	r, err := store.FindOne(q)
	if err != nil {
		return uuid.Nil, err
	}

	return uuid.UUID(r.ID), nil
}
