package borges

import (
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/satori/go.uuid"
	"gopkg.in/src-d/go-kallax.v1"
	"github.com/stretchr/testify/require"
	"srcd.works/core.v0"
	"srcd.works/core.v0/model"
)

func TestLineJobIter(t *testing.T) {
	assert := require.New(t)

	text := `git://foo/bar.git
https://foo/baz.git`
	r := ioutil.NopCloser(strings.NewReader(text))

	DropTables("repositories")
	CreateRepositoryTable()
	storer := core.ModelRepositoryStore()

	iter := NewLineJobIter(r, storer)

	j, err := iter.Next()
	assert.NoError(err)
	ID, err := getIDByEndpoint("git://foo/bar.git", storer)
	assert.NoError(err)
	assert.Equal(&Job{RepositoryID: ID}, j)

	j, err = iter.Next()
	assert.NoError(err)
	ID, err = getIDByEndpoint("https://foo/baz.git", storer)
	assert.NoError(err)
	assert.Equal(&Job{RepositoryID: ID}, j)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)
}

func TestLineJobIterEmpty(t *testing.T) {
	assert := require.New(t)

	r := ioutil.NopCloser(strings.NewReader(""))
	iter := NewLineJobIter(r, nil)

	j, err := iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)
}

func TestLineJobIterNonAbsoluteURL(t *testing.T) {
	assert := require.New(t)

	text := "foo"
	r := ioutil.NopCloser(strings.NewReader(text))

	DropTables("repository")
	DropIndexes("idx_endpoints")
	CreateRepositoryTable()
	storer := core.ModelRepositoryStore()

	iter := NewLineJobIter(r, storer)

	j, err := iter.Next()
	assert.Error(err)
	assert.NotEqual(io.EOF, err)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)
}

func TestLineJobIterBadURL(t *testing.T) {
	assert := require.New(t)

	text := "://"
	r := ioutil.NopCloser(strings.NewReader(text))

	DropTables("repository")
	DropIndexes("idx_endpoints")
	CreateRepositoryTable()
	storer := core.ModelRepositoryStore()

	iter := NewLineJobIter(r, storer)

	j, err := iter.Next()
	assert.Error(err)
	assert.NotEqual(io.EOF, err)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)
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
