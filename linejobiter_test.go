package borges

import (
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestLineJobIter(t *testing.T) {
	// TODO this test will be refactored in the future to check if the iterator
	// saves the urls into the database
	t.Skip()

	assert := assert.New(t)

	text := `git://foo/bar.git
https://foo/baz.git`
	r := ioutil.NopCloser(strings.NewReader(text))
	iter := NewLineJobIter(r)

	j, err := iter.Next()
	assert.NoError(err)
	assert.Equal(&Job{RepositoryID: uuid.Nil}, j)

	j, err = iter.Next()
	assert.NoError(err)
	assert.Equal(&Job{RepositoryID: uuid.Nil}, j)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)
}

func TestLineJobIterEmpty(t *testing.T) {
	assert := assert.New(t)

	r := ioutil.NopCloser(strings.NewReader(""))
	iter := NewLineJobIter(r)

	j, err := iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)
}

func TestLineJobIterNonAbsoluteURL(t *testing.T) {
	assert := assert.New(t)

	text := "foo"
	r := ioutil.NopCloser(strings.NewReader(text))
	iter := NewLineJobIter(r)

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
	assert := assert.New(t)

	text := "://"
	r := ioutil.NopCloser(strings.NewReader(text))
	iter := NewLineJobIter(r)

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
