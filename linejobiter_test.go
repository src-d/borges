package borges

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLineJobIter(t *testing.T) {
	assert := assert.New(t)

	text := `git://foo/bar.git
https://foo/baz.git`
	r := strings.NewReader(text)
	iter := NewLineJobIter(r)

	j, err := iter.Next()
	assert.NoError(err)
	assert.Equal(&Job{RepositoryID: 0, URL: "git://foo/bar.git"}, j)

	j, err = iter.Next()
	assert.NoError(err)
	assert.Equal(&Job{RepositoryID: 0, URL: "https://foo/baz.git"}, j)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)

	j, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(j)
}

func TestLineJobIterEmpty(t *testing.T) {
	assert := assert.New(t)

	r := strings.NewReader("")
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
	r := strings.NewReader(text)
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
	r := strings.NewReader(text)
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
