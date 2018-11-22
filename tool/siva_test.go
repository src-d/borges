package tool

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
)

func TestSiva(t *testing.T) {
	tmp, err := ioutil.TempDir("", "borges")
	require.NoError(t, err)

	fs := osfs.New(tmp)
	suite.Run(t, &SivaSuite{FS: fs, bucket: 0})
	suite.Run(t, &SivaSuite{FS: fs, bucket: 2})
}

type SivaSuite struct {
	ToolSuite
	FS       billy.Basic
	database *Database
	bucket   int
}

func (s *SivaSuite) SetupTest() {
	s.ToolSuite.FS = s.FS
	s.ToolSuite.bucket = s.bucket
	s.ToolSuite.SetupTest()
	s.database = NewDatabase(s.DB)
}

func (s *SivaSuite) TearDownTest() {
	s.ToolSuite.TearDownTest()
}

func (s *SivaSuite) TestSivaAll() {
	c := sivaCase{
		err:   false,
		list:  inits,
		sivas: nil,
		queue: ulid,
	}

	s.testDelete(c)
}

func (s *SivaSuite) TestSivaDry() {
	c := sivaCase{
		err:   false,
		dry:   true,
		list:  inits,
		sivas: inits,
		queue: ulid,
	}

	s.testDelete(c)
}

func (s *SivaSuite) TestSivaRefInAllRepos() {
	c := sivaCase{
		err:   false,
		list:  inits[:1],
		sivas: inits[1:],
		queue: ulid,
	}

	s.testDelete(c)
}

func (s *SivaSuite) TestSivaMultiple() {
	c := sivaCase{
		err:   false,
		list:  inits[4:],
		sivas: inits[:4],
		queue: ulid[4:],
	}

	s.testDelete(c)
}

func (s *SivaSuite) TestSivaError() {
	name := fmt.Sprintf("%s.siva", bucketPath(inits[0], s.bucket))
	err := s.testFS.Remove(name)
	s.NoError(err)

	c := sivaCase{
		err:   true,
		list:  inits,
		sivas: nil,
		queue: ulid,
	}

	s.testDelete(c)
}

type sivaCase struct {
	err   bool
	dry   bool
	list  []string
	sivas []string
	queue []string
}

func (s *SivaSuite) testDelete(c sivaCase) {
	buffer := new(bytes.Buffer)

	siva := NewSiva(s.database, s.testFS)
	siva.WriteQueue(buffer)
	siva.DefaultErrors("testing", false)
	siva.Bucket(s.bucket)
	siva.Dry(c.dry)

	err := siva.Delete(context.TODO(), c.list)
	if c.err {
		s.Error(err)
		return
	}

	s.NoError(err)

	sivas, err := s.database.Siva()
	s.NoError(err)
	s.ElementsMatch(sivas, c.sivas)

	// list of sivas files to delete
	if !c.dry {
		for _, f := range c.list {
			name := fmt.Sprintf("%s.siva", bucketPath(f, s.bucket))
			_, err = s.testFS.Stat(name)
			s.Error(err)
		}
	}

	// rest of siva files
	for _, f := range c.sivas {
		name := fmt.Sprintf("%s.siva", bucketPath(f, s.bucket))
		_, err = s.testFS.Stat(name)
		s.NoError(err)
	}

	repos := strings.Split(buffer.String(), "\n")
	set := NewSet(false)
	for _, r := range repos {
		if r != "" {
			set.Add(r)
		}
	}

	s.ElementsMatch(set.List(), c.queue)
}
