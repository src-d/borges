package tool

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
)

func TestRebucket(t *testing.T) {
	tmp, err := ioutil.TempDir("", "borges")
	require.NoError(t, err)

	fs := osfs.New(tmp)
	suite.Run(t, &RebucketSuite{FS: fs})
}

type RebucketSuite struct {
	ToolSuite
	FS billy.Basic
}

func (s *RebucketSuite) SetupTest() {
	s.ToolSuite.FS = s.FS
	s.ToolSuite.SetupTest()
}

func (s *RebucketSuite) TearDownTest() {
	s.ToolSuite.TearDownTest()
}

func (s *RebucketSuite) TestRebucket() {
	var err error

	siva := NewSiva(nil, s.testFS)
	siva.Bucket(0)
	ctx := context.TODO()

	s.checkBucket(s.testFS, 0, true)

	// dry run should not change anything
	siva.Dry(true)
	err = siva.Rebucket(ctx, inits, 2)
	s.NoError(err)
	s.checkBucket(s.testFS, 0, true)

	siva.Dry(false)

	err = siva.Rebucket(ctx, inits, 2)
	s.NoError(err)
	s.checkBucket(s.testFS, 0, false)
	s.checkBucket(s.testFS, 2, true)

	siva.Bucket(2)
	err = siva.Rebucket(ctx, inits, 4)
	s.NoError(err)
	s.checkBucket(s.testFS, 2, false)
	s.checkBucket(s.testFS, 4, true)

	siva.Bucket(4)
	err = siva.Rebucket(ctx, inits, 0)
	s.NoError(err)
	s.checkBucket(s.testFS, 4, false)
	s.checkBucket(s.testFS, 0, true)
}

func (s *RebucketSuite) checkBucket(fs billy.Basic, bucket int, exist bool) {
	for _, i := range inits {
		name := fmt.Sprintf("%s.siva", bucketPath(i, bucket))
		f, err := s.testFS.Stat(name)
		if exist {
			s.Require().NoError(err)
			s.False(f.IsDir())
		} else {
			s.Error(err)
		}
	}
}
