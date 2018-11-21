package tool

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
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

func TestOpenFS(t *testing.T) {
	require := require.New(t)

	tmp, err := ioutil.TempDir("", "borges")
	require.NoError(err)
	defer os.RemoveAll(tmp)

	_, err = OpenFS("invalid:///some/path")
	require.Error(err)

	fs, err := OpenFS(fmt.Sprintf("file://%s", tmp))
	require.NoError(err)

	testFile := filepath.Join(tmp, "test")
	err = ioutil.WriteFile(testFile, []byte("data"), 0660)
	require.NoError(err)

	_, err = fs.Stat("test")
	require.NoError(err)
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

	s.checkBucket(s.testFS, 0, true)

	// dry run should not change anything
	err = Rebucket(s.testFS, inits, 0, 2, true)
	s.NoError(err)
	s.checkBucket(s.testFS, 0, true)

	err = Rebucket(s.testFS, inits, 0, 2, false)
	s.NoError(err)
	s.checkBucket(s.testFS, 0, false)
	s.checkBucket(s.testFS, 2, true)

	err = Rebucket(s.testFS, inits, 2, 4, false)
	s.NoError(err)
	s.checkBucket(s.testFS, 2, false)
	s.checkBucket(s.testFS, 4, true)

	err = Rebucket(s.testFS, inits, 4, 0, false)
	s.NoError(err)
	s.checkBucket(s.testFS, 4, false)
	s.checkBucket(s.testFS, 0, true)
}

func (s *RebucketSuite) checkBucket(fs billy.Basic, bucket int, exist bool) {
	for _, i := range inits {
		name := fmt.Sprintf("%s.siva", bucketPath(i, bucket))
		f, err := s.testFS.Stat(name)
		if exist {
			s.NoError(err)
			s.False(f.IsDir())
		} else {
			s.Error(err)
		}
	}
}
