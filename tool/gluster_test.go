// +build gluster

package tool

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-billy-gluster.v0"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/helper/chroot"
	"gopkg.in/src-d/go-billy.v4/util"
)

func connectGluster() (string, billy.Basic, error) {
	fs, err := gluster.New("localhost", "billy")
	if err != nil {
		return "", nil, err
	}

	tmp, err := util.TempDir(fs, "", "billy")
	if err != nil {
		return "", nil, err
	}

	tmpFS := chroot.New(fs, tmp)
	return tmp, tmpFS, nil
}

func TestSivaGluster(t *testing.T) {
	_, fs, err := connectGluster()
	require.NoError(t, err)

	suite.Run(t, &SivaSuite{FS: fs, bucket: 0})
	suite.Run(t, &SivaSuite{FS: fs, bucket: 2})
}

func TestRebucketGluster(t *testing.T) {
	_, fs, err := connectGluster()
	require.NoError(t, err)

	suite.Run(t, &RebucketSuite{FS: fs})
}

func TestOpenFSGluster(t *testing.T) {
	_, err := OpenFS("gluster://localhost")
	require.Error(t, err)

	tmp, fs, err := connectGluster()
	require.NoError(t, err)

	url := fmt.Sprintf("gluster://localhost/billy/%s", tmp)
	gfs, err := OpenFS(url)
	require.NoError(t, err)

	text := []byte("data")
	file := "borges-test"
	err = util.WriteFile(fs, file, text, 0666)
	require.NoError(t, err)

	f, err := gfs.Open(file)
	require.NoError(t, err)

	b := make([]byte, 1024)
	n, err := f.Read(b)
	require.Equal(t, io.EOF, err)
	require.Equal(t, len(text), n)

	err = f.Close()
	require.NoError(t, err)

	require.Equal(t, text, b[:len(text)])
}
