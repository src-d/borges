// +build gluster

package tool

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-billy-gluster.v0"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/helper/chroot"
	"gopkg.in/src-d/go-billy.v4/util"
)

func connectGluster() (billy.Basic, error) {
	fs, err := gluster.New("localhost", "billy")
	if err != nil {
		return nil, err
	}

	tmp, err := util.TempDir(fs, "", "billy")
	if err != nil {
		return nil, err
	}

	tmpFS := chroot.New(fs, tmp)
	return tmpFS, nil
}

func TestSivaGluster(t *testing.T) {
	fs, err := connectGluster()
	require.NoError(t, err)

	suite.Run(t, &SivaSuite{FS: fs, bucket: 0})
	suite.Run(t, &SivaSuite{FS: fs, bucket: 2})
}

func TestRebucketGluster(t *testing.T) {
	fs, err := connectGluster()
	require.NoError(t, err)

	suite.Run(t, &RebucketSuite{FS: fs})
}
