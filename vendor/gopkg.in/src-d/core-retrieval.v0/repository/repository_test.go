package repository

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/colinmarc/hdfs"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

const (
	tmpPrefix = "core-retrieval-test"
	hdfsURL   = "localhost:9000"
)

var (
	h1 = plumbing.NewHash("0000000000000000000000000000000000000001")
)

func TestFilesystemSuite(t *testing.T) {
	suite.Run(t, &FilesystemSuite{})
}

type FilesystemSuite struct {
	suite.Suite
	tmpDirs map[string]bool
}

func (s *FilesystemSuite) SetupTest() {
	s.NoError(fixtures.Init())
	s.tmpDirs = make(map[string]bool)
}

func (s *FilesystemSuite) TearDownTest() {
	hc, err := hdfs.New(hdfsURL)
	s.NoError(err)
	err = hc.Remove("/")
	s.NoError(err)

	s.cleanUpTempDirectories()
	s.NoError(fixtures.Clean())
}

func (s *FilesystemSuite) cleanUpTempDirectories() {
	require := require.New(s.T())
	var err error
	for dir := range s.tmpDirs {
		if e := os.RemoveAll(dir); e != nil && err != nil {
			err = e
		}

		delete(s.tmpDirs, dir)
	}

	require.NoError(err)
}

func (s *FilesystemSuite) Test() {
	fsPairs := []*fsPair{
		{"mem to mem", NewLocalCopier(memfs.New(), 0), memfs.New()},
		{"mem to os", NewLocalCopier(memfs.New(), 0), s.newFilesystem()},
		{"os to mem", NewLocalCopier(s.newFilesystem(), 0), memfs.New()},
		{"os to os", NewLocalCopier(s.newFilesystem(), 0), s.newFilesystem()},
		{"os to HDFS", NewHDFSCopier(hdfsURL, s.newTempPath(), s.newTempPath(), 0), s.newFilesystem()},
		{"mem to HDFS", NewHDFSCopier(hdfsURL, s.newTempPath(), s.newTempPath(), 0), memfs.New()},

		{"mem to mem", NewLocalCopier(memfs.New(), 2), memfs.New()},
		{"mem to os", NewLocalCopier(memfs.New(), 2), s.newFilesystem()},
		{"os to mem", NewLocalCopier(s.newFilesystem(), 2), memfs.New()},
		{"os to os", NewLocalCopier(s.newFilesystem(), 2), s.newFilesystem()},
		{"os to HDFS", NewHDFSCopier(hdfsURL, s.newTempPath(), s.newTempPath(), 2), s.newFilesystem()},
		{"mem to HDFS", NewHDFSCopier(hdfsURL, s.newTempPath(), s.newTempPath(), 2), memfs.New()},
	}

	for _, fsPair := range fsPairs {
		s.T().Run(fsPair.Name, func(t *testing.T) {
			testRootedTransactioner(t, NewSivaRootedTransactioner(fsPair.Copier, fsPair.Local))
		})

		s.T().Run(fmt.Sprintf("%s with real repository", fsPair.Name), func(t *testing.T) {
			testWithRealRepository(t, NewSivaRootedTransactioner(fsPair.Copier, fsPair.Local))
		})
	}
}

func (s *FilesystemSuite) newFilesystem() billy.Filesystem {
	tmpDir := s.newTempPath()
	s.tmpDirs[tmpDir] = true
	return osfs.New(tmpDir)
}

func (s *FilesystemSuite) newTempPath() string {
	tmpDir, err := ioutil.TempDir(os.TempDir(), tmpPrefix)
	s.NoError(err)
	return tmpDir
}

func testWithRealRepository(t *testing.T, s RootedTransactioner) {
	require := require.New(t)

	f := fixtures.Basic().ByTag("worktree").One()

	rTest, err := git.PlainOpen(f.Worktree().Root())
	require.NoError(err)

	oIterTest, err := rTest.CommitObjects()
	require.NoError(err)

	countTest := 0
	err = oIterTest.ForEach(func(o *object.Commit) error {
		countTest++
		return nil
	})
	require.NoError(err)
	require.NotEqual(0, countTest)

	tx, err := s.Begin(f.Head)
	require.NoError(err)

	r, err := git.Open(tx.Storer(), nil)
	require.NoError(err)
	_, err = r.CreateRemote(&config.RemoteConfig{
		URLs: []string{f.Worktree().Root()},
		Name: git.DefaultRemoteName,
	})
	require.NoError(err)

	err = r.Fetch(&git.FetchOptions{
		RefSpecs: []config.RefSpec{config.RefSpec("+refs/heads/*:refs/heads/*")},
	})
	require.NoError(err)

	cIter, err := r.CommitObjects()
	require.NoError(err)

	count := 0
	err = cIter.ForEach(func(o *object.Commit) error {
		count++
		return nil
	})
	require.NoError(err)
	require.NotEqual(0, count)

	err = tx.Commit()
	require.NoError(err)

	tx2, err := s.Begin(f.Head)
	require.NoError(err)

	r, err = git.Open(tx2.Storer(), nil)
	require.NoError(err)

	cIter, err = r.CommitObjects()
	require.NoError(err)

	count2 := 0
	err = cIter.ForEach(func(o *object.Commit) error {
		count2++
		return nil
	})
	require.NoError(err)
	require.NotEqual(0, count)

	require.Equal(count, count2)
	err = tx2.Rollback()
	require.NoError(err)
}

func testRootedTransactioner(t *testing.T, s RootedTransactioner) {
	require := require.New(t)

	// begin tx1
	tx1, err := s.Begin(h1)
	require.NoError(err)
	require.NotNil(tx1)
	r1, err := git.Open(tx1.Storer(), nil)
	require.NoError(err)

	// tx1 -> create ref1
	refName1 := plumbing.ReferenceName("ref1")
	err = r1.Storer.SetReference(plumbing.NewSymbolicReference(refName1, refName1))
	require.NoError(err)

	// begin tx2
	tx2, err := s.Begin(h1)
	require.NoError(err)
	require.NotNil(tx2)
	r2, err := git.Open(tx2.Storer(), nil)
	require.NoError(err)

	// ref1 not visible in tx2
	_, err = r2.Reference(refName1, false)
	require.Equal(plumbing.ErrReferenceNotFound, err)

	// tx2 -> create ref2
	refName2 := plumbing.ReferenceName("ref2")
	err = r2.Storer.SetReference(plumbing.NewSymbolicReference(refName2, refName2))
	require.NoError(err)

	// ref2 not visible in tx2
	_, err = r1.Reference(refName2, false)
	require.Equal(plumbing.ErrReferenceNotFound, err)

	// commit tx1
	err = tx1.Commit()
	require.NoError(err)

	// ref1 not visible in tx2 (even with tx1 committed)
	_, err = r2.Reference(refName1, false)
	require.Equal(plumbing.ErrReferenceNotFound, err)

	// rollback tx2
	err = tx2.Rollback()
	require.NoError(err)

	// begin tx3
	tx3, err := s.Begin(h1)
	require.NoError(err)
	require.NotNil(tx3)
	r3, err := git.Open(tx3.Storer(), nil)
	require.NoError(err)

	// ref1 visible in tx3
	_, err = r3.Reference(refName1, false)
	require.NoError(err)
	require.NoError(tx3.Rollback())

	// begin tx4
	tx4, err := s.Begin(h1)
	require.NoError(err)
	require.NotNil(tx4)
	r4, err := git.Open(tx4.Storer(), nil)
	require.NoError(err)

	// tx4 -> create ref4
	refName4 := plumbing.ReferenceName("ref4")
	err = r4.Storer.SetReference(plumbing.NewSymbolicReference(refName4, refName4))
	require.NoError(err)

	// commit tx4
	err = tx4.Commit()
	require.NoError(err)
}

type fsPair struct {
	Name   string
	Copier Copier
	Local  billy.Filesystem
}
