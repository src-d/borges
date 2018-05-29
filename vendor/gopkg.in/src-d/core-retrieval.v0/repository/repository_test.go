package repository

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
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
		{
			"mem to mem",
			NewCopier(memfs.New(), NewLocalFs(memfs.New()), 0),
		},
		{
			"mem to os",
			NewCopier(s.newFilesystem(), NewLocalFs(memfs.New()), 0),
		},
		{
			"os to mem",
			NewCopier(s.newFilesystem(), NewLocalFs(memfs.New()), 0),
		},
		{
			"os to os",
			NewCopier(s.newFilesystem(), NewLocalFs(s.newFilesystem()), 0),
		},
		{
			"os to HDFS",
			NewCopier(s.newFilesystem(), NewHDFSFs(hdfsURL, s.newTempPath(), s.newTempPath()), 0),
		},
		{
			"mem to HDFS",
			NewCopier(memfs.New(), NewHDFSFs(hdfsURL, s.newTempPath(), s.newTempPath()), 0),
		},

		{
			"mem to mem with bucketing",
			NewCopier(memfs.New(), NewLocalFs(memfs.New()), 2),
		},
		{
			"mem to os with bucketing",
			NewCopier(s.newFilesystem(), NewLocalFs(memfs.New()), 2),
		},
		{
			"os to mem with bucketing",
			NewCopier(s.newFilesystem(), NewLocalFs(memfs.New()), 2),
		},
		{
			"os to os with bucketing",
			NewCopier(s.newFilesystem(), NewLocalFs(s.newFilesystem()), 2),
		},
		{
			"os to HDFS with bucketing",
			NewCopier(s.newFilesystem(), NewHDFSFs(hdfsURL, s.newTempPath(), s.newTempPath()), 2),
		},
		{
			"mem to HDFS with bucketing",
			NewCopier(memfs.New(), NewHDFSFs(hdfsURL, s.newTempPath(), s.newTempPath()), 2),
		},
	}

	for _, fsPair := range fsPairs {
		s.T().Run(fsPair.Name, func(t *testing.T) {
			testRootedTransactioner(t, NewSivaRootedTransactioner(fsPair.Copier))
		})

		s.T().Run(fmt.Sprintf("%s with real repository", fsPair.Name), func(t *testing.T) {
			testWithRealRepository(t, NewSivaRootedTransactioner(fsPair.Copier))
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

	tx, err := s.Begin(context.TODO(), f.Head)
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

	err = tx.Commit(context.TODO())
	require.NoError(err)

	tx2, err := s.Begin(context.TODO(), f.Head)
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
	tx1, err := s.Begin(context.TODO(), h1)
	require.NoError(err)
	require.NotNil(tx1)
	r1, err := git.Open(tx1.Storer(), nil)
	require.NoError(err)

	// tx1 -> create ref1
	refName1 := plumbing.ReferenceName("ref1")
	err = r1.Storer.SetReference(plumbing.NewSymbolicReference(refName1, refName1))
	require.NoError(err)

	// begin tx2
	tx2, err := s.Begin(context.TODO(), h1)
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
	err = tx1.Commit(context.TODO())
	require.NoError(err)

	// ref1 not visible in tx2 (even with tx1 committed)
	_, err = r2.Reference(refName1, false)
	require.Equal(plumbing.ErrReferenceNotFound, err)

	// rollback tx2
	err = tx2.Rollback()
	require.NoError(err)

	// begin tx3
	tx3, err := s.Begin(context.TODO(), h1)
	require.NoError(err)
	require.NotNil(tx3)
	r3, err := git.Open(tx3.Storer(), nil)
	require.NoError(err)

	// ref1 visible in tx3
	_, err = r3.Reference(refName1, false)
	require.NoError(err)
	require.NoError(tx3.Rollback())

	// begin tx4
	tx4, err := s.Begin(context.TODO(), h1)
	require.NoError(err)
	require.NotNil(tx4)
	r4, err := git.Open(tx4.Storer(), nil)
	require.NoError(err)

	// tx4 -> create ref4
	refName4 := plumbing.ReferenceName("ref4")
	err = r4.Storer.SetReference(plumbing.NewSymbolicReference(refName4, refName4))
	require.NoError(err)

	// commit tx4
	err = tx4.Commit(context.TODO())
	require.NoError(err)
}

type fsPair struct {
	Name   string
	Copier *Copier
}

func (s *FilesystemSuite) TestRepositoryTmpDeletion() {
	require := require.New(s.T())

	sivaFiles := s.newFilesystem()
	require.NotNil(sivaFiles)

	sivaFiles = &BrokenFS{
		Filesystem: sivaFiles,
	}

	tmpFiles := s.newFilesystem()
	require.NotNil(tmpFiles)

	tmpFiles = &BrokenFS{
		Filesystem: tmpFiles,
	}

	copier := NewCopier(tmpFiles, NewLocalFs(sivaFiles), 0)
	require.NotNil(copier)

	repo := NewSivaRootedTransactioner(copier)
	require.NotNil(repo)

	permissionsHex := "0000000000000000000000000000000000000001"
	brokenHex := "0000000000000000000000000000000000000002"

	require.NoError(createSiva(sivaFiles, permissionsHex, 0))
	testTmpDeletion(require, tmpFiles, repo, permissionsHex)

	require.NoError(createSiva(sivaFiles, brokenHex, 0444))
	testTmpDeletion(require, tmpFiles, repo, brokenHex)
}

func testTmpDeletion(
	require *require.Assertions,
	tmpFiles billy.Filesystem,
	repo RootedTransactioner,
	hash string,
) {
	tx, err := repo.Begin(context.TODO(), plumbing.NewHash(hash))
	require.Error(err)
	require.Nil(tx)

	files, err := tmpFiles.ReadDir("/")
	require.NoError(err)
	require.Len(files, 0)
}

func createSiva(tmpFS billy.Filesystem, name string, mode os.FileMode) error {
	fileName := fmt.Sprintf("%s.siva", name)
	file, err := tmpFS.Create(fileName)
	if err != nil {
		return err
	}

	_, err = file.Write([]byte("invalid siva contents"))
	file.Close()

	if err != nil {
		return err
	}

	// tmp dir does not have Change interface
	fullPath := filepath.Join(tmpFS.Root(), fileName)
	return os.Chmod(fullPath, mode)
}

func NewBrokenFS(fs billy.Filesystem) billy.Filesystem {
	return &BrokenFS{
		Filesystem: fs,
	}
}

type BrokenFS struct {
	billy.Filesystem
}

func (fs *BrokenFS) OpenFile(
	name string,
	flag int,
	perm os.FileMode,
) (billy.File, error) {
	file, err := fs.Filesystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return &BrokenFile{
		File: file,
	}, nil
}

type BrokenFile struct {
	billy.File
}

func (fs *BrokenFile) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("could not read from broken file")
}

func (fs *BrokenFile) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("could not write to broken file")
}
