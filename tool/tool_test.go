package tool

import (
	"fmt"

	"github.com/src-d/borges/storage"
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/core-retrieval.v0/test"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/helper/chroot"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-kallax.v1"
)

type repoData struct {
	uuid   string
	status model.FetchStatus
	refs   []string
}

var inits = []string{
	"03ba13b816e46a3ce07189357e73b067b1710bd3",
	"0bb78b2870cfc50e5619327eca2312b0605a905a",
	"0bf14b8e0e36be9fdd487cf675e1819388f391c0",
	"10016f064fbfc8a717fc26969c78529001608674",
	"11708ab4fc35f5dc9b535d265cee2f52f69b5444",
	"1438588d601dc0f1786df2b80ca97c4bf6582186",
	"1585741ddebf74f3828cd8ded360433716692909",
}

var ulid = []string{
	"0165476b-98f3-df29-4967-c00f2c6c077a",
	"0165476b-991f-7e59-8552-136c47bd532c",
	"0165476b-98bb-aecd-feb3-c7504564af91",
	"0165476b-9890-3a66-5727-771267ed0c94",
	"0165476b-982f-db92-6ae2-6393290d02b5",
	"0165476b-9847-be1f-1561-84c64b83a982",
	"0165476b-9885-f5d8-b812-86a73af4c531",
}

var testRepos = []repoData{
	{ulid[0], model.Pending, []string{inits[0], inits[0]}},
	{ulid[1], model.Fetched, []string{inits[0], inits[1]}},
	{ulid[2], model.Fetching, []string{inits[0], inits[2]}},
	{ulid[3], model.NotFound, []string{inits[0], inits[3]}},
	{ulid[4], model.AuthRequired, []string{inits[0], inits[4]}},
	{ulid[5], model.Pending, []string{inits[0], inits[5], inits[5]}},
	{ulid[6], model.Fetched, []string{inits[0], inits[6], inits[5]}},
}

type ToolSuite struct {
	test.Suite
	FS     billy.Basic
	bucket int
	testFS billy.Basic
	tmp    string
	store  *storage.DatabaseStore
}

func (s *ToolSuite) SetupTest() {
	s.Setup()
	s.store = storage.FromDatabase(s.DB)
	s.createEnv()
}

func (s *ToolSuite) TearDownTest() {
	s.TearDown()
}

func (s *ToolSuite) createEnv() {
	var err error

	if s.FS != nil {
		dir, ok := s.FS.(billy.Dir)
		s.True(ok)
		s.tmp, err = util.TempDir(dir, "", "borges")
		s.NoError(err)

		s.testFS = chroot.New(s.FS, s.tmp)
	}

	for _, r := range testRepos {
		s.createRepo(r)
		for _, ref := range r.refs {
			s.createSiva(ref)
		}
	}
}

func (s *ToolSuite) createSiva(siva string) {
	if s.testFS == nil {
		return
	}

	name := fmt.Sprintf("%s.siva", bucketPath(siva, s.bucket))
	err := util.WriteFile(s.testFS, name, []byte("data"), 0660)
	s.Require().NoError(err)
}

func (s *ToolSuite) createRepo(r repoData) {
	ulid, err := kallax.NewULIDFromText(r.uuid)
	s.Require().NoError(err)

	var refs []*model.Reference
	for _, d := range r.refs {
		sha1 := model.NewSHA1(d)

		ref := model.NewReference()
		ref.Hash = sha1
		ref.Init = sha1

		refs = append(refs, ref)
	}

	repo := model.NewRepository()
	repo.ID = ulid
	repo.Status = r.status
	repo.References = refs

	err = s.store.Create(repo)
	s.Require().NoError(err)
}
