package test

import (
	"testing"

	"gopkg.in/src-d/core-retrieval.v0/model"

	"github.com/stretchr/testify/suite"
)

func TestSuite(t *testing.T) {
	suite.Run(t, new(SuiteSuite))
}

type SuiteSuite struct {
	Suite

	store          *model.RepositoryStore
	referenceStore *model.ReferenceStore
}

func (s *SuiteSuite) SetupTest() {
	s.Setup()

	s.store = model.NewRepositoryStore(s.DB)
	s.referenceStore = model.NewReferenceStore(s.DB)
}

func (s *SuiteSuite) TearDownTest() {
	s.TearDown()
}

func (s *SuiteSuite) TestSchemaChanges() {
	r1 := model.NewReference()
	r1.Init = model.NewSHA1("dede")
	r1.Roots = model.SHA1List{
		model.NewSHA1("dada"),
		model.NewSHA1("adad"),
	}

	r2 := model.NewReference()
	r2.Init = model.NewSHA1("eded")
	r2.Roots = model.SHA1List{
		model.NewSHA1("abab"),
		model.NewSHA1("baba"),
	}

	repo := model.NewRepository()
	repo.References = []*model.Reference{r1, r2}
	err := s.store.Insert(repo)
	s.NoError(err)

	repo, err = s.store.FindOne(model.NewRepositoryQuery().WithReferences(nil))
	s.NoError(err)
	s.Require().NotNil(repo)
	s.Len(repo.References, 2)

	ref, err := s.referenceStore.FindOne(model.NewReferenceQuery())
	s.NoError(err)
	s.Require().NotNil(ref)
	s.Len(ref.Roots, 2)
}
