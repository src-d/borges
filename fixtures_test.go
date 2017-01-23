package borges

import (
	"encoding/hex"

	"srcd.works/core.v0/models"
	"srcd.works/go-git.v4"
	"srcd.works/go-git.v4/storage/memory"
)

type RefFixture struct {
	Name   string
	Head   string
	Roots  []string
	Action Action
}

func (rs *RefFixture) Create() *RefFixture {
	return rs.withAction(Create)
}

func (rs *RefFixture) Update() *RefFixture {
	return rs.withAction(Update)
}

func (rs *RefFixture) Delete() *RefFixture {
	return rs.withAction(Delete)
}

func (rs *RefFixture) withAction(a Action) *RefFixture {
	return &RefFixture{
		Head:   rs.Head,
		Name:   rs.Name,
		Action: a,
		Roots:  rs.Roots,
	}
}

func (rs *RefFixture) WithHead(hash string) *RefFixture {
	return &RefFixture{
		Head:   hash,
		Name:   rs.Name,
		Action: rs.Action,
		Roots:  rs.Roots,
	}
}

func (rs *RefFixture) WithRoots(roots ...string) *RefFixture {
	return &RefFixture{
		Head:   rs.Head,
		Name:   rs.Name,
		Action: rs.Action,
		Roots:  roots,
	}
}

func (rs *RefFixture) ToRef() *models.Reference {
	roots := rs.toHash(rs.Roots...)
	return &models.Reference{
		Roots: roots,
		Init:  roots[0],
		Hash:  rs.toHash(rs.Head)[0],
		Name:  rs.Name,
	}
}

func (rs *RefFixture) toHash(hs ...string) []models.SHA1 {
	var result []models.SHA1
	for _, h := range hs {
		b, _ := hex.DecodeString(h)
		var h models.SHA1
		copy(h[:], b)

		result = append(result, h)
	}

	return result
}

func withInitCommit(initCommit models.SHA1, r []*RefFixture) []*RefFixture {
	ic := initCommit.String()

	var result []*RefFixture
	for _, sr := range r {
		if sr.Roots[0] == ic {
			result = append(result, sr)
		}
	}

	return result
}

func getName(c *Command) string {
	var name string
	switch c.Action() {
	case Create, Update:
		name = c.New.Name
	case Delete:
		name = c.Old.Name
	}

	return name
}

func getByName(name string, r []*RefFixture) *RefFixture {
	for _, sr := range r {
		if sr.Name == name {
			return sr
		}
	}

	return nil
}

func getRefByName(name string, r []*models.Reference) *models.Reference {
	for _, sr := range r {
		if sr.Name == name {
			return sr
		}
	}

	return nil
}

type ChangesFixture struct {
	Repository      *git.Repository
	InitCommitCount int
	OldReferences   []*models.Reference
	Expected        []*RefFixture
}

var RefFixtures []*RefFixture = []*RefFixture{{
	Name: "refs/heads/master",
	Head: "6ecf0ef2c2dffb796033e5a02219af86ec6584e5",
	Roots: []string{
		"b029517f6300c2da0f4b651b8642506cd6aaf45d",
	},
}, {
	Name: "refs/heads/branch",
	Head: "e8d3ffab552895c19b9fcf7aa264d277cde33881",
	Roots: []string{
		"b029517f6300c2da0f4b651b8642506cd6aaf45d",
	},
}, {
	Name: "refs/heads/1",
	Head: "caf05fe371a5a6feab588a73ebd9ac73abdd072c",
	Roots: []string{
		"8ec19d64748c54c6d047f30c81b4c444a8232b41",
		"04fffad6eacd4512554cb22ca3a0d6b8a38a96cc",
		"058cec4b81e8f0a9c3763e0671bbfba0666a4b33",
	},
}, {
	Name: "refs/heads/2",
	Head: "04fffad6eacd4512554cb22ca3a0d6b8a38a96cc",
	Roots: []string{
		"04fffad6eacd4512554cb22ca3a0d6b8a38a96cc",
	},
}, {
	Name: "refs/heads/3",
	Head: "058cec4b81e8f0a9c3763e0671bbfba0666a4b33",
	Roots: []string{
		"058cec4b81e8f0a9c3763e0671bbfba0666a4b33",
	},
}, {
	Name: "refs/heads/functionalityOne",
	Head: "ca858bfd043ac70bf532d53a4031be0cdf7483b4",
	Roots: []string{
		"5e4661353b435315edb0aab7a472bd43c82fed5c",
	},
}, {
	Name: "refs/heads/functionalityTwo",
	Head: "79b3db5091672bcb9da2704a2d7b269bcd1ef36f",
	Roots: []string{
		"8829746417d76e7a64e540e906abcb7970679e47",
		"5e4661353b435315edb0aab7a472bd43c82fed5c",
	},
}, {
	Name: "refs/heads/rootReference",
	Head: "a135c3e77219a8eaf166a643f6ce3192e97b7e5e",
	Roots: []string{
		"a135c3e77219a8eaf166a643f6ce3192e97b7e5e",
	},
}, {
	Name: "refs/tags/v1.0.0",
	Head: "6ecf0ef2c2dffb796033e5a02219af86ec6584e5",
	Roots: []string{
		"b029517f6300c2da0f4b651b8642506cd6aaf45d",
	},
}}

func newEmptyRepository() *git.Repository {
	r, err := git.Init(memory.NewStorage(), nil)
	if err != nil {
		panic(err)
	}

	return r
}
