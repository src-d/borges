package borges

import (
	"time"

	"github.com/src-d/go-git-fixtures"
	"github.com/src-d/go-kallax"
	"srcd.works/core.v0/models"
	"srcd.works/go-billy.v1/memfs"
	"srcd.works/go-git.v4"
	"srcd.works/go-git.v4/storage/filesystem"
	"srcd.works/go-git.v4/storage/memory"
)

var (
	timeNow  = time.Now()
	timePast = timeNow.Add(-48 * time.Hour)
)

type FixtureReferences []*models.Reference

func (f FixtureReferences) ByName(name string) *models.Reference {
	for _, ref := range f {
		if ref.Name == name {
			return ref
		}
	}

	return nil
}

func withHash(h models.SHA1, r *models.Reference) *models.Reference {
	return &models.Reference{
		Name:  r.Name,
		Hash:  h,
		Init:  r.Init,
		Roots: r.Roots,
		Timestamps: kallax.Timestamps{
			CreatedAt: r.CreatedAt,
		},
	}
}

func withRoots(r *models.Reference, roots ...models.SHA1) *models.Reference {
	return &models.Reference{
		Name:  r.Name,
		Hash:  r.Hash,
		Init:  roots[0],
		Roots: roots,
		Timestamps: kallax.Timestamps{
			CreatedAt: r.CreatedAt,
		},
	}
}

func withTime(r *models.Reference, firstSeenAt, updatedAt time.Time) *models.Reference {
	return &models.Reference{
		Name:  r.Name,
		Hash:  r.Hash,
		Init:  r.Init,
		Roots: r.Roots,
		Timestamps: kallax.Timestamps{
			CreatedAt: firstSeenAt,
			UpdatedAt: updatedAt,
		},
	}
}

var fixtureReferences FixtureReferences = FixtureReferences{{
	Name: "refs/heads/master",
	Hash: models.NewSHA1("6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
	Init: models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	Roots: []models.SHA1{
		models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/branch",
	Hash: models.NewSHA1("e8d3ffab552895c19b9fcf7aa264d277cde33881"),
	Init: models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	Roots: []models.SHA1{
		models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/1",
	Hash: models.NewSHA1("caf05fe371a5a6feab588a73ebd9ac73abdd072c"),
	Init: models.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"),
	Roots: []models.SHA1{
		models.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"),
		models.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"),
		models.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/2",
	Hash: models.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"),
	Init: models.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"),
	Roots: []models.SHA1{
		models.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/3",
	Hash: models.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"),
	Init: models.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"),
	Roots: []models.SHA1{
		models.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/functionalityOne",
	Hash: models.NewSHA1("ca858bfd043ac70bf532d53a4031be0cdf7483b4"),
	Init: models.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"),
	Roots: []models.SHA1{
		models.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/functionalityTwo",
	Hash: models.NewSHA1("79b3db5091672bcb9da2704a2d7b269bcd1ef36f"),
	Init: models.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"),
	Roots: []models.SHA1{
		models.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"),
		models.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/rootReference",
	Hash: models.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"),
	Init: models.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"),
	Roots: []models.SHA1{
		models.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/tags/v1.0.0",
	Hash: models.NewSHA1("6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
	Init: models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	Roots: []models.SHA1{
		models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}}

type ChangesFixture struct {
	TestName      string
	OldReferences []*models.Reference
	NewRepository func() (*git.Repository, error)
	Expected      Changes
}

func defaultRepository() (*git.Repository, error) {
	srcFs := fixtures.ByTag("root-reference").One().DotGit()
	sto, err := filesystem.NewStorage(srcFs)
	if err != nil {
		return nil, err
	}

	r, err := git.Open(sto, memfs.New())
	if err != nil {
		return nil, err
	}

	return r, nil
}

func emptyRepository() (*git.Repository, error) {
	r, err := git.Init(memory.NewStorage(), nil)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func testAddCommand(r *models.Reference) *Command {
	return &Command{New: withTime(r, timeNow, timeNow)}
}

func testAddRootCommand(r *models.Reference) *Command {
	return &Command{New: withTime(r, timePast, timeNow)}
}

func testDeleteCommand(r *models.Reference) *Command {
	return &Command{Old: r}
}

func testUpdateCommand(old, new *models.Reference) *Command {
	return &Command{
		Old: old,
		New: withTime(new, timePast, timeNow),
	}
}

var ChangesFixtures = []*ChangesFixture{{
	TestName:      "no previous references and no updates",
	NewRepository: emptyRepository,
	OldReferences: nil,
	Expected:      Changes{},
}, {
	TestName:      "one existing reference is removed",
	NewRepository: emptyRepository,
	OldReferences: []*models.Reference{
		fixtureReferences.ByName("refs/heads/master"),
	},
	Expected: Changes{
		models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/master")),
		},
	},
}, {
	TestName:      "all references are new",
	NewRepository: defaultRepository,
	OldReferences: nil,
	Expected: Changes{
		models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/master")),
			testAddCommand(fixtureReferences.ByName("refs/heads/branch")),
			testAddCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		models.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/1")),
		},
		models.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		models.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		models.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		models.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		models.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}, {
	TestName:      "all references are up to date",
	NewRepository: defaultRepository,
	OldReferences: []*models.Reference{
		fixtureReferences.ByName("refs/heads/master"),
		fixtureReferences.ByName("refs/heads/branch"),
		fixtureReferences.ByName("refs/heads/1"),
		fixtureReferences.ByName("refs/heads/2"),
		fixtureReferences.ByName("refs/heads/3"),
		fixtureReferences.ByName("refs/heads/functionalityOne"),
		fixtureReferences.ByName("refs/heads/functionalityTwo"),
		fixtureReferences.ByName("refs/heads/rootReference"),
		fixtureReferences.ByName("refs/tags/v1.0.0"),
	},
	Expected: Changes{},
}, {
	TestName:      "all reference are new except two (up to date)",
	NewRepository: defaultRepository,
	OldReferences: []*models.Reference{
		fixtureReferences.ByName("refs/heads/master"),
		fixtureReferences.ByName("refs/heads/1"),
	},
	Expected: Changes{
		models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/branch")),
			testAddCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		models.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		models.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		models.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		models.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		models.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}, {
	TestName:      "all reference are new except one (updated)",
	NewRepository: defaultRepository,
	OldReferences: []*models.Reference{
		withHash(
			models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			fixtureReferences.ByName("refs/heads/master"),
		),
	},
	Expected: Changes{
		models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testUpdateCommand(withHash(models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				fixtureReferences.ByName("refs/heads/master")),
				fixtureReferences.ByName("refs/heads/master")),
			testAddCommand(fixtureReferences.ByName("refs/heads/branch")),
			testAddCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		models.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/1")),
		},
		models.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		models.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		models.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		models.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		models.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}, {
	TestName:      "all reference are new except one (updated with new init)",
	NewRepository: defaultRepository,
	OldReferences: []*models.Reference{
		withRoots(
			withHash(models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				fixtureReferences.ByName("refs/heads/master")),
			models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
	},
	Expected: Changes{
		models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): []*Command{
			{Old: withRoots(
				withHash(models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
					fixtureReferences.ByName("refs/heads/master")),
				models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))},
		},
		models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testAddRootCommand(fixtureReferences.ByName("refs/heads/master")),
			testAddCommand(fixtureReferences.ByName("refs/heads/branch")),
			testAddCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		models.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/1")),
		},
		models.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		models.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		models.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		models.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		models.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}, {
	TestName:      "all reference are new except one (one root removed)",
	NewRepository: defaultRepository,
	//refRootChange := getByName("refs/heads/master", FixtureReferences).
	//WithRoots(s.aHash, s.bHash)
	OldReferences: []*models.Reference{
		withRoots(
			withHash(models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				fixtureReferences.ByName("refs/heads/master")),
			models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d")),
	},
	Expected: Changes{
		models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): []*Command{
			testDeleteCommand(withRoots(
				withHash(models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
					fixtureReferences.ByName("refs/heads/master")),
				models.NewSHA1("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"))),
		},
		models.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testAddRootCommand(fixtureReferences.ByName("refs/heads/master")),
			testAddCommand(fixtureReferences.ByName("refs/heads/branch")),
			testAddCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		models.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/1")),
		},
		models.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		models.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		models.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		models.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		models.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}}
