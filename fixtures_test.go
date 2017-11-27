package borges

import (
	"time"

	"gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"gopkg.in/src-d/go-kallax.v1"
)

var (
	timeNow  = time.Now()
	timePast = timeNow.Add(-48 * time.Hour)
)

type FixtureReferences []*model.Reference

func (f FixtureReferences) ByName(name string) *model.Reference {
	for _, ref := range f {
		if ref.Name == name {
			return ref
		}
	}

	return nil
}

func withHash(h model.SHA1, r *model.Reference) *model.Reference {
	return &model.Reference{
		Name:  r.Name,
		Hash:  h,
		Init:  r.Init,
		Roots: r.Roots,
		Timestamps: kallax.Timestamps{
			CreatedAt: r.CreatedAt,
		},
	}
}

func withRoots(r *model.Reference, roots ...model.SHA1) *model.Reference {
	return &model.Reference{
		Name:  r.Name,
		Hash:  r.Hash,
		Init:  roots[0],
		Roots: roots,
		Timestamps: kallax.Timestamps{
			CreatedAt: r.CreatedAt,
		},
	}
}

func withTime(r *model.Reference, firstSeenAt, updatedAt time.Time) *model.Reference {
	return &model.Reference{
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

var defaultReferences []*model.Reference = []*model.Reference{
	fixtureReferences.ByName("refs/heads/master"),
	fixtureReferences.ByName("refs/heads/branch"),
	fixtureReferences.ByName("refs/heads/1"),
	fixtureReferences.ByName("refs/heads/2"),
	fixtureReferences.ByName("refs/heads/3"),
	fixtureReferences.ByName("refs/heads/functionalityOne"),
	fixtureReferences.ByName("refs/heads/functionalityTwo"),
	fixtureReferences.ByName("refs/heads/rootReference"),
	fixtureReferences.ByName("refs/tags/v1.0.0"),
}

var branchOneHash = fixtureReferences.ByName("refs/heads/1").Hash

var refOnePointingToRefTwo = &model.Reference{
	Name:       fixtureReferences.ByName("refs/heads/1").Name,
	Timestamps: fixtureReferences.ByName("refs/heads/1").Timestamps,
	Init:       fixtureReferences.ByName("refs/heads/2").Init,
	Hash:       fixtureReferences.ByName("refs/heads/2").Hash,
	Roots:      fixtureReferences.ByName("refs/heads/2").Roots,
}

var fixtureReferences FixtureReferences = FixtureReferences{{
	Name: "refs/heads/master",
	Hash: model.NewSHA1("6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
	Init: model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	Roots: []model.SHA1{
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/branch",
	Hash: model.NewSHA1("e8d3ffab552895c19b9fcf7aa264d277cde33881"),
	Init: model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	Roots: []model.SHA1{
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/1",
	Hash: model.NewSHA1("caf05fe371a5a6feab588a73ebd9ac73abdd072c"),
	Init: model.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"),
	Roots: []model.SHA1{
		model.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"),
		model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"),
		model.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/2",
	Hash: model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"),
	Init: model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"),
	Roots: []model.SHA1{
		model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/3",
	Hash: model.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"),
	Init: model.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"),
	Roots: []model.SHA1{
		model.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/functionalityOne",
	Hash: model.NewSHA1("ca858bfd043ac70bf532d53a4031be0cdf7483b4"),
	Init: model.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"),
	Roots: []model.SHA1{
		model.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/functionalityTwo",
	Hash: model.NewSHA1("79b3db5091672bcb9da2704a2d7b269bcd1ef36f"),
	Init: model.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"),
	Roots: []model.SHA1{
		model.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"),
		model.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/heads/rootReference",
	Hash: model.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"),
	Init: model.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"),
	Roots: []model.SHA1{
		model.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}, {
	Name: "refs/tags/v1.0.0",
	Hash: model.NewSHA1("6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
	Init: model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	Roots: []model.SHA1{
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
	},
	Timestamps: kallax.Timestamps{
		CreatedAt: timePast,
		UpdatedAt: timePast,
	},
}}

type ChangesFixture struct {
	TestName      string
	OldReferences []*model.Reference
	NewReferences []*model.Reference
	Changes       Changes
}

func (f *ChangesFixture) OldRepository() (*git.Repository, error) {
	return f.newRepoFromRefs(f.OldReferences)
}

func (f *ChangesFixture) NewRepository() (*git.Repository, error) {
	return f.newRepoFromRefs(f.NewReferences)
}

func (f *ChangesFixture) newRepoFromRefs(refs []*model.Reference) (*git.Repository, error) {
	if len(refs) == 0 {
		return emptyRepository()
	}

	r, err := defaultRepository()
	if err != nil {
		return nil, err
	}

	return r, f.setReferences(r, refs)
}

func (f *ChangesFixture) setReferences(r *git.Repository, refs []*model.Reference) error {
	if err := f.deleteReferences(r); err != nil {
		return err
	}

	for _, ref := range refs {
		if err := r.Storer.SetReference(ref.GitReference()); err != nil {
			return err
		}
	}

	return nil
}

func (f *ChangesFixture) deleteReferences(r *git.Repository) error {
	iter, err := r.Storer.IterReferences()
	if err != nil {
		return err
	}

	return iter.ForEach(func(ref *plumbing.Reference) error {
		return r.Storer.RemoveReference(ref.Name())
	})
}

func defaultRepository() (*git.Repository, error) {
	srcFs := fixtures.ByTag("root-reference").One().DotGit()
	sto, err := filesystem.NewStorage(srcFs)
	if err != nil {
		return nil, err
	}

	return git.Open(sto, memfs.New())
}

func emptyRepository() (*git.Repository, error) {
	return git.Init(memory.NewStorage(), nil)
}

func testAddCommand(r *model.Reference) *Command {
	return &Command{New: withTime(r, timeNow, timeNow)}
}

func testAddRootCommand(r *model.Reference) *Command {
	return &Command{New: withTime(r, timePast, timeNow)}
}

func testDeleteCommand(r *model.Reference) *Command {
	return &Command{Old: r}
}

func testUpdateCommand(old, new *model.Reference) *Command {
	return &Command{
		Old: old,
		New: withTime(new, timePast, timeNow),
	}
}

const multipleRootsFixture = 9

var ChangesFixtures = []*ChangesFixture{{
	TestName:      "no previous references and no updates",
	OldReferences: nil,
	NewReferences: nil,
	Changes:       Changes{},
}, {
	TestName: "one existing reference is removed (output with no references)",
	OldReferences: []*model.Reference{
		fixtureReferences.ByName("refs/heads/master"),
	},
	NewReferences: nil,
	Changes: Changes{
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/master")),
		},
	},
}, {
	TestName:      "one existing reference is removed (output with references)",
	OldReferences: defaultReferences,
	NewReferences: []*model.Reference{
		fixtureReferences.ByName("refs/heads/branch"),
		fixtureReferences.ByName("refs/heads/1"),
		fixtureReferences.ByName("refs/heads/2"),
		fixtureReferences.ByName("refs/heads/3"),
		fixtureReferences.ByName("refs/heads/functionalityOne"),
		fixtureReferences.ByName("refs/heads/functionalityTwo"),
		fixtureReferences.ByName("refs/heads/rootReference"),
		fixtureReferences.ByName("refs/tags/v1.0.0"),
	},
	Changes: Changes{
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/master")),
		},
	},
}, {
	TestName:      "one reference changes his hash",
	OldReferences: defaultReferences,
	NewReferences: []*model.Reference{
		fixtureReferences.ByName("refs/heads/master"),
		fixtureReferences.ByName("refs/heads/branch"),
		refOnePointingToRefTwo,
		fixtureReferences.ByName("refs/heads/2"),
		fixtureReferences.ByName("refs/heads/3"),
		fixtureReferences.ByName("refs/heads/functionalityOne"),
		fixtureReferences.ByName("refs/heads/functionalityTwo"),
		fixtureReferences.ByName("refs/heads/rootReference"),
		fixtureReferences.ByName("refs/tags/v1.0.0"),
	},
	Changes: Changes{
		model.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/1")),
		},
		model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddRootCommand(refOnePointingToRefTwo),
		},
	},
}, {
	TestName:      "all references are new",
	OldReferences: nil,
	NewReferences: defaultReferences,
	Changes: Changes{
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/master")),
			testAddCommand(fixtureReferences.ByName("refs/heads/branch")),
			testAddCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		model.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/1")),
		},
		model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		model.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		model.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		model.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		model.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}, {
	TestName:      "all references are deleted",
	OldReferences: defaultReferences,
	NewReferences: nil,
	Changes: Changes{
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/master")),
			testDeleteCommand(fixtureReferences.ByName("refs/heads/branch")),
			testDeleteCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		model.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/1")),
		},
		model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		model.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		model.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		model.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		model.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testDeleteCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}, {
	TestName:      "all references are up to date",
	OldReferences: defaultReferences,
	NewReferences: defaultReferences,
	Changes:       Changes{},
}, {
	TestName: "all reference are new except two (up to date)",
	OldReferences: []*model.Reference{
		fixtureReferences.ByName("refs/heads/master"),
		fixtureReferences.ByName("refs/heads/1"),
	},
	NewReferences: defaultReferences,
	Changes: Changes{
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/branch")),
			testAddCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		model.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		model.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		model.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		model.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}, {
	TestName: "all reference are new except one (updated)",
	OldReferences: []*model.Reference{
		withHash(
			branchOneHash,
			fixtureReferences.ByName("refs/heads/master"),
		),
	},
	NewReferences: defaultReferences,
	Changes: Changes{
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testUpdateCommand(withHash(branchOneHash,
				fixtureReferences.ByName("refs/heads/master")),
				fixtureReferences.ByName("refs/heads/master")),
			testAddCommand(fixtureReferences.ByName("refs/heads/branch")),
			testAddCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		model.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/1")),
		},
		model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		model.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		model.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		model.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		model.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}, { // multipleRootsFixture
	TestName: "all reference are new except one (updated with new init)",
	OldReferences: []*model.Reference{
		withRoots(
			withHash(branchOneHash,
				fixtureReferences.ByName("refs/heads/master")),
			branchOneHash),
	},
	NewReferences: defaultReferences,
	Changes: Changes{
		branchOneHash: []*Command{
			{Old: withRoots(
				withHash(branchOneHash,
					fixtureReferences.ByName("refs/heads/master")),
				branchOneHash)},
		},
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testAddRootCommand(fixtureReferences.ByName("refs/heads/master")),
			testAddCommand(fixtureReferences.ByName("refs/heads/branch")),
			testAddCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		model.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/1")),
		},
		model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		model.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		model.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		model.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		model.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}, {
	TestName: "all reference are new except one (one root removed)",
	OldReferences: []*model.Reference{
		withRoots(
			withHash(branchOneHash,
				fixtureReferences.ByName("refs/heads/master")),
			branchOneHash,
			model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d")),
	},
	NewReferences: defaultReferences,
	Changes: Changes{
		branchOneHash: []*Command{
			testDeleteCommand(withRoots(
				withHash(branchOneHash,
					fixtureReferences.ByName("refs/heads/master")),
				branchOneHash,
				model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"))),
		},
		model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d"): []*Command{
			testAddRootCommand(fixtureReferences.ByName("refs/heads/master")),
			testAddCommand(fixtureReferences.ByName("refs/heads/branch")),
			testAddCommand(fixtureReferences.ByName("refs/tags/v1.0.0")),
		},
		model.NewSHA1("8ec19d64748c54c6d047f30c81b4c444a8232b41"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/1")),
		},
		model.NewSHA1("04fffad6eacd4512554cb22ca3a0d6b8a38a96cc"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/2")),
		},
		model.NewSHA1("058cec4b81e8f0a9c3763e0671bbfba0666a4b33"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/3")),
		},
		model.NewSHA1("5e4661353b435315edb0aab7a472bd43c82fed5c"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityOne")),
		},
		model.NewSHA1("8829746417d76e7a64e540e906abcb7970679e47"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/functionalityTwo")),
		},
		model.NewSHA1("a135c3e77219a8eaf166a643f6ce3192e97b7e5e"): []*Command{
			testAddCommand(fixtureReferences.ByName("refs/heads/rootReference")),
		},
	},
}}
