// borges archives repositories in a universal git library.
//
// The goal of borges is fetching repositories and maintain them updated.
// Repositories are arranged in a repository storage where that contains one
// repository per init commit found.
//
// We define root commit as any commit with no parents (the first commit of a
// repository). Note that a repository can contain multiple root commits.
//
// For each reference, we define its init commit as the root commit that is
// reached by following the first parent of each commit in the history. This
// is the commit that would be obtained with:
//
//   $ git rev-list --first-parent <ref> | tail -n 1
//
// When borges fetches a repository, it groups all references by init commit
// and pushes each group of references to a repository for its init commit.
package borges
