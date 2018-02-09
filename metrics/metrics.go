package metrics

import (
	"expvar"
	"net/http"
	"sync"
	"time"
)

// Start will start the server at the given export and will expose the
// metric variables.
func Start(addr string) error {
	return http.ListenAndServe(addr, nil)
}

var (
	reposProcessedMu       sync.Mutex
	reposProcessed         = expvar.NewInt("repos_processed")
	reposProcessingAvgTime = expvar.NewFloat("repos_processing_avgtime")

	reposNotFound = expvar.NewInt("repos_not_found")
	reposPrivate  = expvar.NewInt("repos_private")
	reposFailed   = expvar.NewInt("repos_failed")
	reposSkipped  = expvar.NewInt("repos_skipped")

	producedRepos       = expvar.NewInt("repos_produced")
	producedReposFailed = expvar.NewInt("repos_produced_failed")
)

// RepoProcessed increments the counter of processed repositories and updates
// the average time it takes to process a repository.
func RepoProcessed(elapsed time.Duration) {
	reposProcessedMu.Lock()
	defer reposProcessedMu.Unlock()
	reposProcessed.Add(1)
	processed := float64(reposProcessed.Value())
	// (t[n] + t[0..n-1] * (n - 1)) / n
	t := (float64(elapsed) + reposProcessingAvgTime.Value()*(processed-1)) / processed
	reposProcessingAvgTime.Set(t)
}

// RepoNotFound increments the counter of repositories not found.
func RepoNotFound() {
	reposNotFound.Add(1)
}

// RepoPrivate increments the counter of private repositories.
func RepoPrivate() {
	reposPrivate.Add(1)
}

// RepoFailed increments the counter of repositories failed.
func RepoFailed() {
	reposFailed.Add(1)
}

// RepoSkipped increments the counter of skipped repositories for processing.
func RepoSkipped() {
	reposSkipped.Add(1)
}

// RepoProduced increments the counter of produced repositories.
func RepoProduced() {
	producedRepos.Add(1)
}

// RepoProduceFailed increments the counter of failures producing repositories.
func RepoProduceFailed() {
	producedReposFailed.Add(1)
}
