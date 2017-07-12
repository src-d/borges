package lock

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type LocalLockSuite struct {
	LockSuite
	Endpoints []string
}

func TestLocalLock(t *testing.T) {
	suite.Run(t, new(LocalLockSuite))
}

func (s *LocalLockSuite) SetupTest() {
	s.ConnectionString = "local:"
}

func TestInternalLocalLock(t *testing.T) {
	require := require.New(t)

	lock := newLocalLock()
	for i := 0; i < 1000; i++ {
		ok := lock.Lock(0)
		require.True(ok)
		lock.Unlock()
	}

	wg := &sync.WaitGroup{}
	niter := 10000
	wg.Add(niter)
	counter := 0
	for i := 0; i < niter; i++ {
		go func() {
			ok := lock.Lock(0)
			require.True(ok)
			counter++
			lock.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()
	require.Equal(niter, counter)

	niter = 10000
	timeout := time.Millisecond * 1
	wg.Add(niter)
	counter = 0
	for i := 0; i < niter; i++ {
		go func() {
			for {
				ok := lock.Lock(timeout)
				if !ok {
					continue
				}

				counter++
				lock.Unlock()
				break
			}

			wg.Done()
		}()
	}

	wg.Wait()
	require.Equal(niter, counter)

	niter = 100
	timeout = time.Millisecond * 5
	wg.Add(niter)
	counter = 0
	for i := 0; i < niter; i++ {
		go func() {
			for {
				ok := lock.Lock(timeout)
				if !ok {
					continue
				}

				counter++
				time.Sleep(time.Millisecond * 10)
				lock.Unlock()
				break
			}

			wg.Done()
		}()
	}

	wg.Wait()
	require.Equal(niter, counter)
}
