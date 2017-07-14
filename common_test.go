package lock

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type LockSuite struct {
	suite.Suite
	ConnectionString string
}

func (s *LockSuite) NewService() Service {
	assert := s.Assert()
	srv, err := New(s.ConnectionString)
	assert.NoError(err)
	return srv
}

func (s *LockSuite) TestLockUnlockSingleLockNoConcurrency() {
	assert := s.Assert()
	niter := 100
	id := "mylock"
	service := s.NewService()
	cfg := &SessionConfig{
		Timeout: 1 * time.Second,
		TTL:     1 * time.Second,
	}
	session, err := service.NewSession(cfg)
	assert.NoError(err)
	for i := 0; i < niter; i++ {
		locker := session.NewLocker(id)
		s.testLockUnlock(locker)
	}

	err = service.Close()
	assert.NoError(err)
}

func (s *LockSuite) TestLockTimeout() {
	assert := s.Assert()

	service := s.NewService()
	cfg := &SessionConfig{
		Timeout: time.Millisecond * 500,
	}
	id := "mylock-" + s.T().Name()

	session1, err := service.NewSession(cfg)
	assert.NoError(err)
	locker1 := session1.NewLocker(id)

	session2, err := service.NewSession(cfg)
	assert.NoError(err)
	locker2 := session2.NewLocker(id)

	_, err = locker1.Lock()
	assert.NoError(err)
	_, err = locker2.Lock()
	assert.Error(err)
	assert.True(ErrCanceled.Is(err))

	err = service.Close()
	assert.NoError(err)
}

func (s *LockSuite) TestDoubleClose() {
	assert := s.Assert()
	service := s.NewService()
	err := service.Close()
	assert.NoError(err)
	err = service.Close()
	assert.Error(err)
	assert.True(ErrAlreadyClosed.Is(err))

	service = s.NewService()
	cfg := &SessionConfig{}
	session, err := service.NewSession(cfg)
	assert.NoError(err)
	err = session.Close()
	assert.NoError(err)
	err = session.Close()
	assert.Error(err)
	assert.True(ErrAlreadyClosed.Is(err))
	err = service.Close()
	assert.NoError(err)
	err = service.Close()
	assert.Error(err)
	assert.True(ErrAlreadyClosed.Is(err))
}

func (s *LockSuite) TestLockUnlockMultipleLocksNoConcurrency() {
	assert := s.Assert()
	niter := 100
	id := "mylock"
	service := s.NewService()
	cfg := &SessionConfig{TTL: 1 * time.Second}
	session, err := service.NewSession(cfg)
	assert.NoError(err)
	for i := 0; i < niter; i++ {
		locker := session.NewLocker(id + strconv.Itoa(i))
		s.testLockUnlock(locker)
	}

	err = session.Close()
	assert.NoError(err)
	err = service.Close()
	assert.NoError(err)
}

func (s *LockSuite) TestLockUnlockSingleLockConcurrentNoTimeout() {
	assert := s.Assert()
	niter := 100
	id := "mylock"
	service := s.NewService()
	cfg := &SessionConfig{}
	counter := 0
	wg := &sync.WaitGroup{}
	wg.Add(niter)
	for i := 0; i < niter; i++ {
		go func() {
			f := func() {
				counter++
			}
			session, err := service.NewSession(cfg)
			assert.NoError(err)
			locker := session.NewLocker(id)
			s.testLockUnlockConcurrent(locker, f, false)
			err = session.Close()
			assert.NoError(err)
			wg.Done()
		}()
	}

	wg.Wait()
	assert.Equal(niter, counter)
	err := service.Close()
	assert.NoError(err)
}

func (s *LockSuite) TestLockUnlockSingleLockConcurrentTimeout() {
	assert := s.Assert()
	niter := 100
	id := "mylock"
	service := s.NewService()
	cfg := &SessionConfig{TTL: time.Second * 2}
	counter := 0
	wg := &sync.WaitGroup{}
	wg.Add(niter)
	for i := 0; i < niter; i++ {
		go func() {
			f := func() {
				counter++
			}
			session, err := service.NewSession(cfg)
			assert.NoError(err)
			locker := session.NewLocker(id)
			s.testLockUnlockConcurrent(locker, f, true)
			err = session.Close()
			assert.NoError(err)
			wg.Done()
		}()
	}

	wg.Wait()
	assert.Equal(niter, counter)
	err := service.Close()
	assert.NoError(err)
}

func (s *LockSuite) TestLockUnlockMultipleLocksConcurrentNoTimeout() {
	assert := s.Assert()
	niter := 1000
	id := "mylock"
	service := s.NewService()
	cfg := &SessionConfig{}
	wg := &sync.WaitGroup{}
	wg.Add(niter)
	for i := 0; i < niter; i++ {
		go func(i int) {
			f := func() {}
			session, err := service.NewSession(cfg)
			assert.NoError(err)
			locker := session.NewLocker(id + strconv.Itoa(i))
			s.testLockUnlockConcurrent(locker, f, false)
			err = session.Close()
			assert.NoError(err)
			wg.Done()
		}(i)
	}

	wg.Wait()
	err := service.Close()
	assert.NoError(err)
}

func (s *LockSuite) TestLockUnlockMultipleLocksConcurrentNoTimeoutSingleSession() {
	assert := s.Assert()
	niter := 1000
	id := "mylock"
	service := s.NewService()
	cfg := &SessionConfig{}
	session, err := service.NewSession(cfg)
	assert.NoError(err)
	wg := &sync.WaitGroup{}
	wg.Add(niter)
	for i := 0; i < niter; i++ {
		go func(i int) {
			f := func() {}
			locker := session.NewLocker(id + strconv.Itoa(i))
			s.testLockUnlockConcurrent(locker, f, false)
			wg.Done()
		}(i)
	}

	wg.Wait()
	err = session.Close()
	assert.NoError(err)
	err = service.Close()
	assert.NoError(err)
}

func (s *LockSuite) TestLockUnlockMultipleLocksConcurrentTimeout() {
	assert := s.Assert()
	niter := 1000
	id := "mylock"
	service := s.NewService()
	cfg := &SessionConfig{TTL: time.Second * 2}
	wg := &sync.WaitGroup{}
	wg.Add(niter)
	for i := 0; i < niter; i++ {
		go func(i int) {
			f := func() {}
			session, err := service.NewSession(cfg)
			assert.NoError(err)
			locker := session.NewLocker(id + strconv.Itoa(i))
			s.testLockUnlockConcurrent(locker, f, true)
			err = session.Close()
			assert.NoError(err)
			wg.Done()
		}(i)
	}

	wg.Wait()
	err := service.Close()
	assert.NoError(err)
}

func (s *LockSuite) testLockUnlockConcurrent(locker Locker, f func(), hasTimeout bool) {
	assert := s.Assert()
	t := s.T()
	lostLocks := 0
	for {
		cancel, err := locker.Lock()
		if hasTimeout {
			if err != nil {
				if ErrCanceled.Is(err) {
					continue
				}

				assert.True(ErrCanceled.Is(err))
				break
			}
		} else {
			assert.NoError(err)
			if err != nil {
				break
			}
		}

		done := false
		select {
		case <-cancel:
			assert.Fail("lost lock")
			lostLocks++
		default:
			f()
			done = true
		}
		err = locker.Unlock()
		assert.NoError(err)
		if done {
			if lostLocks > 0 {
				t.Logf("lock lost %d times", lostLocks)
			}
			break
		}

		if lostLocks >= 3 {
			assert.Fail("too many lost locks")
			break
		}
	}
}

func (s *LockSuite) testLockUnlock(locker Locker) {
	assert := s.Assert()
	_, err := locker.Lock()
	assert.NoError(err)
	err = locker.Unlock()
	assert.NoError(err)
}

func Example() {
	service, err := New("local:")
	if err != nil {
		panic(err)
	}

	cfg := &SessionConfig{TTL: time.Second * 10}

	id := "mylock"
	counter := 0
	niter := 1000
	wg := &sync.WaitGroup{}
	wg.Add(niter)
	for i := 0; i < niter; i++ {
		go func() {
			session, err := service.NewSession(cfg)
			if err != nil {
				panic(err)
			}

			lock := session.NewLocker(id)
			cancel, err := lock.Lock()
			if err != nil {
				panic(err)
			}

			select {
			case <-cancel:
				panic("lost lock")
			default:
				counter++
			}

			err = lock.Unlock()
			if err != nil {
				panic(err)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	fmt.Println(counter)
	//Output: 1000
}

func TestNewUnsupportedService(t *testing.T) {
	require := require.New(t)
	srv, err := New("unsupported:")
	require.Error(err)
	require.True(ErrUnsupportedService.Is(err))
	require.Nil(srv)
}

func TestNewInvalidConnectionString(t *testing.T) {
	require := require.New(t)
	srv, err := New(":")
	require.Error(err)
	require.True(ErrInvalidConnectionString.Is(err))
	require.Nil(srv)
}
