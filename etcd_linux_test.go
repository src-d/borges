package lock

import (
	"syscall"
	"time"
)

// +build linux

func (s *EtcdLockSuite) TestLockExpire() {
	assert := s.Assert()

	id := "mylock"
	service := s.NewService()
	session, err := service.NewSession(&SessionConfig{
		TTL: 2 * time.Second,
	})
	assert.NoError(err)

	locker := session.NewLocker(id)
	ch, err := locker.Lock()
	assert.NoError(err)

	err = s.cmd.Process.Signal(syscall.SIGSTOP)
	assert.NoError(err)
	<-ch
	err = s.cmd.Process.Signal(syscall.SIGCONT)
	assert.NoError(err)

	err = service.Close()
	assert.NoError(err)
}

func (s *EtcdLockSuite) TestSessionError() {
	assert := s.Assert()

	service := s.NewService()

	err := s.cmd.Process.Signal(syscall.SIGSTOP)
	assert.NoError(err)

	session, err := service.NewSession(&SessionConfig{
		TTL: 2 * time.Second,
	})
	assert.Error(err)
	assert.Nil(session)

	err = s.cmd.Process.Signal(syscall.SIGCONT)
	assert.NoError(err)

	err = service.Close()
	assert.NoError(err)
}
