package lock

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-errors.v0"
)

type EtcdLockSuite struct {
	LockSuite
	Endpoints []string
	etcdPath  string
	dataDir   string
	cmd       *exec.Cmd
}

func TestEtcdLock(t *testing.T) {
	suite.Run(t, new(EtcdLockSuite))
}

func (s *EtcdLockSuite) SetupSuite() {
	t := s.T()
	etcdPath, err := exec.LookPath("etcd")
	if err != nil {
		t.Skip("etcd not available in PATH")
	}
	s.etcdPath = etcdPath
}

func (s *EtcdLockSuite) SetupTest() {
	require := s.Require()

	dataDir, err := ioutil.TempDir("", "etcd-data_")
	require.NoError(err)
	s.dataDir = dataDir

	endpoint := "http://" + freeAddress()
	peerEndpoint := "http://" + freeAddress()
	s.Endpoints = []string{endpoint}

	cmd := exec.Command(
		s.etcdPath,
		"--name=node1",
		"--data-dir="+dataDir,
		"--initial-advertise-peer-urls", peerEndpoint,
		"--listen-peer-urls", peerEndpoint,
		"--advertise-client-urls", endpoint,
		"--listen-client-urls", endpoint,
		"--initial-cluster", "node1="+peerEndpoint,
	)
	s.cmd = cmd
	s.cmd.Stderr = os.Stderr
	s.cmd.Stdout = ioutil.Discard
	err = s.cmd.Start()
	require.NoError(err)

	s.ConnectionString = fmt.Sprintf(
		"etcd:%s?dial-timeout=2s&dial-keep-alive-timeout=2s",
		strings.Join(s.Endpoints, ","))

	retries := 0
	for {
		srv, err := NewEtcd(s.ConnectionString)
		if err == nil {
			_ = srv.Close()
			break
		}

		retries++
		if retries >= 10 {
			require.Fail("cannot connect to etcd", err)
			break
		}
	}
}

func (s *EtcdLockSuite) TearDownTest() {
	assert := s.Assert()

	go s.cmd.Wait()

	err := s.cmd.Process.Kill()
	assert.NoError(err)

	err = os.RemoveAll(s.dataDir)
	assert.NoError(err)
}

func (s *EtcdLockSuite) TestUnavailableEtcd() {
	assert := s.Assert()

	service, err := New("etcd:https://localhost:19191?dial-timeout=2s")
	assert.NoError(err)

	_, err = service.NewSession(&SessionConfig{})
	assert.Error(err)
	err = service.Close()
	assert.NoError(err)
}

func (s *EtcdLockSuite) TestSmallTimeout() {
	assert := s.Assert()
	// etcd uses a timeout of at least 1 second
	// so whenever we specify a timeout > 0, it should be fixed to 1 second or more
	// instead of 0 (no timeout)
	service := s.NewService()
	cfg := &SessionConfig{
		Timeout: time.Millisecond * 1,
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

func freeAddress() string {
	// Ensure we get IPv6 address. etcd v2.3.0 on Travis with IPv6 fails.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestNewEtcdParseError(t *testing.T) {
	require := require.New(t)
	srv, err := NewEtcd("local:")
	require.Error(err)
	require.True(ErrUnsupportedService.Is(err))
	require.Nil(srv)
}

func TestParseEtcdConnectionstring(t *testing.T) {
	require := require.New(t)
	for _, tc := range []struct {
		Input     string
		Output    clientv3.Config
		ErrorKind *errors.Kind
	}{{
		Input: "etcd:http://foo:8888",
		Output: clientv3.Config{
			Endpoints: []string{"http://foo:8888"},
		},
	}, {
		Input: "etcd:http://foo:8888,http://bar:9999",
		Output: clientv3.Config{
			Endpoints: []string{"http://foo:8888", "http://bar:9999"},
		},
	}, {
		Input: "etcd:http://foo:8888,http://bar:9999?dial-timeout=2s",
		Output: clientv3.Config{
			Endpoints:   []string{"http://foo:8888", "http://bar:9999"},
			DialTimeout: 2 * time.Second,
		},
	}, {
		Input: "etcd:http://foo:8888,http://bar:9999?dial-timeout=1s&dial-timeout=2s",
		Output: clientv3.Config{
			Endpoints:   []string{"http://foo:8888", "http://bar:9999"},
			DialTimeout: 2 * time.Second,
		},
	}, {
		Input:     "etcd:http://foo:8888,http://bar:9999?dial-timeout=invalid",
		ErrorKind: ErrInvalidConnectionString,
	}, {
		Input: "etcd:http://foo:8888,http://bar:9999?auto-sync-interval=3s",
		Output: clientv3.Config{
			Endpoints:        []string{"http://foo:8888", "http://bar:9999"},
			AutoSyncInterval: 3 * time.Second,
		},
	}, {
		Input:     "etcd:http://foo:8888,http://bar:9999?auto-sync-interval=invalid",
		ErrorKind: ErrInvalidConnectionString,
	}, {
		Input: "etcd:http://foo:8888,http://bar:9999?dial-keep-alive-time=4s",
		Output: clientv3.Config{
			Endpoints:         []string{"http://foo:8888", "http://bar:9999"},
			DialKeepAliveTime: 4 * time.Second,
		},
	}, {
		Input:     "etcd:http://foo:8888,http://bar:9999?dial-keep-alive-time=invalid",
		ErrorKind: ErrInvalidConnectionString,
	}, {
		Input: "etcd:http://foo:8888,http://bar:9999?dial-keep-alive-timeout=5s",
		Output: clientv3.Config{
			Endpoints:            []string{"http://foo:8888", "http://bar:9999"},
			DialKeepAliveTimeout: 5 * time.Second,
		},
	}, {
		Input:     "etcd:http://foo:8888,http://bar:9999?dial-keep-alive-timeout=invalid",
		ErrorKind: ErrInvalidConnectionString,
	}, {
		Input: "etcd:http://foo:8888,http://bar:9999?username=foo&password=bar",
		Output: clientv3.Config{
			Endpoints: []string{"http://foo:8888", "http://bar:9999"},
			Username:  "foo",
			Password:  "bar",
		},
	}, {
		Input: "etcd:http://foo:8888,http://bar:9999?reject-old-cluster=true",
		Output: clientv3.Config{
			Endpoints:        []string{"http://foo:8888", "http://bar:9999"},
			RejectOldCluster: true,
		},
	}, {
		Input:     "etcd:http://foo:8888,http://bar:9999?reject-old-cluster=invalid",
		ErrorKind: ErrInvalidConnectionString,
	}, {
		Input:     "etcd:http://foo:8888?invalid-key=true",
		ErrorKind: ErrInvalidConnectionString,
	}, {
		Input:     "etcd://foo:8888?invalid-key=true",
		ErrorKind: ErrInvalidConnectionString,
	}, {
		Input:     ":",
		ErrorKind: ErrInvalidConnectionString,
	}, {
		Input:     "local:http://foo:8888",
		ErrorKind: ErrUnsupportedService,
	}} {
		cfg, err := parseEtcdConnectionstring(tc.Input)
		if tc.ErrorKind != nil {
			require.Error(err, tc.Input)
			require.True(tc.ErrorKind.Is(err), tc.Input)
			continue
		}

		require.NoError(err, tc.Input)
		require.Equal(tc.Output, cfg, tc.Input)
	}
}
