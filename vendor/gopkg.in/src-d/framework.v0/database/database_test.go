package database

import (
	"database/sql"
	"testing"
	"time"

	defaults "github.com/mcuadros/go-defaults"
	"github.com/stretchr/testify/require"
)

func invalid() *DatabaseConfig {
	return new(DatabaseConfig)
}

func mkConf(fn func(*DatabaseConfig)) *DatabaseConfig {
	c := new(DatabaseConfig)
	defaults.SetDefaults(c)
	fn(c)
	return c
}

func TestDataSourceName(t *testing.T) {
	require := require.New(t)

	var defaultConf = new(DatabaseConfig)
	defaults.SetDefaults(defaultConf)

	cases := []struct {
		name     string
		conf     *DatabaseConfig
		expected string
		err      bool
	}{
		{
			"default",
			defaultConf,
			"postgres://testing:testing@0.0.0.0:5432/testing?sslmode=disable&connect_timeout=30",
			false,
		},
		{
			"empty",
			&DatabaseConfig{},
			"",
			true,
		},
		{
			"no name",
			mkConf(func(c *DatabaseConfig) {
				c.Name = ""
			}),
			"",
			true,
		},
		{
			"no port",
			mkConf(func(c *DatabaseConfig) {
				c.Port = 0
			}),
			"",
			true,
		},
		{
			"no host",
			mkConf(func(c *DatabaseConfig) {
				c.Host = ""
			}),
			"",
			true,
		},
		{
			"no user",
			mkConf(func(c *DatabaseConfig) {
				c.Username = ""
			}),
			"",
			true,
		},
		{
			"no sslmode",
			mkConf(func(c *DatabaseConfig) {
				c.SSLMode = SSLMode("")
			}),
			"postgres://testing:testing@0.0.0.0:5432/testing?sslmode=disable&connect_timeout=30",
			false,
		},
		{
			"custom no timeout",
			&DatabaseConfig{
				Username: "foo",
				Password: "bar",
				Host:     "baz",
				Port:     1337,
				Name:     "qux",
				SSLMode:  VerifyFull,
			},
			"postgres://foo:bar@baz:1337/qux?sslmode=verify-full",
			false,
		},
		{
			"custom app name",
			mkConf(func(c *DatabaseConfig) {
				c.AppName = "myapp"
			}),
			"postgres://testing:testing@0.0.0.0:5432/testing?sslmode=disable&application_name=myapp&connect_timeout=30",
			false,
		},
		{
			"custom timeout",
			mkConf(func(c *DatabaseConfig) {
				c.Timeout = 1 * time.Second
			}),
			"postgres://testing:testing@0.0.0.0:5432/testing?sslmode=disable&connect_timeout=1",
			false,
		},
	}

	for _, c := range cases {
		ds, err := c.conf.DataSourceName()
		if c.err {
			require.NotNil(err, c.name)
		} else {
			require.Equal(c.expected, ds, c.name)
		}
	}
}

func TestGet(t *testing.T) {
	require := require.New(t)

	_, err := Get(nil)
	require.Equal(ErrNoConfig, err)

	db, err := Get(DefaultConfig)
	require.Nil(err)
	require.NotNil(db)
	close(t, db)

	db, err = Get(DefaultConfig, WithName("Foo"))
	require.Nil(err)
	require.NotNil(db)
	close(t, db)

	_, err = Get(DefaultConfig, WithName(""))
	require.NotNil(err)
}

func TestDefault(t *testing.T) {
	require := require.New(t)
	db, err := Default()
	require.Nil(err)
	require.NotNil(db)
	close(t, db)

	_, err = Default(WithName(""))
	require.NotNil(err)
}

func TestMust(t *testing.T) {
	require := require.New(t)

	require.NotPanics(func() {
		close(t, Must(Default()))
	})

	require.Panics(func() {
		close(t, Must(Get(nil)))
	})
}

func TestWithName(t *testing.T) {
	cfg := WithName("foo")(DefaultConfig)
	require.Equal(t, "foo", cfg.Name)
}

func close(t *testing.T, db *sql.DB) {
	require.Nil(t, db.Close())
}
