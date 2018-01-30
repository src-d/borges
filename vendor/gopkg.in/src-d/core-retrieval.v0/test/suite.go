package test

import (
	"database/sql"
	"errors"
	"fmt"
	"go/build"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/src-d/core-retrieval.v0/schema"
	"gopkg.in/src-d/framework.v0/database"

	"github.com/stretchr/testify/suite"
)

type Suite struct {
	suite.Suite
	dbName string
	DB     *sql.DB
}

func (s *Suite) Setup() {
	require := s.Require()
	s.dbName = fmt.Sprintf("db_%d", time.Now().UnixNano())
	db, err := database.Default()
	require.NoError(err, "can't get default database")

	require.NoError(db.Ping(), "unable to connect to the database")

	_, err = db.Exec("CREATE DATABASE " + s.dbName)
	require.NoError(err, "can't create database %s", s.dbName)
	s.NoError(db.Close(), "can't close database conn")

	s.DB, err = database.Default(database.WithName(s.dbName))
	require.NoError(err, "can't get default database with name %s", s.dbName)

	require.NoError(schema.Create(s.DB), "can't create database schema")
}

func (s *Suite) TearDown() {
	s.NoError(s.DB.Close())

	db, err := database.Default()
	s.NoError(err)

	_, err = db.Exec("DROP DATABASE " + s.dbName)
	s.NoError(err)
	s.NoError(db.Close())
}

var (
	rootDir    string
	schemaPath string
)

func init() {
	// First look at possible vendor directories
	srcs := vendorDirectories()

	// And then GOPATH
	srcs = append(srcs, build.Default.SrcDirs()...)

	for _, src := range srcs {
		rf := filepath.Join(src, "gopkg.in", "src-d", "core-retrieval.v0")

		if _, err := os.Stat(rf); err == nil {
			rootDir = rf
			schemaPath = filepath.Join(rootDir, "schema", "schema.sql")
			return
		}
	}

	panic(errors.New("core-retrieval.v0 directory not found"))
}

func vendorDirectories() []string {
	dir, err := os.Getwd()
	if err != nil {
		return nil
	}

	var dirs []string

	for {
		if dir == "." || dir == "/" {
			break
		}

		dirs = append(dirs, filepath.Join(dir, "vendor"))
		dir = filepath.Dir(dir)
	}

	return dirs
}
