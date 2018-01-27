package schema

import (
	"database/sql"
	"fmt"
)

// Create the database schema for all the models in the given database.
func Create(db *sql.DB) error {
	data, err := schemaSqlSchemaSqlBytes()
	if err != nil {
		return fmt.Errorf("unable to get database schema: %s", err)
	}

	_, err = db.Exec(string(data))
	if err != nil {
		return fmt.Errorf("unable to create database schema: %s", err)
	}

	return nil
}
