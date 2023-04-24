package common

import (
	"database/sql"
)

func ShowBrandDatabases(db *sql.DB) ([]string, error) {
	var databases []string
	rows, err := db.Query("SHOW DATABASES LIKE 'ky_hygl_%'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, err
		}

		databases = append(databases, dbName)
	}
	return databases, nil
}
