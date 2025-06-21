package dataloader_test

import (
	"context"
	"database/sql"
	"os/exec"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const (
	host      = "127.0.0.1"
	myPort    = "3306"
	pgPort    = "5432"
	myUser    = "root"
	myPass    = "root"
	pgUser    = "postgres"
	pgPass    = "root"
	database  = "testdb"
	batchSize = 1000
	parallel  = 1
)

func startContainers(ctx context.Context) error {
	cmd := exec.Command("docker-compose", "-f", "docker-compose.yml", "up", "-d")
	return cmd.Start()
}

// Helper to create test table with various field types.
func createTestTable(db *sql.DB, dbType string) error {
	var createTableSQL string

	switch dbType {
	case "mysql":
		createTableSQL = `
		CREATE TABLE IF NOT EXISTS test_table (
			id INT AUTO_INCREMENT PRIMARY KEY,
			int_col INT,
			float_col FLOAT,
			varchar_col VARCHAR(100),
			text_col TEXT,
			bool_col BOOLEAN,
			date_col DATE,
			timestamp_col TIMESTAMP
		)
		`
	case "postgres":
		createTableSQL = `
		CREATE TABLE IF NOT EXISTS test_table (
			id SERIAL PRIMARY KEY,
			int_col INTEGER,
			float_col REAL,
			varchar_col VARCHAR(100),
			text_col TEXT,
			bool_col BOOLEAN,
			date_col DATE,
			timestamp_col TIMESTAMP
		)
		`
	}

	_, err := db.Exec(createTableSQL)
	return err
}

// Helper to count rows in test table.
func countRows(db *sql.DB) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	return count, err
}

func cleanup(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS test_table CASCADE")

	return err
}
