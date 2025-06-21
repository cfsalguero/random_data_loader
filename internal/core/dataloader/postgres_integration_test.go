package dataloader_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cfsalguero/random_data_loader/internal/core/dataloader"
	postgresparser "github.com/cfsalguero/random_data_loader/internal/core/services/postgres"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to connect to Postgres DB.
func connectPostgres(ctx context.Context, host, port, user, pass, database string) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host,
		port,
		user,
		pass,
		database,
	)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	// Wait for connection to be established
	for range 10 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// continue
		}

		err = db.Ping()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	return db, err
}

func TestPostgresDataLoader(t *testing.T) {
	ctx := t.Context()

	db, err := connectPostgres(ctx, host, pgPort, pgUser, pgPass, database)
	require.NoError(t, err)
	defer db.Close()

	err = cleanup(db)
	require.NoError(t, err)

	err = createTestTable(db, "postgres")
	require.NoError(t, err)

	tableStruct, err := postgresparser.Parse(db, "testdb", "test_table")
	require.NoError(t, err)

	loader := dataloader.NewTableDataLoader(db, "postgres", tableStruct, batchSize, parallel)
	loader.SetDefaultGenerators()

	loader.BatchSize = 10
	loader.NumGoroutines = 1

	numRows := 100
	err = loader.LoadData(ctx, numRows, batchSize)
	require.NoError(t, err)

	count, err := countRows(db)
	require.NoError(t, err)
	assert.Equal(t, numRows, count)

	err = cleanup(db)
	assert.NoError(t, err)
}
