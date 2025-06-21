package dataloader_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cfsalguero/random_data_loader/internal/core/dataloader"
	mysqlparser "github.com/cfsalguero/random_data_loader/internal/core/services/mysql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func connectMySQL(ctx context.Context, host, port, user, pass, database string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, pass, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	for range 10 {
		err = db.PingContext(ctx)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	return db, err
}

func TestMySQLDataLoader(t *testing.T) {
	ctx := t.Context()

	db, err := connectMySQL(ctx, host, myPort, myUser, myPass, "testdb")
	require.NoError(t, err)
	defer db.Close()

	err = cleanup(db)
	assert.NoError(t, err)

	err = createTestTable(db, "mysql")
	require.NoError(t, err)

	tableStruct, err := mysqlparser.Parse(db, "testdb", "test_table")
	require.NoError(t, err)

	loader := dataloader.NewTableDataLoader(db, "mysql", tableStruct, batchSize, parallel)
	loader.SetDefaultGenerators()

	loader.BatchSize = 10
	loader.NumGoroutines = 1

	numRows := 100
	err = loader.LoadData(ctx, numRows)
	require.NoError(t, err)

	count, err := countRows(db)
	require.NoError(t, err)
	assert.Equal(t, numRows, count)

	//err = cleanup(db)
	//assert.NoError(t, err)
}
