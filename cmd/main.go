package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/cfsalguero/random_data_loader/internal/core/dataloader"
	"github.com/cfsalguero/random_data_loader/internal/core/domain"
	mysqlparser "github.com/cfsalguero/random_data_loader/internal/core/services/mysql"
	postgresparser "github.com/cfsalguero/random_data_loader/internal/core/services/postgres"
)

type cliOptions struct {
	DBType    string `kong:"name='type',enum='mysql,postgres',default='mysql',required,help='Database type (mysql or postgres)'"`
	DSN       string `kong:"name='dsn',default='root=root@tcp(localhost=3306)/my_database',required,help='Database connection string'"`
	Schema    string `kong:"name='schema',default='my_database',required,help='Database schema name'"`
	Table     string `kong:"name='table',default='test_table',required,help='Table name to parse'"`
	NumRows   int    `kong:"name='rows',default='10',help='Number of rows to generate'"`
	Parallel  int    `kong:"name='parallel',default='1',help='Number of parallel processes to use'"`
	BatchSize int    `kong:"name='batch',default='1000',help='Batch size for inserting data'"`
	LogLevel  string `kong:"name='log',default='info',enum='debug,info,warn,error',help='Log level (debug, info, warn, error)'"`
}

func main() {
	var cli cliOptions
	kong.Parse(&cli)
	setLogger(cli.LogLevel)

	db, err := dbConnect(cli.DBType, cli.DSN)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot connect to the database")
	}

	var tableStruct *domain.TableStructure

	start := time.Now()

	switch cli.DBType {
	case "mysql":
		tableStruct, err = mysqlparser.Parse(db, cli.Schema, cli.Table)
	case "postgres":
		tableStruct, err = postgresparser.Parse(db, cli.Schema, cli.Table)
	}
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse table structure")
	}

	logTableStruct(tableStruct)

	log.Info().Any("Parsed table structure in ", time.Since(start))
	log.Info().Msgf("Loading %d random rows into %s...\n", cli.NumRows, cli.Table)
	start = time.Now()

	loader := dataloader.NewTableDataLoader(db, cli.DBType, tableStruct, cli.BatchSize, cli.Parallel)

	if err = loader.SetDefaultGenerators(); err != nil {
		log.Fatal().Err(err).Msg("Failed to set default generators")
	}

	ctx := context.Background()
	if err = loader.LoadData(ctx, cli.NumRows, cli.BatchSize); err != nil {
		log.Fatal().Err(err).Msg("Failed to load data")
	}

	log.Info().Msgf("Loaded %d rows in %v\n", cli.NumRows, time.Since(start))
	log.Info().Msgf("Average insertion rate: %.2f rows/sec\n", float64(cli.NumRows)/time.Since(start).Seconds())

	_ = db.Close()
}

func logTableStruct(tableStruct *domain.TableStructure) {
	log.Debug().Str("Table", tableStruct.Name)

	log.Debug().Msg("Columns:")
	for _, column := range tableStruct.Columns {
		nullable := "NOT NULL"
		if column.Nullable {
			nullable = "NULL"
		}

		defaultValue := ""
		if column.Default != "" {
			defaultValue = fmt.Sprintf(" DEFAULT %s", column.Default)
		}

		log.Debug().Msgf("  %s %s %s%s\n", column.Name, column.DataType, nullable, defaultValue)
	}

	log.Debug().Msg("Indexes:")
	for _, index := range tableStruct.Indexes {
		indexType := "INDEX"
		if index.IsPrimary {
			indexType = "PRIMARY KEY"
		} else if index.IsUnique {
			indexType = "UNIQUE INDEX"
		}

		log.Debug().Msgf("  %s %s (%s)\n", indexType, index.Name, strings.Join(index.Columns, ", "))
	}

	log.Debug().Msg("Foreign Keys:")
	for _, fk := range tableStruct.ForeignKeys {
		log.Debug().Msgf("  %s (%s) REFERENCES %s (%s)\n",
			fk.Name,
			strings.Join(fk.Columns, ", "),
			fk.ReferencedTable,
			strings.Join(fk.ReferencedColumns, ", "),
		)
	}
}

func dbConnect(dbType, dsn string) (*sql.DB, error) {
	var db *sql.DB
	var err error

	switch dbType {
	case "mysql":
		db, err = sql.Open("mysql", dsn)
	case "postgres":
		db, err = sql.Open("postgres", dsn)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}

	if err != nil {
		return nil, err
	}

	// Test the database connection
	if err = db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
func setLogger(level string) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix //nolint:reassign // Setting the default logger.

	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}
