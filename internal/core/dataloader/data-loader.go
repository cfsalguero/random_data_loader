// Package dataloader provides functionality to load random data into database tables
package dataloader

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/cfsalguero/random_data_loader/internal/core/domain"
	"github.com/rs/zerolog/log"
)

// TableDataLoader handles loading random data into a database table.
type TableDataLoader struct {
	DB            *sql.DB
	DBType        string // "mysql" or "postgres"
	TableStruct   *domain.TableStructure
	Generators    map[string]DataGenerator
	BatchSize     int
	NumGoroutines int
}

// NewTableDataLoader creates a new table data loader.
func NewTableDataLoader(
	db *sql.DB,
	dbType string,
	tableStruct *domain.TableStructure,
	batchSize, parallel int,
) *TableDataLoader {
	return &TableDataLoader{
		DB:            db,
		DBType:        dbType,
		TableStruct:   tableStruct,
		Generators:    make(map[string]DataGenerator),
		BatchSize:     batchSize,
		NumGoroutines: parallel,
	}
}

func (l *TableDataLoader) LoadData(ctx context.Context, numRows, batchSize int) error {
	// Prepare column names and placeholders for the insert statement
	var columnNames []string
	var placeholders []string

	for _, column := range l.TableStruct.Columns {
		// Skip columns that don't have generators
		if _, ok := l.Generators[column.Name]; !ok {
			continue
		}
		columnNames = append(columnNames, column.Name)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		l.TableStruct.Name,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "),
	)

	// Replace ? with $n for PostgreSQL
	if l.DBType == "postgres" {
		for i := 1; i <= len(placeholders); i++ {
			query = strings.Replace(query, "?", fmt.Sprintf("$%d", i), 1)
		}
	}

	ch := l.generateRows(ctx, numRows, batchSize)
	wg := &sync.WaitGroup{}
	for range l.NumGoroutines {
		wg.Add(1)
		go l.Load(ctx, query, ch, wg)
	}
	wg.Wait()

	return nil
}

func (l *TableDataLoader) generateRows(ctx context.Context, numRows, batchSize int) chan []any {
	ch := make(chan []any, batchSize)
	go func() {
		for range numRows {
			var values []any
			for _, column := range l.TableStruct.Columns {
				generator, ok := l.Generators[column.Name]
				if !ok {
					continue
				}
				values = append(values, generator.GenerateValue())
			}
			select {
			case <-ctx.Done():
				return
			default:
				ch <- values
			}
		}
		close(ch)
	}()

	return ch
}

//nolint:gocognit // It is more clear this way.
func (l *TableDataLoader) Load(ctx context.Context, query string, valuesChan chan []any, wg *sync.WaitGroup) {
	defer wg.Done()
	tx, _ := l.DB.BeginTx(ctx, nil)
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		log.Error().Str("worker failed to prepare statement", err.Error())
		return
	}
	defer stmt.Close()

	count := 0
OuterLoop:

	for values := range valuesChan {
		select {
		case <-ctx.Done():
			break OuterLoop
		default:
		}

		if _, err = stmt.ExecContext(ctx, values...); err != nil {
			log.Error().Str("worker failed to execute statement", err.Error())
			return
		}

		count++
		if count >= l.BatchSize {
			if err = tx.Commit(); err != nil {
				log.Error().Str("worker failed to commit transaction", err.Error())
				return
			}

			tx, err = l.DB.BeginTx(ctx, nil)
			if err != nil {
				log.Error().Str("worker failed to begin transaction", err.Error())
				return
			}

			stmt, err = tx.PrepareContext(ctx, query)
			if err != nil {
				log.Error().Str("worker failed to prepare statement", err.Error())
				return
			}

			count = 0
		}
	}

	//if count > 0 {
	if err = tx.Commit(); err != nil {
		log.Error().Str("worker failed to commit transaction", err.Error())
	}
	//}
}
