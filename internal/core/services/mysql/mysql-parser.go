// Package mysqlparser implements the TableParser interface for MySQL databases
package mysqlparser

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/cfsalguero/random_data_loader/internal/core/domain"
	_ "github.com/go-sql-driver/mysql"
)

// Parse implements the TableParser interface for MySQL.
func Parse(dbConn any, schema, tableName string) (*domain.TableStructure, error) {
	db, ok := dbConn.(*sql.DB)
	if !ok {
		return nil, errors.New("invalid connection type, expected *sql.DB")
	}

	// Create table structure
	tableStruct := &domain.TableStructure{
		Name: tableName,
	}

	// Get columns
	if err := parseColumns(db, schema, tableName, tableStruct); err != nil {
		return nil, fmt.Errorf("error parsing columns: %w", err)
	}

	// Get indexes
	if err := parseIndexes(db, schema, tableName, tableStruct); err != nil {
		return nil, fmt.Errorf("error parsing indexes: %w", err)
	}

	// Get foreign keys
	if err := parseForeignKeys(db, schema, tableName, tableStruct); err != nil {
		return nil, fmt.Errorf("error parsing foreign keys: %w", err)
	}

	return tableStruct, nil
}

// parseColumns fetches and parses the columns of a table.
func parseColumns(db *sql.DB, schema, tableName string, tableStruct *domain.TableStructure) error {
	query := `
		SELECT 
			COLUMN_NAME, 
			DATA_TYPE, 
			IS_NULLABLE, 
			COLUMN_DEFAULT
		FROM 
			INFORMATION_SCHEMA.COLUMNS 
		WHERE 
			TABLE_SCHEMA = ? 
			AND TABLE_NAME = ?
		ORDER BY 
			ORDINAL_POSITION
	`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var column domain.TableColumn
		var isNullable, columnDefault sql.NullString

		if err := rows.Scan(&column.Name, &column.DataType, &isNullable, &columnDefault); err != nil {
			return err
		}

		column.Nullable = strings.ToUpper(isNullable.String) == "YES"
		if columnDefault.Valid {
			column.Default = columnDefault.String
		}

		tableStruct.Columns = append(tableStruct.Columns, column)
	}

	return rows.Err()
}

// parseIndexes fetches and parses the indexes of a table.
func parseIndexes(db *sql.DB, schema, tableName string, tableStruct *domain.TableStructure) error {
	query := `
		SELECT 
			INDEX_NAME,
			COLUMN_NAME,
			NON_UNIQUE
		FROM 
			INFORMATION_SCHEMA.STATISTICS
		WHERE 
			TABLE_SCHEMA = ? 
			AND TABLE_NAME = ?
		ORDER BY 
			INDEX_NAME, 
			SEQ_IN_INDEX
	`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	indexMap := make(map[string]*domain.TableIndex)

	for rows.Next() {
		var indexName, columnName string
		var nonUnique int

		if err := rows.Scan(&indexName, &columnName, &nonUnique); err != nil {
			return err
		}

		index, exists := indexMap[indexName]
		if !exists {
			index = &domain.TableIndex{
				Name:      indexName,
				IsUnique:  nonUnique == 0,
				IsPrimary: indexName == "PRIMARY",
			}
			indexMap[indexName] = index
		}

		index.Columns = append(index.Columns, columnName)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Convert map to slice
	for _, index := range indexMap {
		tableStruct.Indexes = append(tableStruct.Indexes, *index)
	}

	return nil
}

// parseForeignKeys fetches and parses the foreign keys of a table.
func parseForeignKeys(db *sql.DB, schema, tableName string, tableStruct *domain.TableStructure) error {
	query := `
		SELECT 
			CONSTRAINT_NAME,
			COLUMN_NAME,
			REFERENCED_TABLE_NAME,
			REFERENCED_COLUMN_NAME
		FROM 
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE 
			TABLE_SCHEMA = ? 
			AND TABLE_NAME = ?
			AND REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY 
			CONSTRAINT_NAME, 
			ORDINAL_POSITION
	`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	fkMap := make(map[string]*domain.ForeignKey)

	for rows.Next() {
		var constraintName, columnName, referencedTable, referencedColumn string

		if err := rows.Scan(&constraintName, &columnName, &referencedTable, &referencedColumn); err != nil {
			return err
		}

		fk, exists := fkMap[constraintName]
		if !exists {
			fk = &domain.ForeignKey{
				Name:            constraintName,
				ReferencedTable: referencedTable,
			}
			fkMap[constraintName] = fk
		}

		fk.Columns = append(fk.Columns, columnName)
		fk.ReferencedColumns = append(fk.ReferencedColumns, referencedColumn)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Convert map to slice
	for _, fk := range fkMap {
		tableStruct.ForeignKeys = append(tableStruct.ForeignKeys, *fk)
	}

	return nil
}
