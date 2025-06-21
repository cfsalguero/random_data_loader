// Package postgresparser implements the TableParser interface for PostgreSQL databases
package postgresparser

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/cfsalguero/random_data_loader/internal/core/domain"
	_ "github.com/lib/pq"
)

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
			column_name, 
			data_type, 
			is_nullable, 
			column_default
		FROM 
			information_schema.columns 
		WHERE 
			table_catalog = $1 
			AND table_name = $2
		ORDER BY 
			ordinal_position
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
			i.relname AS index_name,
			a.attname AS column_name,
			ix.indisunique AS is_unique,
			ix.indisprimary AS is_primary
		FROM
			pg_class t,
			pg_class i,
			pg_index ix,
			pg_attribute a,
			pg_namespace n
		WHERE
			t.oid = ix.indrelid
			AND i.oid = ix.indexrelid
			AND a.attrelid = t.oid
			AND a.attnum = ANY(ix.indkey)
			AND t.relnamespace = n.oid
			AND n.nspname = $1
			AND t.relname = $2
		ORDER BY
			i.relname,
			a.attnum
	`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	indexMap := make(map[string]*domain.TableIndex)

	for rows.Next() {
		var indexName, columnName string
		var isUnique, isPrimary bool

		if err := rows.Scan(&indexName, &columnName, &isUnique, &isPrimary); err != nil {
			return err
		}

		index, exists := indexMap[indexName]
		if !exists {
			index = &domain.TableIndex{
				Name:      indexName,
				IsUnique:  isUnique,
				IsPrimary: isPrimary,
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
			tc.constraint_name,
			kcu.column_name,
			ccu.table_name AS referenced_table,
			ccu.column_name AS referenced_column
		FROM
			information_schema.table_constraints AS tc
			JOIN information_schema.key_column_usage AS kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.table_schema = kcu.table_schema
			JOIN information_schema.constraint_column_usage AS ccu
				ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
		WHERE
			tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2
		ORDER BY
			tc.constraint_name,
			kcu.ordinal_position
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
