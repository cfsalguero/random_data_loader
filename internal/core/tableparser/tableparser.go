package tableparser

import "github.com/cfsalguero/random_data_loader/internal/core/domain"

// TableParser defines the interface for parsing database tables.
type TableParser interface {
	// Parse fetches and returns the table structure for the given table name
	Parse(dbConn any, schema, tableName string) (*domain.TableStructure, error)
}
