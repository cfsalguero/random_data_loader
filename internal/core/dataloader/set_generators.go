package dataloader

import (
	"strconv"
	"strings"
	"time"
)

func (l *TableDataLoader) SetGenerator(columnName string, generator DataGenerator) {
	l.Generators[columnName] = generator
}

// SetDefaultGenerators sets default generators based on column data types.
//
//nolint:gocognit // Ignore cognitive complexity
func (l *TableDataLoader) SetDefaultGenerators() error {
	for _, column := range l.TableStruct.Columns {
		dataType := strings.ToLower(column.DataType)

		// Handle special case: check if column is primary key with auto-increment
		isPrimaryAutoIncrement := false
		for _, index := range l.TableStruct.Indexes {
			if index.IsPrimary && len(index.Columns) == 1 && index.Columns[0] == column.Name {
				// For MySQL, auto-increment is not directly part of the table structure
				// For Postgres, we would check if there's a sequence attached, but that's more complex
				if strings.Contains(dataType, "int") {
					isPrimaryAutoIncrement = true
					break
				}
			}
		}

		if isPrimaryAutoIncrement {
			// Skip auto-increment columns
			continue
		}

		// Check if column is a foreign key
		isForeignKey := false
		for _, fk := range l.TableStruct.ForeignKeys {
			for _, col := range fk.Columns {
				if col == column.Name {
					isForeignKey = true
					break
				}
			}
			if isForeignKey {
				break
			}
		}

		if isForeignKey {
			// For foreign keys, we would ideally generate values that exist in the referenced table
			// But for simplicity, we'll just generate plausible data
			if strings.Contains(dataType, "int") {
				l.Generators[column.Name] = NewIntGenerator(1, 1000)
			} else if strings.Contains(dataType, "char") || strings.Contains(dataType, "text") {
				l.Generators[column.Name] = NewStringGenerator(8)
			}
			continue
		}

		// Set generator based on data type
		typeTokens := strings.Split(dataType, " ")
		switch typeTokens[0] {
		case "char", "varchar":
			// Extract length from type if possible
			length := 10 // Default
			if start := strings.Index(dataType, "("); start != -1 {
				if end := strings.Index(dataType[start:], ")"); end != -1 {
					if size, err := strconv.Atoi(dataType[start+1 : start+end]); err == nil {
						length = min(size, 100) // Cap at 100 chars for efficiency
					}
				}
			}
			l.Generators[column.Name] = NewStringGenerator(length)

		case "text":
			// Different sizes for different text types
			switch {
			case strings.Contains(dataType, "tiny"):
				l.Generators[column.Name] = NewStringGenerator(50)
			case strings.Contains(dataType, "medium"):
				l.Generators[column.Name] = NewStringGenerator(200)
			case strings.Contains(dataType, "long"):
				l.Generators[column.Name] = NewStringGenerator(500)
			default:
				l.Generators[column.Name] = NewStringGenerator(100)
			}

		// Numeric types
		case "tinyint":
			if strings.Contains(dataType, "unsigned") {
				l.Generators[column.Name] = NewIntGenerator(0, 255)
			} else {
				l.Generators[column.Name] = NewIntGenerator(-128, 127)
			}

		case "smallint":
			if strings.Contains(dataType, "unsigned") {
				l.Generators[column.Name] = NewIntGenerator(0, 65535)
			} else {
				l.Generators[column.Name] = NewIntGenerator(-32768, 32767)
			}

		case "mediumint":
			if strings.Contains(dataType, "unsigned") {
				l.Generators[column.Name] = NewIntGenerator(0, 16777215)
			} else {
				l.Generators[column.Name] = NewIntGenerator(-8388608, 8388607)
			}

		case "int", "integer":
			if strings.Contains(dataType, "unsigned") {
				l.Generators[column.Name] = NewIntGenerator(0, 2147483647)
			} else {
				l.Generators[column.Name] = NewIntGenerator(-2147483648, 2147483647)
			}

		case "bigint":
			if strings.Contains(dataType, "unsigned") {
				l.Generators[column.Name] = NewIntGenerator(0, 9223372036854775807)
			} else {
				l.Generators[column.Name] = NewIntGenerator(-9223372036854775808, 9223372036854775807)
			}

		case "float", "real":
			l.Generators[column.Name] = NewFloatGenerator(-1000.0, 1000.0, 2)

		case "double", "decimal", "numeric":
			precision := 2 // Default precision
			// Try to extract precision from the type
			if start := strings.Index(dataType, "("); start != -1 {
				if end := strings.Index(dataType[start:], ")"); end != -1 {
					parts := strings.Split(dataType[start+1:start+end], ",")
					if len(parts) > 1 {
						if p, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
							precision = p
						}
					}
				}
			}
			l.Generators[column.Name] = NewFloatGenerator(-10000.0, 10000.0, precision)

		case "bool", "boolean":
			l.Generators[column.Name] = NewBoolGenerator()

		// Date and time types
		case "date":
			start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			end := time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)
			l.Generators[column.Name] = NewDateGenerator(start, end)

		case "time":
			if !strings.Contains(dataType, "timestamp") {
				// Just time of day, not timestamp
				l.Generators[column.Name] = NewStringGenerator(8) // HH:MM:SS format
			}

		case "timestamp", "datetime":
			start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			end := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)
			withTZ := strings.Contains(dataType, "with time zone") || strings.Contains(dataType, "timestamptz")
			l.Generators[column.Name] = NewTimestampGenerator(start, end, withTZ)

		// Special types
		case "enum":
			values := parseEnumValues(column.DataType)
			l.Generators[column.Name] = NewEnumGenerator(values)

		case "set":
			values := parseEnumValues(column.DataType)
			l.Generators[column.Name] = NewEnumGenerator(values)

		case "json", "jsonb":
			l.Generators[column.Name] = NewJSONGenerator(3, 2, 3, l.DBType)

		case "uuid":
			l.Generators[column.Name] = NewUUIDGenerator()

		case "inet", "cidr":
			l.Generators[column.Name] = NewIPGenerator(false)

		case "macaddr":
			l.Generators[column.Name] = NewStringGenerator(17) // MAC address format

		case "bit", "varbit":
			length := 8 // Default
			if start := strings.Index(dataType, "("); start != -1 {
				if end := strings.Index(dataType[start:], ")"); end != -1 {
					if size, err := strconv.Atoi(dataType[start+1 : start+end]); err == nil {
						length = min(size, 64) // Cap at 64 bits
					}
				}
			}
			l.Generators[column.Name] = NewBitStringGenerator(length)

		case "blob", "binary", "bytea":
			var length int
			switch {
			case strings.Contains(dataType, "tiny"):
				length = 100
			case strings.Contains(dataType, "medium"):
				length = 1000
			case strings.Contains(dataType, "long"):
				length = 10000
			default:
				length = 500
			}
			l.Generators[column.Name] = NewBinaryGenerator(length)

		case "point", "geometry": //nolint:goconst // Ignore.
			geomType := "point"
			if strings.Contains(dataType, "linestring") {
				geomType = "linestring"
			} else if strings.Contains(dataType, "polygon") {
				geomType = "polygon"
			}
			l.Generators[column.Name] = NewGeometryGenerator(geomType)

		case "money":
			l.Generators[column.Name] = NewMoneyGenerator(0, 10000)

		case "interval":
			l.Generators[column.Name] = NewIntervalGenerator(0, 100)

		default:
			// For unknown types, use string generator as a fallback
			l.Generators[column.Name] = NewStringGenerator(10)
		}
	}

	return nil
}

// parseEnumValues extracts enum values from MySQL/PostgreSQL type definition.
func parseEnumValues(dataType string) []string {
	// Extract values between parentheses
	start := strings.Index(dataType, "(")
	end := strings.LastIndex(dataType, ")")

	if start == -1 || end == -1 || start >= end {
		return []string{"enum_value1", "enum_value2"} // Default fallback
	}

	// Get the content between parentheses
	valuesString := dataType[start+1 : end]

	// Split by comma and clean up quotes
	rawValues := strings.Split(valuesString, ",")
	values := make([]string, 0, len(rawValues))

	for _, val := range rawValues {
		// Remove quotes and whitespace
		cleanVal := strings.Trim(strings.TrimSpace(val), "'\"")
		if cleanVal != "" {
			values = append(values, cleanVal)
		}
	}

	if len(values) == 0 {
		return []string{"enum_value1", "enum_value2"} // Fallback
	}

	return values
}
