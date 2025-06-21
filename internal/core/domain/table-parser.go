package domain

// TableColumn represents a column in a database table.
type TableColumn struct {
	Name     string
	DataType string
	Nullable bool
	Default  string
}

// TableIndex represents an index in a database table.
type TableIndex struct {
	Name      string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
}

// ForeignKey represents a foreign key constraint in a database table.
type ForeignKey struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
}

// TableStructure represents the structure of a database table.
type TableStructure struct {
	Name        string
	Columns     []TableColumn
	Indexes     []TableIndex
	ForeignKeys []ForeignKey
}
