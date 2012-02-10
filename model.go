package mry

// Database model
type Model struct {
	*tableCollection
}

func newModel() *Model {
	return &Model{newTableCollection(nil)}
}


// Structure that represents a table in the storage
type Table struct {
	Name        string

	indexes     []Index
	parentTable *Table
	subTables   *tableCollection
}

func newTable(name string) *Table {
	t := &Table{
		Name: name,
		indexes: make([]Index, 0),
	}
	t.subTables = newTableCollection(t)
	return t
}

func (t *Table) AddSubTable(name string) *Table {
	return t.subTables.CreateTable(name)
}

func (t *Table) SubTables() []*Table {
	return t.subTables.ToSlice()
}

// Index on a field of table
type Index struct {
	Field string
}

// Colection of tables
type tableCollection struct {
	parentTable   *Table
	tablesMap     map[string]*Table
	tablesArr     []*Table
}

func newTableCollection(parent *Table) *tableCollection {
	return &tableCollection{
		parentTable: parent,
		tablesMap: make(map[string]*Table),
	}
}

func (c *tableCollection) Len() int {
	return len(c.tablesMap)
}

func (c *tableCollection) GetTable(name string) *Table {
	return c.tablesMap[name]
}

func (c *tableCollection) CreateTable(name string) *Table {
	if table, found := c.tablesMap[name]; found {
		return table
	}

	table := newTable(name)
	table.parentTable = c.parentTable
	c.tablesMap[name] = table
	c.tablesArr = nil

	return table
}

func (c *tableCollection) ToSlice() []*Table {
	if c.tablesArr == nil {
		c.tablesArr = make([]*Table, len(c.tablesMap))

		i := 0
		for _, v := range c.tablesMap {
			c.tablesArr[i] = v
			i++
		}
	}
	return c.tablesArr
}
