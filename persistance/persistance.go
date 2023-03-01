package persistance

type Persistor interface {
	Insert(table string, values map[string]interface{}) (int64, error)
	Update(table string, where []SqlSnippetGenerator, updates []SqlSnippetGenerator) (int64, error)
	Delete(table string, where []SqlSnippetGenerator) (int64, error)
	Read(table string, fields []string, where []SqlSnippetGenerator, orderBy []string, pageSize int, page int) (*QueryResult, error)
	TestConnection(table string) (*QueryResult, error)
}

type SqlSnippetGenerator interface {
	ToSqlSnippet() *SqlSnippet
	ParamsRequired() int
}

type SqlSnippet struct {
	Snippet string
	Params  []interface{}
}

type SqlPart struct {
	Value  string
	Params []interface{}
}

type QueryResult struct {
	ColumnNames   []string
	RowCount      int
	Data          [][]interface{}
	TotalRowCount int
}
