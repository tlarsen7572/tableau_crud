package persistance

type Persistor interface {
	Insert(values map[string]interface{}) (int, error)
	Update(where []SqlSnippetGenerator, updates []SqlSnippetGenerator) (int, error)
	Delete(where []SqlSnippetGenerator) (int, error)
	Read(fields []string, where []SqlSnippetGenerator, orderBy []string, pageSize int, page int) (*QueryResult, error)
	TestConnection() (*QueryResult, error)
}

type SqlSnippetGenerator interface {
	ToSqlSnippet(paramNames ...string) (*SqlSnippet, error)
	ParamsRequired() int
}

type SqlSnippet struct {
	Snippet string
	Params  []Param
}

type SqlPart struct {
	Value     string
	Params    []Param
	NextParam int
}

type Param struct {
	Name  string
	Value interface{}
}

type QueryResult struct {
	ColumnNames   []string
	RowCount      int
	Data          [][]interface{}
	TotalRowCount int
}
