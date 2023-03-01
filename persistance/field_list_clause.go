package persistance

import (
	"strings"
)

type FieldListClause struct {
	Fields []string
}

func (clause *FieldListClause) ToSqlSnippet() *SqlSnippet {
	quoteds := make([]string, len(clause.Fields))
	for index, field := range clause.Fields {
		quoteds[index] = QuoteIdentifier(field)
	}
	return &SqlSnippet{
		Snippet: strings.Join(quoteds, `,`),
		Params:  make([]interface{}, 0),
	}
}

func (clause *FieldListClause) ParamsRequired() int {
	return 0
}
