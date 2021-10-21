package persistance_mssql

import (
	"errors"
	"fmt"
	"strings"
	p "tableau_crud/persistance"
)

type FieldListClause struct {
	Fields []string
}

func (clause *FieldListClause) ToSqlSnippet(paramNames ...string) (*p.SqlSnippet, error) {
	if len(paramNames) > 0 {
		return nil, errors.New(fmt.Sprintf(`expecting 0 paramNames in ToSqlSnippet of FieldListClause but got %v`, len(paramNames)))
	}
	quoteds := make([]string, len(clause.Fields))
	for index, field := range clause.Fields {
		quoteds[index] = QuoteIdentifier(field)
	}
	return &p.SqlSnippet{
		Snippet: strings.Join(quoteds, `,`),
		Params:  []p.Param{},
	}, nil
}

func (clause *FieldListClause) ParamsRequired() int {
	return 0
}
