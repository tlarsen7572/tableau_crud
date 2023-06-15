package persistance

import (
	"fmt"
	"strings"
)

type UpdateClause struct {
	Identifier string
	NewValue   interface{}
}

func (clause *UpdateClause) ToSqlSnippet() *SqlSnippet {
	quoted := QuoteIdentifier(clause.Identifier)
	updateClause := fmt.Sprintf(`%v=?`, quoted)

	return &SqlSnippet{
		Snippet: updateClause,
		Params:  []interface{}{clause.NewValue},
	}
}

func (clause *UpdateClause) ParamsRequired() int {
	return 1
}

func GenerateCombinedUpdateClause(clauses []SqlSnippetGenerator) *SqlPart {
	updates := make([]string, 0, len(clauses))
	allParams := make([]interface{}, 0, len(clauses))
	for _, clause := range clauses {
		update := clause.ToSqlSnippet()
		updates = append(updates, update.Snippet)
		allParams = append(allParams, update.Params[0])
	}
	return &SqlPart{Value: strings.Join(updates, `,`), Params: allParams}
}
