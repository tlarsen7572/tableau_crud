package persistance

import (
	"fmt"
	"strings"
)

type EqualClause struct {
	Identifier string
	Value      interface{}
}

func (clause *EqualClause) ToSqlSnippet() *SqlSnippet {
	quoted := QuoteIdentifier(clause.Identifier)
	whereClause := fmt.Sprintf(`%v=?`, quoted)
	return &SqlSnippet{
		Snippet: whereClause,
		Params:  []interface{}{clause.Value},
	}
}

func (clause *EqualClause) ParamsRequired() int {
	return 1
}

type InClause struct {
	Identifier string
	Exclude    bool
	Values     []interface{}
}

func (clause *InClause) ToSqlSnippet() *SqlSnippet {
	quoted := QuoteIdentifier(clause.Identifier)
	var whereClause string
	if values := len(clause.Values); values > 0 {
		inValues := `?` + strings.Repeat(`,?`, values-1)
		whereClause = fmt.Sprintf(`%v IN (%v)`, quoted, inValues)
	} else {
		whereClause = `1=2`
	}
	if clause.Exclude {
		whereClause = fmt.Sprintf(`NOT (%v)`, whereClause)
	}
	return &SqlSnippet{
		Snippet: whereClause,
		Params:  clause.Values,
	}
}

func (clause *InClause) ParamsRequired() int {
	return len(clause.Values)
}

type RangeClause struct {
	Identifier   string
	IncludeNulls bool
	MinValue     interface{}
	MaxValue     interface{}
}

func (clause *RangeClause) ToSqlSnippet() *SqlSnippet {
	params := clause.generateParams()
	quoted := QuoteIdentifier(clause.Identifier)
	var whereClause string

	if clause.MinValue == nil && clause.MaxValue == nil {
		whereClause = fmt.Sprintf(`%v IS NULL`, quoted)
	} else if clause.MinValue != nil && clause.MaxValue == nil {
		whereClause = fmt.Sprintf(`%v >= ?`, quoted)
	} else if clause.MinValue == nil && clause.MaxValue != nil {
		whereClause = fmt.Sprintf(`%v <= ?`, quoted)
	} else {
		whereClause = fmt.Sprintf(`%v BETWEEN ? AND ?`, quoted)
	}
	if clause.IncludeNulls {
		whereClause = fmt.Sprintf(`(%v OR %v IS NULL)`, whereClause, quoted)
	}
	return &SqlSnippet{
		Snippet: whereClause,
		Params:  params,
	}
}

func (clause *RangeClause) ParamsRequired() int {
	if clause.MinValue == nil || clause.MaxValue == nil {
		return 1
	}
	return 2
}

func (clause *RangeClause) generateParams() []interface{} {
	params := make([]interface{}, 0)
	if clause.MinValue != nil {
		params = append(params, clause.MinValue)
	}
	if clause.MaxValue != nil {
		params = append(params, clause.MaxValue)
	}
	return params
}

func GenerateCombinedWhereClause(clauses []SqlSnippetGenerator) *SqlPart {
	wheres := make([]string, 0, len(clauses))
	allParams := make([]interface{}, 0)
	for _, clause := range clauses {
		where := clause.ToSqlSnippet()
		wheres = append(wheres, where.Snippet)
		allParams = append(allParams, where.Params...)
	}
	return &SqlPart{Value: strings.Join(wheres, ` AND `), Params: allParams}
}
