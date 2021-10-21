package persistance_mssql

import (
	"errors"
	"fmt"
	"strings"
	p "tableau_crud/persistance"
)

type EqualClause struct {
	Identifier string
	Value      interface{}
}

func (clause *EqualClause) ToSqlSnippet(paramNames ...string) (*p.SqlSnippet, error) {
	if len(paramNames) != 1 {
		return nil, errors.New(fmt.Sprintf(`expecting 1 paramName in ToSqlSnippet of EqualClause but got %v`, len(paramNames)))
	}
	paramName := paramNames[0]
	err := ValidateParam(paramName)
	if err != nil {
		return nil,
			errors.New(fmt.Sprintf(`error validating paramName in ToSqlSnippet of EqualClause: %v`, err.Error()))
	}
	quoted := QuoteIdentifier(clause.Identifier)
	whereClause := fmt.Sprintf(`%v=@%v`, quoted, paramName)
	return &p.SqlSnippet{
		Snippet: whereClause,
		Params:  []p.Param{{Name: paramName, Value: clause.Value}},
	}, nil
}

func (clause *EqualClause) ParamsRequired() int {
	return 1
}

type InClause struct {
	Identifier string
	Exclude    bool
	Values     []interface{}
}

func (clause *InClause) ToSqlSnippet(paramNames ...string) (*p.SqlSnippet, error) {
	if len(paramNames) != len(clause.Values) {
		return nil, errors.New(fmt.Sprintf(`expecting %v paramNames in ToSqlSnippet of InClause but got %v`, len(clause.Values), len(paramNames)))
	}

	quoted := QuoteIdentifier(clause.Identifier)
	var whereClause string
	if len(paramNames) > 0 {
		inValues := "@" + strings.Join(paramNames, `,@`)
		whereClause = fmt.Sprintf(`%v IN (%v)`, quoted, inValues)
	} else {
		whereClause = `1=2`
	}
	if clause.Exclude {
		whereClause = fmt.Sprintf(`NOT (%v)`, whereClause)
	}
	params := []p.Param{}
	for index, paramName := range paramNames {
		err := ValidateParam(paramName)
		if err != nil {
			return nil,
				errors.New(fmt.Sprintf(`error validating paramName in ToSqlSnippet of InClause: %v`, err.Error()))
		}
		params = append(params, p.Param{Name: paramName, Value: clause.Values[index]})
	}
	return &p.SqlSnippet{
		Snippet: whereClause,
		Params:  params,
	}, nil
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

func (clause *RangeClause) ToSqlSnippet(paramNames ...string) (*p.SqlSnippet, error) {
	if clause.MinValue == nil && clause.MaxValue == nil {
		return nil, errors.New(`both MinValue and MaxValue were nil; at least one must be provided`)
	}
	if required := clause.ParamsRequired(); len(paramNames) != required {
		return nil, errors.New(fmt.Sprintf(`expecting %v paramNames in ToSqlSnippet of RangeClause but got %v`, required, len(paramNames)))
	}

	for _, paramName := range paramNames {
		err := ValidateParam(paramName)
		if err != nil {
			return nil, err
		}
	}
	params := clause.generateParams(paramNames...)

	quoted := QuoteIdentifier(clause.Identifier)
	var whereClause string
	if clause.MinValue != nil && clause.MaxValue == nil {
		whereClause = fmt.Sprintf(`%v >= @%v`, quoted, paramNames[0])
	} else if clause.MinValue == nil && clause.MaxValue != nil {
		whereClause = fmt.Sprintf(`%v <= @%v`, quoted, paramNames[0])
	} else {
		whereClause = fmt.Sprintf(`%v BETWEEN @%v AND @%v`, quoted, paramNames[0], paramNames[1])
	}
	if clause.IncludeNulls {
		whereClause = fmt.Sprintf(`(%v OR %v IS NULL)`, whereClause, quoted)
	}
	return &p.SqlSnippet{
		Snippet: whereClause,
		Params:  params,
	}, nil
}

func (clause *RangeClause) ParamsRequired() int {
	if clause.MinValue == nil || clause.MaxValue == nil {
		return 1
	}
	return 2
}

func (clause *RangeClause) generateParams(paramNames ...string) []p.Param {
	params := []p.Param{}
	if clause.MinValue != nil {
		params = append(params, p.Param{Name: paramNames[0], Value: clause.MinValue})
	} else {
		params = append(params, p.Param{Name: paramNames[0], Value: clause.MaxValue})
		return params
	}
	if clause.MaxValue != nil {
		params = append(params, p.Param{Name: paramNames[1], Value: clause.MaxValue})
	}
	return params
}

func GenerateCombinedWhereClause(clauses []p.SqlSnippetGenerator, startParamAt int) (*p.SqlPart, error) {
	paramNum := startParamAt
	wheres := make([]string, len(clauses))
	allParams := []p.Param{}
	for index, clause := range clauses {
		neededParams := clause.ParamsRequired()
		params := make([]string, neededParams)
		for i := 0; i < neededParams; i++ {
			params[i] = fmt.Sprintf(`param%v`, paramNum)
			paramNum++
		}
		where, err := clause.ToSqlSnippet(params...)
		if err != nil {
			return nil, err
		}
		wheres[index] = where.Snippet
		allParams = append(allParams, where.Params...)
	}
	return &p.SqlPart{Value: strings.Join(wheres, ` AND `), Params: allParams, NextParam: paramNum}, nil
}
