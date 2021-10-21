package persistance_mssql

import (
	"errors"
	"fmt"
	"strings"
	p "tableau_crud/persistance"
)

type ParamListClause struct {
	ParamValues []interface{}
}

func (clause *ParamListClause) ToSqlSnippet(paramNames ...string) (*p.SqlSnippet, error) {
	if len(paramNames) != len(clause.ParamValues) {
		return nil, errors.New(fmt.Sprintf(`expecting %v paramNames in ToSqlSnippet of ParamListClause but got %v`, len(clause.ParamValues), len(paramNames)))
	}
	params := []p.Param{}
	for index, param := range paramNames {
		err := ValidateParam(param)
		if err != nil {
			return nil, err
		}
		params = append(params, p.Param{Name: param, Value: clause.ParamValues[index]})
	}
	snippet := `@` + strings.Join(paramNames, `,@`)
	return &p.SqlSnippet{
		Snippet: snippet,
		Params:  params,
	}, nil
}

func (clause *ParamListClause) ParamsRequired() int {
	return len(clause.ParamValues)
}
