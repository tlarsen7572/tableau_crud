package persistance_mssql

import (
	"errors"
	"fmt"
	"strings"
	p "tableau_crud/persistance"
)

type UpdateClause struct {
	Identifier string
	NewValue   interface{}
}

func (clause *UpdateClause) ToSqlSnippet(paramNames ...string) (*p.SqlSnippet, error) {
	if len(paramNames) != 1 {
		return nil, errors.New(fmt.Sprintf(`expecting 1 paramName in ToSqlSnippet of UpdateClause but got %v`, len(paramNames)))
	}
	err := ValidateParam(paramNames[0])
	if err != nil {
		return nil, err
	}
	param := paramNames[0]
	params := []p.Param{
		{Name: param, Value: clause.NewValue},
	}
	quoted := QuoteIdentifier(clause.Identifier)
	updateClause := fmt.Sprintf(`%v=@%v`, quoted, param)

	return &p.SqlSnippet{
		Snippet: updateClause,
		Params:  params,
	}, nil
}

func (clause *UpdateClause) ParamsRequired() int {
	return 1
}

func GenerateCombinedUpdateClause(clauses []p.SqlSnippetGenerator, startParamAt int) (*p.SqlPart, error) {
	paramNum := startParamAt
	updates := make([]string, len(clauses))
	allParams := []p.Param{}
	for index, clause := range clauses {
		neededParams := clause.ParamsRequired()
		params := make([]string, neededParams)
		for i := 0; i < neededParams; i++ {
			params[i] = fmt.Sprintf(`param%v`, paramNum)
			paramNum++
		}
		update, err := clause.ToSqlSnippet(params...)
		if err != nil {
			return nil, err
		}
		updates[index] = update.Snippet
		allParams = append(allParams, update.Params...)
	}
	return &p.SqlPart{Value: strings.Join(updates, `,`), Params: allParams, NextParam: paramNum}, nil
}
