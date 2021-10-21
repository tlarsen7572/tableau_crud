package params_validators

import (
	"errors"
	"fmt"
	p "tableau_crud/persistance"
	mssql "tableau_crud/persistance_mssql"
)

type UpdateParams struct {
	Where   []p.SqlSnippetGenerator
	Updates []p.SqlSnippetGenerator
}

func ValidateUpdateParams(params map[string]interface{}) (*UpdateParams, error) {
	where, ok := params[`where`]
	if !ok {
		return nil, errors.New(`missing 'where' parameter`)
	}
	whereClauses, err := validateWhereClauses(where)
	if err != nil {
		return nil, err
	}
	update, ok := params[`updates`]
	if !ok {
		return nil, errors.New(`missing 'updates' parameter`)
	}
	updateClauses, err := validateUpdateClauses(update)
	if err != nil {
		return nil, err
	}
	return &UpdateParams{
		Where:   whereClauses,
		Updates: updateClauses,
	}, nil
}

func validateUpdateClauses(update interface{}) ([]p.SqlSnippetGenerator, error) {
	switch typedUpdate := update.(type) {
	default:
		return nil, errors.New(fmt.Sprintf(`expected update to be a map[string]interface{} but got %T`, typedUpdate))
	case map[string]interface{}:
		updateClauses := []p.SqlSnippetGenerator{}
		for key, value := range typedUpdate {
			updateClauses = append(updateClauses, &mssql.UpdateClause{
				Identifier: key,
				NewValue:   value,
			})
		}
		return updateClauses, nil
	}
}
