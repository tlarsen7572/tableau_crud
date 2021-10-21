package params_validators

import (
	"errors"
	"tableau_crud/persistance"
)

func ValidateDeleteParams(params map[string]interface{}) ([]persistance.SqlSnippetGenerator, error) {
	where, ok := params[`where`]
	if !ok {
		return nil, errors.New(`missing 'where' parameter`)
	}
	return validateWhereClauses(where)
}
