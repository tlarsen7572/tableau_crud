package params_validators

import (
	"errors"
	"fmt"
)

func ValidateInsertParams(params map[string]interface{}) (map[string]interface{}, error) {
	values, ok := params[`values`]
	if !ok {
		return nil, errors.New(`missing 'values' parameter`)
	}
	switch t := values.(type) {
	default:
		return nil, errors.New(fmt.Sprintf(`expected values param to be a map[string]interface{} but got %T`, t))
	case map[string]interface{}:
		return t, nil
	}
}
