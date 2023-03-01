package params_validators

import (
	"errors"
	"fmt"
	p "tableau_crud/persistance"
)

func ValidateUpdateClauses(update map[string]interface{}) ([]p.SqlSnippetGenerator, error) {
	updateClauses := make([]p.SqlSnippetGenerator, 0)
	for key, value := range update {
		updateClauses = append(updateClauses, &p.UpdateClause{
			Identifier: key,
			NewValue:   value,
		})
	}
	return updateClauses, nil
}

func ValidateWhereClauses(where []interface{}) ([]p.SqlSnippetGenerator, error) {
	whereGenerators := make([]p.SqlSnippetGenerator, 0)
	for index, typedWhereEntry := range where {
		switch whereClause := typedWhereEntry.(type) {
		default:
			return nil, errors.New(fmt.Sprintf(`expected entry %v to be a map[string]interface{} but got %T`, index+1, whereClause))
		case map[string]interface{}:
			field, ok := whereClause[`field`]
			if !ok {
				return nil, errors.New(fmt.Sprintf(`missing 'field' in where clause %v`, index+1))
			}
			fieldStr, ok := InterfaceToString(field)
			if !ok {
				return nil, errors.New(fmt.Sprintf(`'field' is not a string in where clause %v`, index+1))
			}
			operator, ok := whereClause[`operator`]
			if !ok {
				return nil, errors.New(fmt.Sprintf(`missing 'operator' in where clause %v`, index+1))
			}
			operatorStr, ok := InterfaceToString(operator)
			if !ok {
				return nil, errors.New(fmt.Sprintf(`'operator' is not a string in where clause %v`, index+1))
			}
			values, ok := whereClause[`values`]
			if !ok {
				return nil, errors.New(fmt.Sprintf(`missing 'values' operator in where clause %v`, index+1))
			}
			valuesList, ok := InterfaceToList(values)
			if !ok {
				return nil, errors.New(fmt.Sprintf(`'values' is not a []interface{} in where clause %v`, index+1))
			}

			if operatorStr == `equals` {
				if len(valuesList) != 1 {
					return nil, errors.New(fmt.Sprintf(`where clause %v is an equals operator but does not have 1 value`, index+1))
				}
				whereGenerators = append(whereGenerators, &p.EqualClause{
					Identifier: fieldStr,
					Value:      valuesList[0],
				})
				continue
			}
			if operator == `in` {
				exclude, excludeOk := whereClause[`exclude`]
				excludeBool := false
				if excludeOk {
					excludeBool, ok = InterfaceToBool(exclude)
					if !ok {
						return nil, errors.New(fmt.Sprintf(`'exclude' is not a bool in clause %v`, index+1))
					}
				}
				whereGenerators = append(whereGenerators, &p.InClause{
					Identifier: fieldStr,
					Exclude:    excludeBool,
					Values:     valuesList,
				})
				continue
			}
			if operator == `range` {
				includeNulls, includeNullsOk := whereClause[`includeNulls`]
				includeNullsBool := false
				if includeNullsOk {
					includeNullsBool, ok = InterfaceToBool(includeNulls)
					if !ok {
						return nil, errors.New(fmt.Sprintf(`'includeNulls' is not a bool in clause %v`, index+1))
					}
				}
				if len(valuesList) != 2 {
					return nil, errors.New(fmt.Sprintf(`where clause %v is a range operator but does not have 2 values`, index+1))
				}
				whereGenerators = append(whereGenerators, &p.RangeClause{
					Identifier:   fieldStr,
					MinValue:     valuesList[0],
					MaxValue:     valuesList[1],
					IncludeNulls: includeNullsBool,
				})
				continue
			}
			return nil, errors.New(fmt.Sprintf(`where clause %v is not a valid operator.  Should be 'equals', 'in', or 'range'`, index+1))
		}
	}
	return whereGenerators, nil
}

func InterfaceToList(value interface{}) ([]interface{}, bool) {
	switch t := value.(type) {
	default:
		return nil, false
	case []interface{}:
		return t, true
	}
}

func InterfaceToString(value interface{}) (string, bool) {
	switch t := value.(type) {
	default:
		return ``, false
	case string:
		return t, true
	}
}

func InterfaceToBool(value interface{}) (bool, bool) {
	switch t := value.(type) {
	default:
		return false, false
	case bool:
		return t, true
	}
}
