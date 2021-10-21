package params_validators

import (
	"errors"
	p "tableau_crud/persistance"
)

type ReadParams struct {
	Fields   []string
	Where    []p.SqlSnippetGenerator
	OrderBy  []string
	PageSize int
	Page     int
}

func ValidateReadParams(params map[string]interface{}) (*ReadParams, error) {
	fields, ok := params[`fields`]
	if !ok {
		return nil, errors.New(`missing 'fields' parameter`)
	}
	fieldsList, ok := InterfaceToListOfStrings(fields)
	if !ok {
		return nil, errors.New(`'fields' is not a list of strings`)
	}
	where, ok := params[`where`]
	if !ok {
		return nil, errors.New(`missing 'where' parameter`)
	}
	whereClauses, err := validateWhereClauses(where)
	if err != nil {
		return nil, err
	}
	orderBy, ok := params[`orderBy`]
	if !ok {
		return nil, errors.New(`missing 'orderBy' parameter`)
	}
	orderByList, ok := InterfaceToListOfStrings(orderBy)
	if !ok {
		return nil, errors.New(`'orderBy' is not a list of strings`)
	}
	pageSize, ok := params[`pageSize`]
	if !ok {
		return nil, errors.New(`missing 'pageSize' parameter`)
	}
	pageSizeInt, ok := InterfaceToInt(pageSize)
	if !ok {
		return nil, errors.New(`'pageSize' is not an int`)
	}
	page, ok := params[`page`]
	if !ok {
		return nil, errors.New(`missing 'page' parameter`)
	}
	pageInt, ok := InterfaceToInt(page)
	if !ok {
		return nil, errors.New(`'page' is not an int`)
	}
	return &ReadParams{
		Fields:   fieldsList,
		Where:    whereClauses,
		OrderBy:  orderByList,
		PageSize: pageSizeInt,
		Page:     pageInt,
	}, nil
}
