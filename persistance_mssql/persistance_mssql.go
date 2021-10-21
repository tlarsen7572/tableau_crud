package persistance_mssql

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	p "tableau_crud/persistance"
)

type MssqlPersistor struct {
	Server   string
	Port     string
	Username string
	Password string
	Database string
	Schema   string
	Table    string
}

func (persistor *MssqlPersistor) Insert(values map[string]interface{}) (int, error) {
	if len(values) == 0 {
		return 0, errors.New(`at least 1 field and value must be provided`)
	}
	var fields []string
	var paramValues []interface{}
	var paramNames []string
	paramNum := 1
	for key, value := range values {
		paramName := fmt.Sprintf(`param%v`, paramNum)
		fields = append(fields, key)
		paramValues = append(paramValues, value)
		paramNames = append(paramNames, paramName)
		paramNum++
	}
	fieldsClause := FieldListClause{Fields: fields}
	fieldsSql, _ := fieldsClause.ToSqlSnippet()
	valuesClause := ParamListClause{ParamValues: paramValues}
	valuesSql, _ := valuesClause.ToSqlSnippet(paramNames...)
	table := persistor.generateTableString()
	stmnt := fmt.Sprintf(`INSERT INTO %v (%v) VALUES (%v)`, table, fieldsSql.Snippet, valuesSql.Snippet)
	return persistor.exec(stmnt, valuesSql.Params)
}

func (persistor *MssqlPersistor) Update(where []p.SqlSnippetGenerator, updates []p.SqlSnippetGenerator) (int, error) {
	if len(where) == 0 {
		return 0, errors.New(`at least 1 where clause must be provided`)
	}
	if len(updates) == 0 {
		return 0, errors.New(`at least 1 update clause must be provided`)
	}
	whereClause, err := GenerateCombinedWhereClause(where, 0)
	if err != nil {
		return 0, err
	}
	updateClause, err := GenerateCombinedUpdateClause(updates, whereClause.NextParam)
	if err != nil {
		return 0, err
	}
	table := persistor.generateTableString()
	stmnt := fmt.Sprintf(`UPDATE %v SET %v WHERE %v`, table, updateClause.Value, whereClause.Value)
	params := append(whereClause.Params, updateClause.Params...)
	return persistor.exec(stmnt, params)
}

func (persistor *MssqlPersistor) Delete(where []p.SqlSnippetGenerator) (int, error) {
	if len(where) == 0 {
		return 0, errors.New(`at least 1 where clause must be provided`)
	}
	whereClause, err := GenerateCombinedWhereClause(where, 1)
	if err != nil {
		return 0, err
	}
	table := persistor.generateTableString()
	stmnt := fmt.Sprintf(`DELETE FROM %v WHERE %v`, table, whereClause.Value)
	return persistor.exec(stmnt, whereClause.Params)
}

func (persistor *MssqlPersistor) Read(fields []string, where []p.SqlSnippetGenerator, orderBy []string, pageSize int, page int) (*p.QueryResult, error) {
	if len(fields) == 0 {
		return nil, errors.New(`at least 1 field must be provided`)
	}
	if len(orderBy) == 0 {
		return nil, errors.New(`at least 1 Order By field must be provided`)
	}
	selectFields := QuoteIdentifiers(fields)
	table := persistor.generateTableString()
	whereClause, err := GenerateCombinedWhereClause(where, 1)
	if err != nil {
		return nil, err
	}
	orderByFields := QuoteIdentifiers(orderBy)
	offset := (page - 1) * pageSize
	var stmnt string
	if len(where) > 0 {
		stmnt = fmt.Sprintf(`SELECT %v FROM %v WHERE %v ORDER BY %v OFFSET %v ROWS FETCH NEXT %v ROWS ONLY; SELECT count(*) FROM %v WHERE %v`, selectFields, table, whereClause.Value, orderByFields, offset, pageSize, table, whereClause.Value)
	} else {
		stmnt = fmt.Sprintf(`SELECT %v FROM %v ORDER BY %v OFFSET %v ROWS FETCH NEXT %v ROWS ONLY; SELECT count(*) FROM %v`, selectFields, table, orderByFields, offset, pageSize, table)
	}

	return persistor.query(stmnt, whereClause.Params)
}

func (persistor *MssqlPersistor) TestConnection() (*p.QueryResult, error) {
	table := persistor.generateTableString()
	stmnt := fmt.Sprintf(`SELECT TOP 0 * FROM %v`, table)
	return persistor.query(stmnt, []p.Param{})
}

func (persistor *MssqlPersistor) query(stmnt string, params []p.Param) (*p.QueryResult, error) {
	db, err := sql.Open(`sqlserver`, persistor.generateUrl())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	prepared, err := db.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	sqlParams := generateSqlParams(params)
	rows, err := prepared.Query(sqlParams...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	rowValues := make([]interface{}, len(colNames))
	rowPointers := make([]interface{}, len(colNames))
	for index := range colNames {
		rowPointers[index] = &rowValues[index]
	}
	queryRows := make([][]interface{}, len(colNames))
	queryResult := &p.QueryResult{
		ColumnNames:   colNames,
		RowCount:      0,
		Data:          queryRows,
		TotalRowCount: 0,
	}

	rowCount := 0
	for rows.Next() {
		err := rows.Scan(rowPointers...)
		if err != nil {
			return nil, err
		}
		for index := range colNames {
			colType := colTypes[index].DatabaseTypeName()
			if colType == `DECIMAL` || colType == `NUMERIC` {
				if rowValues[index] == nil {
					queryResult.Data[index] = append(queryResult.Data[index], nil)
					continue
				}
				value, err := strconv.ParseFloat(string(rowValues[index].([]uint8)), 64)
				if err != nil {
					return nil, err
				}
				queryResult.Data[index] = append(queryResult.Data[index], value)
				continue
			}
			queryResult.Data[index] = append(queryResult.Data[index], rowValues[index])
		}
		rowCount++
	}
	queryResult.RowCount = rowCount

	if rows.NextResultSet() {
		var totalRowCount int
		rows.Next()
		err := rows.Scan(&totalRowCount)
		if err != nil {
			return nil, err
		}
		queryResult.TotalRowCount = totalRowCount
	}
	return queryResult, nil
}

func (persistor *MssqlPersistor) exec(stmnt string, params []p.Param) (int, error) {
	sqlUrl := persistor.generateUrl()
	db, err := sql.Open(`sqlserver`, sqlUrl)
	if err != nil {
		return 0, err
	}
	prepared, err := db.Prepare(stmnt)
	if err != nil {
		return 0, err
	}
	sqlParams := generateSqlParams(params)
	result, err := prepared.Exec(sqlParams...)
	if err != nil {
		return 0, nil
	}
	affectedRows, err := result.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return int(affectedRows), nil
}

func (persistor *MssqlPersistor) generateUrl() string {
	var urlBuilder = &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(persistor.Username, persistor.Password),
		Host:   fmt.Sprintf(`%v:%v`, persistor.Server, persistor.Port),
	}
	return urlBuilder.String()
}

func (persistor *MssqlPersistor) generateTableString() string {
	quotedDb := QuoteIdentifier(persistor.Database)
	quotedSchema := QuoteIdentifier(persistor.Schema)
	quotedTable := QuoteIdentifier(persistor.Table)
	return fmt.Sprintf(`%v.%v.%v`, quotedDb, quotedSchema, quotedTable)
}

func generateSqlParams(params []p.Param) []interface{} {
	sqlParams := make([]interface{}, len(params))
	for index, param := range params {
		sqlParams[index] = sql.Named(param.Name, param.Value)
	}
	return sqlParams
}
