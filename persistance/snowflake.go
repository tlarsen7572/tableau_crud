package persistance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/snowflakedb/gosnowflake"
	"log"
	"strconv"
	"strings"
	"time"
)

func NewPersistor(connStr string) (Persistor, error) {
	db, err := sql.Open(`snowflake`, connStr)
	if err != nil {
		return nil, err
	}
	persistor := &SnowflakePersistor{db: db}
	go persistor.keepAlive()
	return persistor, nil
}

type SnowflakePersistor struct {
	db *sql.DB
}

func (s *SnowflakePersistor) Insert(table string, values map[string]interface{}) (int64, error) {
	table = QuoteIdentifier(table)
	fields := make([]string, 0, len(values))
	params := make([]interface{}, 0, len(values))
	for key, value := range values {
		fields = append(fields, key)
		params = append(params, value)
	}
	clause := FieldListClause{
		Fields: fields,
	}
	snippet := clause.ToSqlSnippet()
	stmt := fmt.Sprintf(`INSERT INTO %v (%v) VALUES (%v)`, table, snippet.Snippet, `?`+strings.Repeat(`,?`, len(params)-1))
	return s.exec(stmt, params)
}

func (s *SnowflakePersistor) Update(table string, where []SqlSnippetGenerator, updates []SqlSnippetGenerator) (int64, error) {
	table = QuoteIdentifier(table)
	whereClause := GenerateCombinedWhereClause(where)
	updateClause := GenerateCombinedUpdateClause(updates)
	stmnt := fmt.Sprintf(`UPDATE %v SET %v WHERE %v`, table, updateClause.Value, whereClause.Value)
	params := append(updateClause.Params, whereClause.Params...)
	return s.exec(stmnt, params)
}

func (s *SnowflakePersistor) Delete(table string, where []SqlSnippetGenerator) (int64, error) {
	table = QuoteIdentifier(table)
	whereClause := GenerateCombinedWhereClause(where)
	stmnt := fmt.Sprintf(`DELETE FROM %v WHERE %v`, table, whereClause.Value)
	return s.exec(stmnt, whereClause.Params)
}

func (s *SnowflakePersistor) Read(table string, fields []string, where []SqlSnippetGenerator, orderBy []string, pageSize int, page int) (*QueryResult, error) {
	if len(fields) == 0 {
		return nil, errors.New(`at least 1 field must be provided`)
	}
	if len(orderBy) == 0 {
		return nil, errors.New(`at least 1 Order By field must be provided`)
	}
	selectFields := QuoteIdentifiers(fields)
	table = QuoteIdentifier(table)
	whereClause := GenerateCombinedWhereClause(where)
	orderByFields := QuoteIdentifiers(orderBy)
	offset := (page - 1) * pageSize
	var stmnt string
	if len(where) > 0 {
		stmnt = fmt.Sprintf(`SELECT %v FROM %v WHERE %v ORDER BY %v OFFSET %v ROWS FETCH NEXT %v ROWS ONLY; SELECT count(*) FROM %v WHERE %v`, selectFields, table, whereClause.Value, orderByFields, offset, pageSize, table, whereClause.Value)
	} else {
		stmnt = fmt.Sprintf(`SELECT %v FROM %v ORDER BY %v OFFSET %v ROWS FETCH NEXT %v ROWS ONLY; SELECT count(*) FROM %v`, selectFields, table, orderByFields, offset, pageSize, table)
	}

	return s.query(stmnt, 2, whereClause.Params)
}

func (s *SnowflakePersistor) TestConnection(table string) (*QueryResult, error) {
	table = QuoteIdentifier(table)
	stmnt := fmt.Sprintf(`SELECT TOP 0 * FROM %v`, table)
	return s.query(stmnt, 1, []interface{}{})
}

func (s *SnowflakePersistor) keepAlive() {
	for {
		rows, err := s.db.Query(`SELECT 1`)
		if err != nil {
			log.Printf(err.Error())
			continue
		}
		_ = rows.Close()
		time.Sleep(time.Hour)
	}
}

func (s *SnowflakePersistor) exec(stmt string, params []interface{}) (int64, error) {
	prep, err := s.db.Prepare(stmt)
	if err != nil {
		return 0, err
	}
	result, err := prep.Exec(params...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *SnowflakePersistor) query(stmnt string, totalStatements int, params []interface{}) (*QueryResult, error) {
	prepared, err := s.db.Prepare(stmnt)
	if err != nil {
		return nil, err
	}

	multiStmnt, err := gosnowflake.WithMultiStatement(context.Background(), totalStatements)

	rows, err := prepared.QueryContext(multiStmnt, params...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

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
	queryResult := &QueryResult{
		ColumnNames:   colNames,
		RowCount:      0,
		Data:          queryRows,
		TotalRowCount: 0,
	}

	rowCount := 0
	for rows.Next() {
		err = rows.Scan(rowPointers...)
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
		err = rows.Scan(&totalRowCount)
		if err != nil {
			return nil, err
		}
		queryResult.TotalRowCount = totalRowCount
	}
	return queryResult, nil
}
