package persistance_mssql

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

func QuoteIdentifiers(identifiers []string) string {
	quoted := make([]string, len(identifiers))
	for index, identifier := range identifiers {
		quoted[index] = QuoteIdentifier(identifier)
	}
	return strings.Join(quoted, `,`)
}

func QuoteIdentifier(identifier string) string {
	quoted := strings.ReplaceAll(identifier, `]`, `]]`)
	return fmt.Sprintf(`[%v]`, quoted)
}

func ValidateParam(param string) error {
	regex := regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`)
	if !regex.MatchString(param) {
		return errors.New(fmt.Sprintf(`param '%v' does not start with a letter or does not contain only numbers and letters`, param))
	}
	return nil
}
