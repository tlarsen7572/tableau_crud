package error_messaging

import "fmt"

func GenerateErrorMessage(message string, err error) string {
	return fmt.Sprintf("%v:\n%v", message, err.Error())
}
