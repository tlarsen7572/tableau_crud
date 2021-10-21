package params_validators

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"github.com/zavla/dpapi"
	"unicode/utf16"
	"unicode/utf8"
)

type EncryptPasswordParams struct {
	Password string
}

func ValidateEncryptPasswordParams(params map[string]interface{}) (*EncryptPasswordParams, error) {
	password, ok := params[`password`]
	if !ok {
		return nil, errors.New(`missing 'password' parameter`)
	}
	passwordStr, ok := InterfaceToString(password)
	if !ok {
		return nil, errors.New(`'password' is not a string`)
	}
	return &EncryptPasswordParams{Password: passwordStr}, nil
}

func Encrypt(password string) (string, error) {
	utf16encoded := dpapi.ConvertToUTF16LittleEndianBytes(password)
	encrypted, err := dpapi.Encrypt(utf16encoded)
	if err != nil {
		return ``, err
	}
	base64Encoded := base64.StdEncoding.EncodeToString(encrypted)
	return base64Encoded, nil
}

func Decrypt(base64Password string) (string, error) {
	passwordBytes, err := base64.StdEncoding.DecodeString(base64Password)
	if err != nil {
		return ``, err
	}
	decrypted, err := dpapi.Decrypt(passwordBytes)
	if err != nil {
		return ``, err
	}
	decoded := convertFromUTF16LittleEndianBytes(decrypted)
	return decoded, nil
}

func convertFromUTF16LittleEndianBytes(b []byte) string {
	utf := make([]uint16, (len(b)+(2-1))/2)
	for i := 0; i+(2-1) < len(b); i += 2 {
		utf[i/2] = binary.LittleEndian.Uint16(b[i:])
	}
	if len(b)/2 < len(utf) {
		utf[len(utf)-1] = utf8.RuneError
	}
	return string(utf16.Decode(utf))
}
