package utils

import (
	"bytes"
	"io"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

func Unpack(s []string, vars ...*string) {
	for i := range vars {
		*vars[i] = s[i]
	}
}

func PathAdd(paths ...string) string {
	var path string = "."
	for _, point := range paths {
		path += "/" + point
	}
	return path
}

func DecodeCP866(s string) string {
	reader := transform.NewReader(bytes.NewReader([]byte(s)), charmap.CodePage866.NewDecoder())
	res, _ := io.ReadAll(reader)
	return string(res)
}

func StringsToAny(strings []string) []any {
	res := make([]any, len(strings))
	for i, str := range strings {
		res[i] = str
	}
	return res[:len(res)-1]
}
