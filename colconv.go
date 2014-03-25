package database

import (
	"strings"
)

func SnakeCaseConverter(col string) string {
	name := ""
	if l := len(col); l > 0 {
		chunks := strings.Split(col, "_")
		for i, v := range chunks {
			chunks[i] = strings.Title(v)
		}
		name = strings.Join(chunks, "")
	}
	return name
}
