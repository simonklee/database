package database

import "fmt"

// Returns "?"
func BindVar(i int) string {
	return "?"
}

func QuoteField(f string) string {
	return "`" + f + "`"
}

func FullMatch(v interface{}) string {
	return fmt.Sprintf("%%%s%%", v)
}

func SuffixMatch(v interface{}) string {
	return fmt.Sprintf("%s%%", v)
}

func PreMatch(v interface{}) string {
	return fmt.Sprintf("%%%s", v)
}
