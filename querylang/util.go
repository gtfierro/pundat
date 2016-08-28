package querylang

import (
	"strings"
)

// remove trailing commas, replace all / with .
func cleantagstring(inp string) string {
	tmp := strings.TrimSuffix(inp, ",")
	tmp = strings.Replace(tmp, "/", ".", -1)
	return tmp
}
