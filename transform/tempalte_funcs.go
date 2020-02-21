package mast

import (
	"strings"
	"text/template"
)

var templateFuncMap = template.FuncMap{
	"sub1": func(a int) int {
		return a - 1
	},
	"levelSpaces": func(n int) string {
		return strings.Repeat("  ", n)
	},
}
