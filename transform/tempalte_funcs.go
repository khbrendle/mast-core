package mast

import "text/template"

var templateFuncMap = template.FuncMap{
	"sub1": func(a int) int {
		return a - 1
	},
}
