package mast

import (
	"fmt"
	"net/http"
	"time"
)

// ConcatErrMsg ...
func ConcatErrMsg(msg string, err error) string {
	return fmt.Sprintf("%v -- %s : %s", time.Now(), msg, err.Error())
}

// HandleAPIError ...
func HandleAPIError(w http.ResponseWriter, status int, msg string, err error) {
	http.Error(w, ConcatErrMsg(msg, err), status)
	return
}
