// Copyright 2016, RadiantBlue Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pzsvc

import (
	"crypto/rand"
	"errors"
	"fmt"
	"runtime"
	"strconv"
)

// Error is a type designed for easy serialization to JSON
type Error struct {
	Message string `json:"error"`
}

func (err Error) Error() string {
	return err.Message
}

// traceCore is the core behind TraceStr, TraceErr, and ErrWithTrace.
// We'd have used TraceStr instead, but the way that runtime.Caller
// works means that they have to all be the same function depth away.
func traceCore(errStr string) string {
	_, file, line, ok := runtime.Caller(2)
	if ok == true {
		return `(` + file + `, ` + strconv.Itoa(line) + `): ` + errStr
	}
	return `(Trace: trace failed): ` + errStr
}

// TraceStr is a simple utility function that adds filename and line number
// to a string.  Primarily intended for error messages, though it is also
// useful in logging.
func TraceStr(errStr string) string {
	return traceCore(errStr)
}

// TraceErr is a simple utility function for adding a local filename and line number
// on to the beginning of an error message before passing it along.
func TraceErr(err error) error {
	if err != nil {
		return errors.New(traceCore(err.Error()))
	}
	return nil
}

// ErrWithTrace is a simple utility function for generating an error based on
// a string and while adding filename and line number.
func ErrWithTrace(errStr string) error {
	if errStr != "" {
		return errors.New(traceCore(errStr))
	}
	return nil
}

// SliceToCommaSep takes a string slice, and turns it into a comma-separated
// list of strings, suitable for JSON.
func SliceToCommaSep(inSlice []string) string {
	sliLen := len(inSlice)
	if sliLen == 0 {
		return ""
	}
	accum := inSlice[0]
	for i := 1; i < sliLen; i++ {
		accum = accum + "," + inSlice[i]
	}
	return accum
}

// PsuUUID makes a psuedo-UUID.  It may not achieve cryptographic levels of
// randomness, and it won't respond properly to standard ways of pulling data
// out of UUIDs, but it works just fine at generating effectively unique IDs
// for practical purposes.
func PsuUUID() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
