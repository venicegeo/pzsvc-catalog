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

package catalog

// IDAM is an interface for providing authentication and authorization
type IDAM interface {
	authorize(token, role string) bool
	authenticate(authorization string) (bool, string)
}

var myIdam IDAM

// SetIDAM sets the IDAM for this application
func SetIDAM(idam IDAM) {
	myIdam = idam
}

// Authenticate returns true and a token if the authorization string is valid
func Authenticate(authorization string) (bool, string) {
	if myIdam == nil {
		return false, ""
	}
	return myIdam.authenticate(authorization)
}

// Authorize returns true if the token is authorized to perform the role provided
func Authorize(token, role string) bool {
	if myIdam == nil {
		return false
	}
	return myIdam.authorize(token, role)
}

// TestIDAM is an IDAM solely for testing purposes. Very permissive.
// TODO: Move to a test file once we have a real test solution to use
type TestIDAM struct {
}

func (idam TestIDAM) authorize(token, role string) bool {
	if role == "never" {
		return false
	}
	return true
}

func (idam TestIDAM) authenticate(authorization string) (bool, string) {
	if authorization == "never" {
		return false, ""
	}
	return true, "foo"
}
