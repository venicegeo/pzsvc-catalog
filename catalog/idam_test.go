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

import "testing"

func TestIDAMPlanet(t *testing.T) {
	var (
		idam          TestIDAM
		authenticated bool
		token         string
	)

	SetIDAM(idam)

	if authenticated, token = Authenticate("never"); authenticated {
		t.Error("Expected not to be authenticated")
	}
	if authenticated, token = Authenticate("foo"); !authenticated {
		t.Error("Expected to be authenticated")
	}
	if !Authorize(token, "bar") {
		t.Error("Expected to be authorized")
	}

	if Authorize(token, "never") {
		t.Error("Expected not to be authorized")
	}
}
