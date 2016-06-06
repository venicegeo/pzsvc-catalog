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

import (
	"net/http"
	"strings"
)

const dgURLString = "https://evwhs.digitalglobe.com/myDigitalGlobe/sif/featuresearch"

// DoDGRequest performs the request
// URL may be relative or absolute based on baseURLString
func DoDGRequest(body, auth string) (*http.Response, error) {
	var (
		request *http.Request
		err     error
	)
	if request, err = http.NewRequest("POST", dgURLString, strings.NewReader(body)); err != nil {
		return nil, err
	}

	request.Header.Set("Authorization", auth)
	request.Header.Set("Content-Type", "application/json")
	return getClient().Do(request)
}
