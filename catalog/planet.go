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
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"github.com/venicegeo/geojson-go/geojson"
)

const baseURLString = "https://api.planet.com/"

var planetClient *http.Client

// DoPlanetRequest performs the request
func DoPlanetRequest(method, relativeURL, key string) (*http.Response, error) {
	baseURL, _ := url.Parse(baseURLString)
	parsedRelativeURL, _ := url.Parse(relativeURL)
	resolvedURL := baseURL.ResolveReference(parsedRelativeURL)
	parsedURL, _ := url.Parse(resolvedURL.String())
	request, _ := http.NewRequest(method, parsedURL.String(), nil)

	request.Header.Set("Authorization", "Basic "+getPlanetAuth(key))

	if planetClient == nil {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}

		planetClient = &http.Client{Transport: transport}
	}
	return planetClient.Do(request)
}

// UnmarshalPlanetResponse parses the response and returns a Planet Labs response object
func UnmarshalPlanetResponse(response *http.Response) (PlanetResponse, *geojson.FeatureCollection, error) {
	var (
		unmarshal PlanetResponse
		err       error
		body      []byte
		gj        interface{}
		fc        *geojson.FeatureCollection
	)
	defer response.Body.Close()
	if body, err = ioutil.ReadAll(response.Body); err != nil {
		return unmarshal, fc, err
	}

	// Check for HTTP errors
	if response.StatusCode < 200 || response.StatusCode > 299 {
		message := fmt.Sprintf("%v returned %v", response.Request.URL.String(), string(body))
		return unmarshal, fc, &HTTPError{Message: message, Status: response.StatusCode}
	}

	if err = json.Unmarshal(body, &unmarshal); err != nil {
		return unmarshal, fc, err
	}
	if gj, err = geojson.Parse(body); err != nil {
		return unmarshal, fc, err
	}
	fc = gj.(*geojson.FeatureCollection)
	return unmarshal, fc, err
}

func getPlanetAuth(key string) string {
	var result string
	if key == "" {
		key = os.Getenv("PL_API_KEY")
	}
	result = base64.StdEncoding.EncodeToString([]byte(key + ":"))
	return result
}

// PlanetResponse represents the response JSON structure.
type PlanetResponse struct {
	Count string      `json:"auth"`
	Links PlanetLinks `json:"links"`
}

// PlanetLinks represents the links JSON structure.
type PlanetLinks struct {
	Self  string `json:"self"`
	Prev  string `json:"prev"`
	Next  string `json:"next"`
	First string `json:"first"`
}
