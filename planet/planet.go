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

package planet

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
	"github.com/venicegeo/pzsvc-lib"
)

// TODO: pull from environment
const baseURLString = "https://api.planet.com/"

type request struct {
	ItemTypes []string `json:"item_types"`
	Filter    filter   `json:"filter"`
}

type filter struct {
	Type   string        `json:"type"`
	Config []interface{} `json:"config"`
}

type geometryFilter struct {
	Type      string      `json:"type"`
	FieldName string      `json:"field_name"`
	Config    interface{} `json:"config"`
}

type doRequestInput struct {
	method      string
	inputURL    string // URL may be relative or absolute based on baseURLString
	body        []byte
	contentType string
}

type doRequestContext struct {
	planetKey string
}

// doRequest performs the request
func doRequest(input doRequestInput, context doRequestContext) (*http.Response, error) {
	var (
		request   *http.Request
		parsedURL *url.URL
		inputURL  string
		err       error
	)
	if !strings.Contains(input.inputURL, baseURLString) {
		baseURL, _ := url.Parse(baseURLString)
		parsedRelativeURL, _ := url.Parse(input.inputURL)
		resolvedURL := baseURL.ResolveReference(parsedRelativeURL)

		if parsedURL, err = url.Parse(resolvedURL.String()); err != nil {
			return nil, err
		}
		inputURL = parsedURL.String()
	}
	if request, err = http.NewRequest(input.method, inputURL, bytes.NewBuffer(input.body)); err != nil {
		return nil, err
	}
	if input.contentType != "" {
		request.Header.Set("Content-Type", input.contentType)
	}

	request.Header.Set("Authorization", "Basic "+getPlanetAuth(context.planetKey))
	fmt.Printf("Request: %#v", request)
	return pzsvc.HTTPClient().Do(request)
}

// unmarshalResponse parses the response and returns a feature collection
func unmarshalResponse(response *http.Response) (*geojson.FeatureCollection, error) {
	var (
		// unmarshal Response
		err  error
		body []byte
		gj   interface{}
		fc   *geojson.FeatureCollection
	)
	defer response.Body.Close()
	if body, err = ioutil.ReadAll(response.Body); err != nil {
		return fc, err
	}

	// Check for HTTP errors
	if response.StatusCode < 200 || response.StatusCode > 299 {
		message := fmt.Sprintf("%v returned %v", response.Request.URL.String(), string(body))
		return fc, &pzsvc.HTTPError{Message: message, Status: response.StatusCode}
	}

	// if err = json.Unmarshal(body, &unmarshal); err != nil {
	// 	return fc, err
	// }
	if gj, err = geojson.Parse(body); err != nil {
		return fc, err
	}
	fc = gj.(*geojson.FeatureCollection)
	return fc, err
}

func getPlanetAuth(key string) string {
	var result string
	if key == "" {
		key = os.Getenv("PL_API_KEY")
	}
	result = base64.StdEncoding.EncodeToString([]byte(key + ":"))
	return result
}

// Response represents the response JSON structure.
type Response struct {
	Count string `json:"auth"`
	Links Links  `json:"links"`
}

// Links represents the links JSON structure.
type Links struct {
	Self  string `json:"self"`
	Prev  string `json:"prev"`
	Next  string `json:"next"`
	First string `json:"first"`
}

// GetScenes returns a string containing the scenes requested
func GetScenes(inputFeature *geojson.Feature, options catalog.SearchOptions) (string, error) {
	var (
		err      error
		response *http.Response
		body     []byte
		req      request
		fc       *geojson.FeatureCollection
		fci      interface{}
		// polygon  *geojson.Polygon
		// pi       interface{}
	)

	req.ItemTypes = append(req.ItemTypes, "REOrthoTile")
	req.Filter.Type = "AndFilter"
	req.Filter.Config = make([]interface{}, 0)
	if inputFeature != nil {
		req.Filter.Config = append(req.Filter.Config, geometryFilter{Type: "GeometryFilter", FieldName: "geometry", Config: inputFeature.Geometry})
	}
	if body, err = json.Marshal(req); err != nil {
		return "", err
	}
	if response, err = doRequest(doRequestInput{method: "POST", inputURL: "data/v1/quick-search", body: body, contentType: "application/json"}, doRequestContext{planetKey: options.PlanetKey}); err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, _ = ioutil.ReadAll(response.Body)
	if fci, err = geojson.Parse(body); err != nil {
		return "", err
	}
	fc = fci.(*geojson.FeatureCollection)
	body, err = geojson.Write(fc)
	fc = transformFeatureCollection(fc)
	body, err = geojson.Write(fc)
	fmt.Print(string(body))
	return string(body), err
}

func transformFeatureCollection(fc *geojson.FeatureCollection) *geojson.FeatureCollection {
	var (
		result *geojson.FeatureCollection
	)
	fmt.Print("1\n")
	features := make([]*geojson.Feature, len(fc.Features))
	for inx, curr := range fc.Features {
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(float64)
		id := curr.IDStr()
		// properties["path"] = url + "index.html"
		// properties["thumb_large"] = url + id + "_thumb_large.jpg"
		// properties["thumb_small"] = url + id + "_thumb_small.jpg"
		properties["resolution"] = curr.Properties["gsd"].(float64)
		adString := curr.Properties["acquired"].(string)
		properties["acquiredDate"] = adString
		properties["fileFormat"] = "geotiff"
		properties["sensorName"] = curr.Properties["satellite_id"].(string)
		feature := geojson.NewFeature(curr.Geometry, curr.Properties["provider"].(string)+id, properties)
		feature.Bbox = curr.ForceBbox()
		features[inx] = feature
	}
	result = geojson.NewFeatureCollection(features)
	return result
}
