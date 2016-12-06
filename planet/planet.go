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
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/tides"
	"github.com/venicegeo/pzsvc-lib"
)

// TODO: pull from environment
const baseURLString = "https://api.planet.com/"

// SearchOptions are the search options for a quick-search request
type SearchOptions struct {
	Tides     bool
	TidesURL  string
	PlanetKey string
}
type request struct {
	ItemTypes []string `json:"item_types"`
	Filter    filter   `json:"filter"`
}

type filter struct {
	Type   string        `json:"type"`
	Config []interface{} `json:"config"`
}

type objectFilter struct {
	Type      string      `json:"type"`
	FieldName string      `json:"field_name"`
	Config    interface{} `json:"config"`
}

type dateConfig struct {
	GTE string `json:"gte,omitempty"`
	LTE string `json:"lte,omitempty"`
	GT  string `json:"gt,omitempty"`
	LT  string `json:"lt,omitempty"`
}

type rangeConfig struct {
	GTE float64 `json:"gte,omitempty"`
	LTE float64 `json:"lte,omitempty"`
	GT  float64 `json:"gt,omitempty"`
	LT  float64 `json:"lt,omitempty"`
}

type doRequestInput struct {
	method      string
	inputURL    string // URL may be relative or absolute based on baseURLString
	body        []byte
	contentType string
}

// RequestContext is the context for Planet Labs requests
type RequestContext struct {
	PlanetKey string
}

// doRequest performs the request
func doRequest(input doRequestInput, context RequestContext) (*http.Response, error) {
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

	request.Header.Set("Authorization", "Basic "+getPlanetAuth(context.PlanetKey))
	return pzsvc.HTTPClient().Do(request)
}

func getPlanetAuth(key string) string {
	var result string
	if key == "" {
		key = os.Getenv("PL_API_KEY")
	}
	result = base64.StdEncoding.EncodeToString([]byte(key + ":"))
	return result
}

// Assets represents the assets available for a scene
type Assets struct {
	Analytic    Asset `json:"analytic"`
	AnalyticXML Asset `json:"analytic_xml"`
	UDM         Asset `json:"udm"`
	Visual      Asset `json:"visual"`
	VisualXML   Asset `json:"visual_xml"`
}

// Asset represents a single asset available for a scene
type Asset struct {
	Links       Links    `json:"_links"`
	Status      string   `json:"status"`
	Type        string   `json:"type"`
	Location    string   `json:"location,omitempty"`
	ExpiresAt   string   `json:"expires_at,omitempty"`
	Permissions []string `json:"_permissions,omitempty"`
}

// Links represents the links JSON structure.
type Links struct {
	Self     string `json:"_self"`
	Activate string `json:"activate"`
	Type     string `json:"type"`
}

// GetScenes returns a string containing the scenes requested
func GetScenes(inputFeature *geojson.Feature, options SearchOptions) (string, error) {
	var (
		err      error
		response *http.Response
		body     []byte
		req      request
		fc       *geojson.FeatureCollection
		fci      interface{}
	)

	req.ItemTypes = append(req.ItemTypes, "REOrthoTile")
	req.Filter.Type = "AndFilter"
	req.Filter.Config = make([]interface{}, 0)
	if inputFeature != nil {
		if inputFeature.Geometry == nil && inputFeature.Bbox != nil {
			inputFeature.Geometry = inputFeature.Bbox.Polygon()
		}
		if inputFeature.Geometry != nil {
			req.Filter.Config = append(req.Filter.Config, objectFilter{Type: "GeometryFilter", FieldName: "geometry", Config: inputFeature.Geometry})
		}
		acquiredDate := inputFeature.PropertyString("acquiredDate")
		maxAcquiredDate := inputFeature.PropertyString("maxAcquiredDate")
		if acquiredDate != "" || maxAcquiredDate != "" {
			dc := dateConfig{GTE: acquiredDate, LTE: maxAcquiredDate}
			req.Filter.Config = append(req.Filter.Config, objectFilter{Type: "DateRangeFilter", FieldName: "acquired", Config: dc})
		}
		cloudCover := inputFeature.PropertyFloat("cloudCover")
		if cloudCover > 0 {
			cc := rangeConfig{LTE: cloudCover}
			req.Filter.Config = append(req.Filter.Config, objectFilter{Type: "RangeFilter", FieldName: "cloud_cover", Config: cc})
		}
	}
	if body, err = json.Marshal(req); err != nil {
		return "", err
	}
	if response, err = doRequest(doRequestInput{method: "POST", inputURL: "data/v1/quick-search", body: body, contentType: "application/json"}, RequestContext{PlanetKey: options.PlanetKey}); err != nil {
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
	if options.Tides {
		context := tides.Context{TidesURL: options.TidesURL}
		if fc, err = tides.GetTides(fc, context); err != nil {
			return "", err
		}
	}
	body, err = geojson.Write(fc)
	return string(body), err
}

func transformFeatureCollection(fc *geojson.FeatureCollection) *geojson.FeatureCollection {
	var (
		result *geojson.FeatureCollection
	)
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
		feature := geojson.NewFeature(curr.Geometry, id, properties)
		feature.Bbox = curr.ForceBbox()
		features[inx] = feature
	}
	result = geojson.NewFeatureCollection(features)
	return result
}

// Activate returns the status of the analytic asset and
// attempts to activate it if needed
func Activate(id string, context RequestContext) ([]byte, error) {
	var (
		response *http.Response
		err      error
		body     []byte
		assets   Assets
	)
	if response, err = doRequest(doRequestInput{method: "GET", inputURL: "data/v1/item-types/REOrthoTile/items/" + id + "/assets/"}, context); err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, _ = ioutil.ReadAll(response.Body)
	if err = json.Unmarshal(body, &assets); err != nil {
		return nil, err
	}
	if assets.Analytic.Status == "inactive" {
		log.Printf("Attempting to activate image %v.", id)
		go doRequest(doRequestInput{method: "GET", inputURL: assets.Analytic.Links.Activate}, context)
	}
	return json.Marshal(assets.Analytic)
}
