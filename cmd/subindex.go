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

package cmd

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/paulsmith/gogeos/geos"
	"github.com/venicegeo/geojson-geos-go/geojsongeos"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

// SubIndex represents a sub-index for the image catalog
type SubIndex struct {
	WfsURL      string `json:"wfsurl"`
	FeatureType string `json:"featureType"`
	SubIndexID  string `json:"subIndexID"`
}

// ResolveSubIndexID determines the SubIndexID
// based on the other parameters and returns it
func (si *SubIndex) ResolveSubIndexID() string {
	si.SubIndexID = catalog.ImageCatalogPrefix() + ":" + si.WfsURL + ":" + si.FeatureType
	return si.SubIndexID
}

func subIndexHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		bytes    []byte
		err      error
		subIndex SubIndex
	)

	if origin := request.Header.Get("Origin"); origin != "" {
		writer.Header().Set("Access-Control-Allow-Origin", origin)
		writer.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		writer.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if request.Method == "OPTIONS" {
		return
	}

	if request.Method == "POST" {
		defer request.Body.Close()
		if bytes, err = ioutil.ReadAll(request.Body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
		}

		if err = json.Unmarshal(bytes, &subIndex); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
		}
	}
	if subIndex.WfsURL == "" {
		subIndex.WfsURL = request.FormValue("wfsurl")
	}
	if subIndex.WfsURL == "" {
		http.Error(writer, "Calls to /subindex must contain a WFS URL.", http.StatusBadRequest)
	}

	subIndex.ResolveSubIndexID()
	go createSubindex(subIndex)

	writer.Header().Set("Content-Type", "application/json")
	bytes, _ = json.Marshal(subIndex)
	writer.Write(bytes)
}

func createSubindex(subIndex SubIndex) {
	var (
		request  *http.Request
		response *http.Response
		bytes    []byte
		gjIfc    interface{}
		fc       *geojson.FeatureCollection
		ok       bool
		geometry *geos.Geometry
	)

	v := url.Values{}
	v.Set("maxFeatures", "9999")
	v.Set("outputFormat", "application/json")
	v.Set("version", "2.0.0")
	v.Set("request", "GetFeature")
	v.Set("typeName", subIndex.FeatureType)

	qurl := subIndex.WfsURL + v.Encode()

	request, _ = http.NewRequest("GET", qurl, nil)
	response, _ = catalog.HTTPClient().Do(request)

	// Check for HTTP errors
	if response.StatusCode < 200 || response.StatusCode > 299 {
		log.Printf("Received %v: \"%v\" when performing a GetFeature request on %v", response.StatusCode, response.Status, subIndex.WfsURL)
		return
	}

	defer response.Body.Close()
	bytes, _ = ioutil.ReadAll(response.Body)

	gjIfc, _ = geojson.Parse(bytes)
	if fc, ok = gjIfc.(*geojson.FeatureCollection); ok {
		geometry, _ = geojsongeos.GeosFromGeoJSON(fc)
		// This will ensure the sub-index is considered in subsequent operations
		catalog.SetSubIndex(subIndex.SubIndexID, geometry)
		// This will ensure the sub-index is considered with already-harvested images
		catalog.PopulateSubIndex(subIndex.SubIndexID)
	}
}
