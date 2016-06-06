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
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/paulsmith/gogeos/geos"
	"github.com/spf13/cobra"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/geojson-wkt-go/geojsonwkt"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

var dgCmd = &cobra.Command{
	Use:   "dg",
	Short: "Harvest Digital Globe",
	Long: `
Harvest image metadata from Digital Globe

This function will harvest metadata from Planet Labs, using the Authorization PL_API_KEY in the environment`,
	Run: func(cmd *cobra.Command, args []string) {
		harvestPlanet(planetKey)
	},
}

const dgurl = `{"aoiWkt":"POLYGON((%v))","layerControlFilters":{},"includeArchiveResults":true,"sortProperty":"acquisitionDate","sortAscending":false,"pageSize":1,"pageNumber":0,"excludeDems":true,"firstPage":1,"lastPage":null,"currentPage":1,"totalPages":null,"totalRecords":null,"sortKey":"acquisitionDate","order":1}`

const wktPolygon = "%v %v,%v %v,%v %v,%v %v,%v %v"

// DGResponse represents the results of a DG call
type DGResponse struct {
	Count   int       `json:"count"`
	Entries []DGEntry `json:"page"`
}

// DGEntry represents a single DG entry
type DGEntry struct {
	ID              string            `json:"featureId"`
	WKT             string            `json:"geometryWkt"`
	AcquisitionDate int64             `json:"acquisitionDate"`
	FeatureMetadata DGFeatureMetadata `json:"featureMetadata"`
}

// DGFeatureMetadata represents a single DG feature metadata entry
type DGFeatureMetadata struct {
	ImagerySource string  `json:"imagerySource"`
	CloudCover    float64 `json:"cloudCover"`
}

func harvestDG(auth string) {
	var (
		response   *http.Response
		err        error
		requestURL string
		wkt        string
		dgResponse DGResponse
	)
	// for lat := -80.0; lat < 80; lat += 0.5 {
	// 	for long := -180.0; long < 180; long += 0.5 {
	for lat := -10.0; lat < 10; lat += 0.5 {
		for long := -10.0; long < 10; long += 0.5 {
			coords := [4]float64{long, lat, long + 0.5, lat + 0.5}
			bbox := geojson.NewBoundingBox(coords)
			if !whiteList(bbox) {
				continue
			}
			wkt = fmt.Sprintf(wktPolygon, long, lat+0.5, long+.5, lat+0.5, long+0.5, lat, long, lat, long, lat+0.5)
			log.Printf("Sending: %v", wkt)
			requestURL = fmt.Sprintf(dgurl, wkt)
			if response, err = catalog.DoDGRequest(requestURL, auth); err == nil {
				var bodyText []byte
				defer response.Body.Close()
				if bodyText, err = ioutil.ReadAll(response.Body); err == nil {
					if err = json.Unmarshal(bodyText, &dgResponse); err == nil {
						harvestDGResponse(dgResponse)
					}
				}
			}
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		log.Fatal(err.Error())
	}
}

func harvestDGResponse(response DGResponse) {
	for _, entry := range response.Entries {
		var (
			aTime        time.Time
			err          error
			geosGeometry *geos.Geometry
			gjGeometry   interface{}
			threshold    time.Time
			bbox         geojson.BoundingBox
		)

		// If the entry is too old, move on
		threshold = time.Now().Add(-24 * 7 * time.Hour)
		if threshold.After(time.Unix(entry.AcquisitionDate, 0)) {
			log.Printf("%v is too old", time.Unix(entry.AcquisitionDate, 0).Format(time.RFC3339))
			return
		}

		// Get the geometry and do some sanity checking on it
		if geosGeometry, err = geos.FromWKT(entry.WKT); err != nil {
			log.Print(err.Error())
			continue
		}
		if gjGeometry, err = geojsongeos.GeoJSONFromGeos(geosGeometry); err != nil {
			log.Print(err.Error())
			continue
		}
		if forceable, ok := gjGeometry.(geojson.BoundingBoxIfc); ok {
			bbox = forceable.ForceBbox()
			if !whiteList(bbox) {
				continue
			}
		} else {
			continue
		}

		properties := make(map[string]interface{})
		properties["cloudCover"] = entry.FeatureMetadata.CloudCover
		aTime = time.Unix(entry.AcquisitionDate, 0)
		properties["acquiredDate"] = aTime.Format(time.RFC3339)
		score := float64(-entry.AcquisitionDate)
		properties["sensorName"] = entry.FeatureMetadata.ImagerySource
		// properties["bands"] = [4]string{"red", "green", "blue", "red edge"}
		feature := geojson.NewFeature(gjGeometry, "dg:"+entry.FeatureMetadata.ImagerySource+":"+entry.ID, properties)
		feature.Bbox = bbox
		log.Printf("Score: %v; %v", score, feature)
		// catalog.StoreFeature(feature, score)
	}
}
