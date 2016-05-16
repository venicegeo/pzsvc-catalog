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
	"log"
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

type harvestCallback func(*geojson.FeatureCollection)

func harvestPlanetEndpoint(endpoint string, callback harvestCallback) {
	var err error
	for err == nil && (endpoint != "") {
		var (
			next        string
			responseURL *url.URL
		)
		next, err = harvestPlanetOperation(endpoint, callback)
		if (len(next) == 0) || (err != nil) {
			break
		}
		responseURL, err = url.Parse(next)
		endpoint = responseURL.RequestURI()
		// break // comment this line to temporarily cap the dataset size
	}
	if err != nil {
		log.Print(err.Error)
	}
}

func harvestPlanetOperation(endpoint string, callback harvestCallback) (string, error) {
	log.Printf("Harvesting %v", endpoint)
	var (
		response       *http.Response
		fc             *geojson.FeatureCollection
		planetResponse catalog.PlanetResponse
		err            error
	)
	if response, err = catalog.DoPlanetRequest("GET", endpoint); err != nil {
		return "", err
	}

	if planetResponse, fc, err = catalog.UnmarshalPlanetResponse(response); err != nil {
		return "", err
	}
	callback(fc)

	return planetResponse.Links.Next, nil
}

func storePlanetOrtho(fc *geojson.FeatureCollection) {
	for _, curr := range fc.Features {
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
		properties["acquiredDate"] = curr.Properties["acquired"].(string)
		properties["sensorName"] = "PlanetLabsOrthoAnalytic"
		properties["bands"] = [4]string{"red", "green", "blue", "alpha"}
		feature := geojson.NewFeature(curr.Geometry, "pl:ortho:"+curr.ID, properties)
		feature.Bbox = curr.ForceBbox()
		catalog.StoreFeature(feature)
	}
}

func storePlanetRapidEye(fc *geojson.FeatureCollection) {
	for _, curr := range fc.Features {
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
		properties["acquiredDate"] = curr.Properties["acquired"].(string)
		properties["sensorName"] = "PlanetLabsRapidEye"
		properties["bands"] = [4]string{"red", "green", "blue", "red edge"}
		feature := geojson.NewFeature(curr.Geometry, "pl:rapideye:"+curr.ID, properties)
		feature.Bbox = curr.ForceBbox()
		catalog.StoreFeature(feature)
	}
}

func storePlanetLandsat(fc *geojson.FeatureCollection) {
	for _, curr := range fc.Features {
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
		properties["acquiredDate"] = curr.Properties["acquired"].(string)
		properties["sensorName"] = "Landsat8"
		properties["bands"] = [11]string{"coastal", "red", "green", "blue", "nir", "swir1", "swir2", "panchromatic", "cirrus", "tirs1", "tirs2"}
		feature := geojson.NewFeature(curr.Geometry, "pl:landsat:"+curr.ID, properties)
		feature.Bbox = curr.ForceBbox()
		catalog.StoreFeature(feature)
	}
}

var planetCmd = &cobra.Command{
	Use:   "planet",
	Short: "Harvest Planet Labs",
	Long: `
Harvest image metadata from Planet Labs

This function will harvest metadata from Planet Labs, using the PL_API_KEY in the environment`,
	Run: func(cmd *cobra.Command, args []string) {
		// harvestPlanetEndpoint("v0/scenes/ortho/?count=1000", storePlanetOrtho)
		harvestPlanetEndpoint("v0/scenes/landsat/?count=1000", storePlanetLandsat)
		// harvestPlanetEndpoint("v0/scenes/ortho/?count=1000", storePlanetRapidEye)Since RapidEye doesn't report cloud cover, why bother?
	},
}
