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
	"time"

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
		break // comment this line to stop temporarily capping the dataset size
	}
	if err != nil {
		log.Print(err.Error())
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
	var score float64
	for _, curr := range fc.Features {
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
		adString := curr.Properties["acquired"].(string)
		properties["acquiredDate"] = adString
		if adTime, err := time.Parse(time.RFC3339, adString); err == nil {
			score = float64(-adTime.Unix())
		} else {
			score = 0
		}
		properties["sensorName"] = "PlanetLabsOrthoAnalytic"
		properties["bands"] = [4]string{"red", "green", "blue", "alpha"}
		feature := geojson.NewFeature(curr.Geometry, "pl:ortho:"+curr.ID, properties)
		feature.Bbox = curr.ForceBbox()
		catalog.StoreFeature(feature, score)
	}
}

// It seems like a stupid question, but how do I make a resource file like
// data/gz_2010_us_outline_20m.json available to the application
// without downloading it?
// var usBoundary *geos.Geom
//
// func getUSBoundary() *geos.Geom {
//   if usBoundary == nil {
//
//   }
//   return usBoundary
// }
//
// function whiteList(feature *geojson.Feature) bool {
//
// }
//

func storePlanetRapidEye(fc *geojson.FeatureCollection) {
	var score float64
	for _, curr := range fc.Features {
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(int)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
		properties["acquiredDate"] = curr.Properties["acquired"].(string)
		adString := curr.Properties["acquired"].(string)
		properties["acquiredDate"] = adString
		if adTime, err := time.Parse(time.RFC3339, adString); err == nil {
			score = float64(-adTime.Unix())
		} else {
			score = 0
		}
		properties["sensorName"] = "PlanetLabsRapidEye"
		properties["bands"] = [4]string{"red", "green", "blue", "red edge"}
		feature := geojson.NewFeature(curr.Geometry, "pl:rapideye:"+curr.ID, properties)
		feature.Bbox = curr.ForceBbox()
		catalog.StoreFeature(feature, score)
	}
}

func storePlanetLandsat(fc *geojson.FeatureCollection) {
	var score float64
	for _, curr := range fc.Features {
		score = float64(0)
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
		adString := curr.Properties["acquired"].(string)
		properties["acquiredDate"] = adString
		if adTime, err := time.Parse(time.RFC3339, adString); err == nil {
			score = float64(-adTime.Unix())
		} else {
			score = 0
		}
		properties["sensorName"] = "Landsat8"
		bands := make(map[string]string)
		products := curr.Properties["data"].(map[string]interface{})["products"].(map[string]interface{})
		bands["coastal"] = products["band_1"].(map[string]interface{})["full"].(string)
		bands["blue"] = products["band_2"].(map[string]interface{})["full"].(string)
		bands["green"] = products["band_3"].(map[string]interface{})["full"].(string)
		bands["red"] = products["band_4"].(map[string]interface{})["full"].(string)
		bands["nir"] = products["band_5"].(map[string]interface{})["full"].(string)
		bands["swir1"] = products["band_6"].(map[string]interface{})["full"].(string)
		bands["swir2"] = products["band_7"].(map[string]interface{})["full"].(string)
		bands["panchromatic"] = products["band_8"].(map[string]interface{})["full"].(string)
		bands["cirrus"] = products["band_9"].(map[string]interface{})["full"].(string)
		bands["tirs1"] = products["band_10"].(map[string]interface{})["full"].(string)
		bands["tirs2"] = products["band_11"].(map[string]interface{})["full"].(string)
		properties["bands"] = bands
		feature := geojson.NewFeature(curr.Geometry, "pl:landsat:"+curr.ID, properties)
		feature.Bbox = curr.ForceBbox()
		catalog.StoreFeature(feature, score)
	}
}

var planetKey string

var planetCmd = &cobra.Command{
	Use:   "planet",
	Short: "Harvest Planet Labs",
	Long: `
Harvest image metadata from Planet Labs

This function will harvest metadata from Planet Labs, using the PL_API_KEY in the environment`,
	Run: func(cmd *cobra.Command, args []string) {
		catalog.SetPlanetAPIKey(planetKey)
		// harvestPlanetEndpoint("v0/scenes/ortho/?count=1000", storePlanetOrtho)
		harvestPlanetEndpoint("v0/scenes/landsat/?count=1000", storePlanetLandsat)
		// harvestPlanetEndpoint("v0/scenes/rapideye/?count=1000", storePlanetRapidEye)
	},
}

func init() {
	planetCmd.Flags().StringVarP(&planetKey, "PL_API_KEY", "p", "bar", "Planet Labs API Key")
}
