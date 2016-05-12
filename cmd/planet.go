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
	"log"
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

type harvestCallback func(*geojson.FeatureCollection)

func harvestPlanetOrtho() {
	endpoint := "v0/scenes/ortho/?count=1000"
	var err error
	for err == nil {
		var (
			next        string
			responseURL *url.URL
		)
		next, err = harvestPlanetEndpoint(endpoint, storePlanetOrtho)
		if (len(next) == 0) || (err != nil) {
			break
		}
		responseURL, err = url.Parse(next)
		endpoint = responseURL.RequestURI()
	}
	if err != nil {
		log.Print(err.Error)
	}
}

func harvestPlanetRapidEye() {
	endpoint := "v0/scenes/rapideye/?count=1000"
	var err error
	for err == nil {
		var (
			next        string
			responseURL *url.URL
		)
		next, err = harvestPlanetEndpoint(endpoint, storePlanetRapidEye)
		if (len(next) == 0) || (err != nil) {
			break
		}
		responseURL, err = url.Parse(next)
		endpoint = responseURL.RequestURI()
	}
	if err != nil {
		log.Print(err.Error)
	}
}

func harvestPlanetEndpoint(endpoint string, callback harvestCallback) (string, error) {
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
	client := catalog.RedisClient()

	for _, curr := range fc.Features {
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
		properties["acquiredDate"] = curr.Properties["acquired"].(string)
		feature := geojson.NewFeature(curr.Geometry, curr.ID, properties)
		feature.Bbox = curr.ForceBbox()
		bytes, _ := json.Marshal(feature)
		key := "test-image-pl:" + curr.ID
		client.Set(key, string(bytes), 0)
		client.SAdd("test-images", key)
	}
}

func storePlanetRapidEye(fc *geojson.FeatureCollection) {
	client := catalog.RedisClient()

	for _, curr := range fc.Features {
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
		properties["acquiredDate"] = curr.Properties["acquired"].(string)
		feature := geojson.NewFeature(curr.Geometry, curr.ID, properties)
		feature.Bbox = curr.ForceBbox()
		bytes, _ := json.Marshal(feature)
		key := "test-image-pl:" + curr.ID
		client.Set(key, string(bytes), 0)
		client.SAdd("test-images", key)
	}
}

var planetCmd = &cobra.Command{
	Use:   "planet",
	Short: "Harvest Planet Labs",
	Long: `
Harvest image metadata from Planet Labs

This function will harvest metadata from Planet Labs, using the PL_API_KEY in the environment`,
	Run: func(cmd *cobra.Command, args []string) {
		harvestPlanetOrtho()
		// harvestPlanetRapidEye() Since RapidEye doesn't report cloud cover, why bother?
	},
}
