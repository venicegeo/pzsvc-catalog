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
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

type harvestCallback func(*geojson.FeatureCollection)

func harvestPlanetEndpoint(endpoint string, key string, callback harvestCallback) {
	var err error
	for err == nil && (endpoint != "") {
		var (
			next        string
			responseURL *url.URL
		)
		next, err = harvestPlanetOperation(endpoint, key, callback)
		if (len(next) == 0) || (err != nil) {
			break
		}
		responseURL, err = url.Parse(next)
		endpoint = responseURL.RequestURI()
		// break // uncomment this line temporarily cap the dataset size
	}
	if err != nil {
		log.Print(err.Error())
	}
}

func harvestPlanetOperation(endpoint string, key string, callback harvestCallback) (string, error) {
	log.Printf("Harvesting %v", endpoint)
	var (
		response       *http.Response
		fc             *geojson.FeatureCollection
		planetResponse catalog.PlanetResponse
		err            error
	)
	if response, err = catalog.DoPlanetRequest("GET", endpoint, key); err != nil {
		return "", err
	}

	if planetResponse, fc, err = catalog.UnmarshalPlanetResponse(response); err != nil {
		return "", err
	}
	callback(fc)

	return planetResponse.Links.Next, harvestSanityCheck()
}

func harvestSanityCheck() error {
	// if catalog.IndexSize() > 100000 {
	// 	return errors.New("Okay, we're big enough.")
	// }
	return nil
}

func storePlanetOrtho(fc *geojson.FeatureCollection) {
	var score float64
	for _, curr := range fc.Features {
		if !whiteList(curr) {
			continue
		}
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
		properties["resolution"] = curr.Properties["image_statistics"].(map[string]interface{})["gsd"].(float64)
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

var usBoundary *geojson.FeatureCollection

func getUSBoundary() *geojson.FeatureCollection {
	var (
		gj  interface{}
		err error
	)
	if usBoundary == nil {
		if gj, err = geojson.ParseFile("data/Black_list_AOIs.geojson"); err != nil {
			log.Printf("Parse error: %v\n", err.Error())
			return nil
		}
		usBoundary = gj.(*geojson.FeatureCollection)
	}
	return usBoundary
}

func whiteList(feature *geojson.Feature) bool {
	bbox := feature.ForceBbox()
	fc := getUSBoundary()
	if fc != nil {
		for _, curr := range fc.Features {
			if bbox.Overlaps(curr.ForceBbox()) {
				return false
			}
		}
	}
	return true
}

func storePlanetRapidEye(fc *geojson.FeatureCollection) {
	var score float64
	for _, curr := range fc.Features {
		if !whiteList(curr) {
			continue
		}
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(int)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
		properties["acquiredDate"] = curr.Properties["acquired"].(string)
		properties["resolution"] = curr.Properties["image_statistics"].(map[string]interface{})["gsd"].(float64)
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
		if !whiteList(curr) {
			continue
		}
		score = float64(0)
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
		id := curr.ID
		url := landsatIDToS3Path(id)
		properties["thumb_large"] = url + id + "_thumb_large.jpg"
		properties["thumb_small"] = url + id + "_thumb_small.jpg"
		properties["resolution"] = curr.Properties["image_statistics"].(map[string]interface{})["gsd"].(float64)
		adString := curr.Properties["acquired"].(string)
		properties["acquiredDate"] = adString
		if adTime, err := time.Parse(time.RFC3339, adString); err == nil {
			score = float64(-adTime.Unix())
		} else {
			score = 0
		}
		properties["sensorName"] = "Landsat8"
		bands := make(map[string]string)
		bands["coastal"] = url + id + "_B1.TIF"
		bands["blue"] = url + id + "_B2.TIF"
		bands["green"] = url + id + "_B3.TIF"
		bands["red"] = url + id + "_B4.TIF"
		bands["nir"] = url + id + "_B5.TIF"
		bands["swir1"] = url + id + "_B6.TIF"
		bands["swir2"] = url + id + "_B7.TIF"
		bands["panchromatic"] = url + id + "_B8.TIF"
		bands["cirrus"] = url + id + "_B9.TIF"
		bands["tirs1"] = url + id + "_B10.TIF"
		bands["tirs2"] = url + id + "_B11.TIF"
		properties["bands"] = bands
		feature := geojson.NewFeature(curr.Geometry, "landsat:"+curr.ID, properties)
		feature.Bbox = curr.ForceBbox()
		catalog.StoreFeature(feature, score)
	}
}

func landsatIDToS3Path(id string) string {
	result := "http://landsat-pds.s3.amazonaws.com/"
	if strings.HasPrefix(id, "LC8") {
		result += "L8/"
	}
	result += id[3:6] + "/" + id[6:9] + "/" + id + "/"
	return result
}

// Not all products have all bands
func pluckBandToProducts(products map[string]interface{}, bands *map[string]string, bandName string, productName string) {
	if product, ok := products[productName]; ok {
		(*bands)[bandName] = product.(map[string]interface{})["full"].(string)
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
		harvestPlanet(planetKey)
	},
}

func harvestPlanet(key string) {
	// harvestPlanetEndpoint("v0/scenes/ortho/?count=1000", storePlanetOrtho)
	harvestPlanetEndpoint("v0/scenes/landsat/?count=1000", key, storePlanetLandsat)
	// harvestPlanetEndpoint("v0/scenes/rapideye/?count=1000", storePlanetRapidEye)
}

func init() {
	planetCmd.Flags().StringVarP(&planetKey, "PL_API_KEY", "p", "", "Planet Labs API Key")
}
