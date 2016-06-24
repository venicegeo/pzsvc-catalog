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
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

func planetHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		drop, recurring, reharvest, event bool
		optionsBytes                      []byte
	)
	reharvest, _ = strconv.ParseBool(request.FormValue("reharvest"))
	event, _ = strconv.ParseBool(request.FormValue("event"))
	planetKey := request.FormValue("PL_API_KEY")
	pzAuth := request.Header.Get("Authorization")
	recurring, _ = strconv.ParseBool(request.FormValue("recurring"))
	options := HarvestOptions{
		PlanetKey:           planetKey,
		PiazzaAuthorization: pzAuth,
		Reharvest:           reharvest,
		Event:               event,
		Recurring:           recurring}

	// Let's test the credentials before we do anything
	if err := testPiazzaAuth(pzAuth); err != nil {
		if httpError, ok := err.(*HTTPError); ok {
			http.Error(writer, httpError.Message, httpError.Status)
		} else {
			http.Error(writer, "Attempt to authenticate failed. "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if recurring {
		optionsBytes, _ = json.Marshal(options)
		log.Print("This thing should recur.")
	}
	if drop, _ = strconv.ParseBool(request.FormValue("dropIndex")); drop {
		writer.Write([]byte("Dropping existing index.\n"))
		catalog.DropIndex()
	}
	catalog.SetRecurrence("pl", recurring, string(optionsBytes))

	go harvestPlanet(options)
	writer.Write([]byte("Harvesting started. Check back later."))
}

func harvestPlanetEndpoint(endpoint string, options HarvestOptions) {
	var err error
	for err == nil && (endpoint != "") {
		var (
			next        string
			responseURL *url.URL
		)
		next, err = harvestPlanetOperation(endpoint, options)
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

func harvestPlanetOperation(endpoint string, options HarvestOptions) (string, error) {
	log.Printf("Harvesting %v", endpoint)
	var (
		response       *http.Response
		fc             *geojson.FeatureCollection
		planetResponse catalog.PlanetResponse
		err            error
	)
	if response, err = catalog.DoPlanetRequest("GET", endpoint, options.PlanetKey); err != nil {
		return "", err
	}

	if planetResponse, fc, err = catalog.UnmarshalPlanetResponse(response); err != nil {
		return "", err
	}
	if err = options.callback(fc, options.Reharvest); err == nil {
		err = harvestSanityCheck()
	}

	return planetResponse.Links.Next, err
}

func harvestSanityCheck() error {
	// if catalog.IndexSize() > 100000 {
	// 	return errors.New("Okay, we're big enough.")
	// }
	return nil
}

// func storePlanetOrtho(fc *geojson.FeatureCollection, reharvest bool) {
// 	var score float64
// 	for _, curr := range fc.Features {
// 		if !whiteList(curr) {
// 			continue
// 		}
// 		properties := make(map[string]interface{})
// 		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
// 		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
// 		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
// 		properties["resolution"] = curr.Properties["image_statistics"].(map[string]interface{})["gsd"].(float64)
// 		adString := curr.Properties["acquired"].(string)
// 		properties["acquiredDate"] = adString
// 		if adTime, err := time.Parse(time.RFC3339, adString); err == nil {
// 			score = float64(-adTime.Unix())
// 		} else {
// 			score = 0
// 		}
// 		properties["sensorName"] = "PlanetLabsOrthoAnalytic"
// 		properties["bands"] = [4]string{"red", "green", "blue", "alpha"}
// 		feature := geojson.NewFeature(curr.Geometry, "pl:ortho:"+curr.ID, properties)
// 		feature.Bbox = curr.ForceBbox()
// 		catalog.StoreFeature(feature, score, reharvest)
// 	}
// }

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

// func storePlanetRapidEye(fc *geojson.FeatureCollection, reharvest bool) error {
// 	var (
// 		score float64
// 		err   error
// 	)
// 	for _, curr := range fc.Features {
// 		if !whiteList(curr) {
// 			continue
// 		}
// 		properties := make(map[string]interface{})
// 		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(int)
// 		properties["path"] = curr.Properties["links"].(map[string]interface{})["self"].(string)
// 		properties["thumbnail"] = curr.Properties["links"].(map[string]interface{})["thumbnail"].(string)
// 		properties["acquiredDate"] = curr.Properties["acquired"].(string)
// 		properties["resolution"] = curr.Properties["image_statistics"].(map[string]interface{})["gsd"].(float64)
// 		adString := curr.Properties["acquired"].(string)
// 		properties["acquiredDate"] = adString
// 		if adTime, err2 := time.Parse(time.RFC3339, adString); err2 == nil {
// 			score = float64(-adTime.Unix())
// 		} else {
// 			score = 0
// 		}
// 		properties["sensorName"] = "PlanetLabsRapidEye"
// 		properties["bands"] = [4]string{"red", "green", "blue", "red edge"}
// 		feature := geojson.NewFeature(curr.Geometry, "pl:rapideye:"+curr.ID, properties)
// 		feature.Bbox = curr.ForceBbox()
// 		catalog.StoreFeature(feature, score, reharvest)
// 	}
// 	return err
// }

func storePlanetLandsat(fc *geojson.FeatureCollection, reharvest bool) error {
	var (
		score float64
		err   error
	)
	for _, curr := range fc.Features {
		if !whiteList(curr) {
			continue
		}
		score = float64(0)
		properties := make(map[string]interface{})
		properties["cloudCover"] = curr.Properties["cloud_cover"].(map[string]interface{})["estimated"].(float64)
		id := curr.ID
		url := landsatIDToS3Path(curr.ID)
		properties["path"] = url + "index.html"
		properties["thumb_large"] = url + id + "_thumb_large.jpg"
		properties["thumb_small"] = url + id + "_thumb_small.jpg"
		properties["resolution"] = curr.Properties["image_statistics"].(map[string]interface{})["gsd"].(float64)
		adString := curr.Properties["acquired"].(string)
		properties["acquiredDate"] = adString
		if adTime, err2 := time.Parse(time.RFC3339, adString); err2 == nil {
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
		feature := geojson.NewFeature(curr.Geometry, "landsat:"+id, properties)
		feature.Bbox = curr.ForceBbox()
		if err = catalog.StoreFeature(feature, score, reharvest); err != nil {
			break
		}
	}
	return err
}

func landsatIDToS3Path(id string) string {
	result := "https://landsat-pds.s3.amazonaws.com/"
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
		options := HarvestOptions{PlanetKey: planetKey}
		harvestPlanet(options)
	},
}

func harvestPlanet(options HarvestOptions) {
	// harvestPlanetEndpoint("v0/scenes/ortho/?count=1000", storePlanetOrtho)
	options.callback = storePlanetLandsat
	harvestPlanetEndpoint("v0/scenes/landsat/?count=1000", options)
	// harvestPlanetEndpoint("v0/scenes/rapideye/?count=1000", storePlanetRapidEye)
}

func init() {
	planetCmd.Flags().StringVarP(&planetKey, "PL_API_KEY", "p", "", "Planet Labs API Key")
}
