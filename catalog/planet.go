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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-lib"
)

const baseURLString = "https://api.planet.com/"

// HarvestPlanet harvests Planet Labs
func HarvestPlanet(options HarvestOptions) {
	// harvestPlanetEndpoint("v0/scenes/ortho/?count=1000", storePlanetOrtho)
	options.callback = storePlanetLandsat
	harvestPlanetEndpoint("v0/scenes/landsat/?count=1000", options)
	// harvestPlanetEndpoint("v0/scenes/rapideye/?count=1000", storePlanetRapidEye)
}

// DoPlanetRequest performs the request
// URL may be relative or absolute based on baseURLString
func DoPlanetRequest(method, inputURL, key string) (*http.Response, error) {
	var (
		request   *http.Request
		parsedURL *url.URL
		err       error
	)
	if !strings.Contains(inputURL, baseURLString) {
		baseURL, _ := url.Parse(baseURLString)
		parsedRelativeURL, _ := url.Parse(inputURL)
		resolvedURL := baseURL.ResolveReference(parsedRelativeURL)

		if parsedURL, err = url.Parse(resolvedURL.String()); err != nil {
			return nil, err
		}
		inputURL = parsedURL.String()
	}
	if request, err = http.NewRequest(method, inputURL, nil); err != nil {
		return nil, err
	}

	request.Header.Set("Authorization", "Basic "+getPlanetAuth(key))
	return pzsvc.HTTPClient().Do(request)
}

// unmarshalPlanetResponse parses the response and returns a Planet Labs response object
func unmarshalPlanetResponse(response *http.Response) (PlanetResponse, *geojson.FeatureCollection, error) {
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
		return unmarshal, fc, &pzsvc.HTTPError{Message: message, Status: response.StatusCode}
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
		if options.Cap {
			break
		}
	}
	if err != nil {
		log.Print(err.Error())
	}
	log.Printf("Harvested %v images.", IndexSize())
}

func harvestPlanetOperation(endpoint string, options HarvestOptions) (string, error) {
	log.Printf("Harvesting %v", endpoint)
	var (
		response       *http.Response
		fc             *geojson.FeatureCollection
		planetResponse PlanetResponse
		err            error
	)
	if response, err = DoPlanetRequest("GET", endpoint, options.PlanetKey); err != nil {
		return "", err
	}

	if planetResponse, fc, err = unmarshalPlanetResponse(response); err != nil {
		return "", err
	}
	if err = options.callback(fc, options); err == nil {
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

// var usBoundary *geojson.FeatureCollection
//
// func getUSBoundary() *geojson.FeatureCollection {
// 	var (
// 		gj  interface{}
// 		err error
// 	)
// 	if usBoundary == nil {
// 		if gj, err = geojson.ParseFile("data/Black_list_AOIs.geojson"); err != nil {
// 			log.Printf("Parse error: %v\n", err.Error())
// 			return nil
// 		}
// 		usBoundary = gj.(*geojson.FeatureCollection)
// 	}
// 	return usBoundary
// }
//
// func whiteList(feature *geojson.Feature) bool {
// 	bbox := feature.ForceBbox()
// 	fc := getUSBoundary()
// 	if fc != nil {
// 		for _, curr := range fc.Features {
// 			if bbox.Overlaps(curr.ForceBbox()) {
// 				return false
// 			}
// 		}
// 	}
// 	return true
// }

func storePlanetLandsat(fc *geojson.FeatureCollection, options HarvestOptions) error {
	var (
		err error
	)
	for _, curr := range fc.Features {
		// if !whiteList(curr) {
		// 	continue
		// }
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
		properties["fileFormat"] = "geotiff"
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
		if _, err = StoreFeature(feature, options.Reharvest); err != nil {
			log.Print(err.Error())
			break
		}
		if options.Event {
			cb := func(err error) {
				log.Printf("Failed to issue event for %v: %v", id, err.Error())
			}
			go issueEvent(options, feature, cb)
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
