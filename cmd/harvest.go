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
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

type harvestCallback func(*geojson.FeatureCollection, bool) error

func recurrentHandling() {
	for {
		if planetKey := catalog.Recurrence("pl"); planetKey != "" {
			options := HarvestOptions{PlanetKey: planetKey}
			harvestPlanet(options)
		}
		time.Sleep(24 * time.Hour)
	}
}

// HarvestOptions are options for a harvesting operation
type HarvestOptions struct {
	Recurring           bool   `json:"recurring"`
	Event               bool   `json:"event"`
	Reharvest           bool   `json:"reharvest"`
	PlanetKey           string `json:"PL_API_KEY"`
	PiazzaAuthorization string `json:"pz-auth"`
	callback            harvestCallback
}

// HTTPError represents any HTTP error
type HTTPError struct {
	Status  int
	Message string
}

func (err HTTPError) Error() string {
	return fmt.Sprintf("%d: %v", err.Status, err.Message)
}

func testPiazzaAuth(auth string) error {
	var (
		request  *http.Request
		response *http.Response
		err      error
	)
	inputURL := "https://pz-gateway." + os.Getenv("DOMAIN") + "/"
	if request, err = http.NewRequest("GET", inputURL, nil); err != nil {
		return err
	}
	request.Header.Set("Authorization", auth)
	if response, err = catalog.HTTPClient().Do(request); err != nil {
		return err
	}

	// Check for HTTP errors
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return &HTTPError{Status: response.StatusCode, Message: "Failed to authenticate: " + response.Status}
	}

	return nil
}
