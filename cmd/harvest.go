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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pz-gocommon/elasticsearch"
	pzworkflow "github.com/venicegeo/pz-workflow/server"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

type harvestCallback func(*geojson.FeatureCollection, bool) error

const harvestEventKey = "beachfront:harvest:new-image-harvested"

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
	EventID             string
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

func harvestEventID(auth string) (string, error) {
	var (
		request    *http.Request
		response   *http.Response
		err        error
		result     string
		etBytes    []byte
		eventTypes []pzworkflow.EventType
		httpReturn pzworkflow.HTTPReturn
	)
	requestURL := "https://pz-gateway." + os.Getenv("DOMAIN") + "/eventType?per_page=10000"
	if request, err = http.NewRequest("GET", requestURL, nil); err != nil {
		return result, err
	}
	request.Header.Set("Authorization", auth)
	if response, err = catalog.HTTPClient().Do(request); err != nil {
		return result, err
	}

	// Check for HTTP errors
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return result, &HTTPError{Status: response.StatusCode, Message: "Failed to retrieve harvest event ID: " + response.Status}
	}

	defer response.Body.Close()
	if etBytes, err = ioutil.ReadAll(response.Body); err != nil {
		return result, err
	}

	if err = json.Unmarshal(etBytes, &httpReturn); err != nil {
		return result, err
	}

	if etBytes, err = json.Marshal(httpReturn.Data); err != nil {
		return result, err
	}

	if err = json.Unmarshal(etBytes, &eventTypes); err != nil {
		return result, err
	}

	for _, eventType := range eventTypes {
		if eventType.Name == harvestEventKey {
			// TODO: Sanity check to make sure this object has the right signature
			result = eventType.ID.String()
			break
		}
	}

	if result == "" {
		return addEventType(auth)
	}

	return result, nil
}

func addEventType(auth string) (string, error) {
	var (
		request        *http.Request
		response       *http.Response
		err            error
		result         string
		eventTypeBytes []byte
		eventType      pzworkflow.EventType
	)
	eventType.Name = harvestEventKey
	eventType.Mapping = make(map[string]elasticsearch.MappingElementTypeName)
	eventType.Mapping["ImageID"] = "string"
	if eventTypeBytes, err = json.Marshal(&eventType); err != nil {
		return result, err
	}

	requestURL := "https://pz-gateway." + os.Getenv("DOMAIN") + "/eventType"
	log.Printf(requestURL)
	if request, err = http.NewRequest("POST", requestURL, bytes.NewBuffer(eventTypeBytes)); err != nil {
		return result, err
	}

	request.Header.Set("Authorization", auth)
	request.Header.Set("Content-Type", "application/json")
	if response, err = catalog.HTTPClient().Do(request); err != nil {
		return result, err
	}

	// Check for HTTP errors
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return result, &HTTPError{Status: response.StatusCode, Message: "Failed to add harvest event: " + response.Status}
	}

	defer response.Body.Close()
	if eventTypeBytes, err = ioutil.ReadAll(response.Body); err != nil {
		return result, err
	}

	if err = json.Unmarshal(eventTypeBytes, &eventType); err != nil {
		return result, err
	}
	result = eventType.ID.String()
	return result, err
}
