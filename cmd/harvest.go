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
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pz-gocommon/elasticsearch"
	"github.com/venicegeo/pz-gocommon/gocommon"
	"github.com/venicegeo/pz-workflow/workflow"
	"github.com/venicegeo/pzsvc-lib"
)

type harvestCallback func(*geojson.FeatureCollection, HarvestOptions) error

const harvestEventTypeRoot = "beachfront:harvest:new-image-harvested"

var (
	domain = os.Getenv("DOMAIN")

	harvestEventTypeID string
)

// func recurrentHandling() {
// 	for {
// 		if planetKey := catalog.Recurrence("pl"); planetKey != "" {
// 			options := HarvestOptions{PlanetKey: planetKey}
// 			harvestPlanet(options)
// 		}
// 		time.Sleep(24 * time.Hour)
// 	}
// }

// HarvestOptions are options for a harvesting operation
type HarvestOptions struct {
	Recurring           bool   `json:"recurring"`
	Event               bool   `json:"event"`
	Reharvest           bool   `json:"reharvest"`
	PlanetKey           string `json:"PL_API_KEY"`
	PiazzaAuthorization string `json:"pz-auth"`
	callback            harvestCallback
	EventID             piazza.Ident
	Cap                 bool `json:"cap"`
}

var harvestETMapping map[string]elasticsearch.MappingElementTypeName

func harvestEventTypeMapping() map[string]elasticsearch.MappingElementTypeName {
	if harvestETMapping == nil {
		harvestETMapping = make(map[string]elasticsearch.MappingElementTypeName)
		harvestETMapping["imageID"] = elasticsearch.MappingElementTypeString
		harvestETMapping["acquiredDate"] = elasticsearch.MappingElementTypeString
		harvestETMapping["cloudCover"] = elasticsearch.MappingElementTypeLong
		harvestETMapping["resolution"] = elasticsearch.MappingElementTypeLong
		harvestETMapping["sensorName"] = elasticsearch.MappingElementTypeString
		harvestETMapping["minx"] = elasticsearch.MappingElementTypeLong
		harvestETMapping["miny"] = elasticsearch.MappingElementTypeLong
		harvestETMapping["maxx"] = elasticsearch.MappingElementTypeLong
		harvestETMapping["maxy"] = elasticsearch.MappingElementTypeLong
		harvestETMapping["link"] = elasticsearch.MappingElementTypeString
	}
	return harvestETMapping
}

var didOnce bool

func issueEvent(options HarvestOptions, imageID string) error {
	var (
		request    *http.Request
		response   *http.Response
		err        error
		eventBytes []byte
	)
	event := workflow.Event{
		EventTypeId: options.EventID,
		Data:        make(map[string]interface{})}
	event.Data["ImageID"] = imageID

	if eventBytes, err = json.Marshal(&event); err != nil {
		return err
	}

	requestURL := "https://pz-gateway." + domain + "/event"
	if request, err = http.NewRequest("POST", requestURL, bytes.NewBuffer(eventBytes)); err != nil {
		return err
	}
	request.Header.Set("Authorization", options.PiazzaAuthorization)
	request.Header.Set("Content-Type", "application/json")

	if response, err = pzsvc.HTTPClient().Do(request); err != nil {
		return err
	}

	// Check for HTTP errors
	if response.StatusCode < 200 || response.StatusCode > 299 {
		log.Print(requestURL)
		log.Printf("%v", string(eventBytes))
		var responseBytes []byte
		defer response.Body.Close()
		if responseBytes, err = ioutil.ReadAll(response.Body); err != nil {
			return err
		}
		return &pzsvc.HTTPError{Status: response.StatusCode, Message: "Failed to add harvest event:\n" + string(responseBytes)}
	}
	if !didOnce {
		defer response.Body.Close()
		if eventBytes, err = ioutil.ReadAll(response.Body); err != nil {
			return err
		}
		log.Print(string(eventBytes))
		didOnce = true
	}

	return err

}
func eventTypeIDHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		err       error
		eventType *workflow.EventType
	)
	pzAuth := request.Header.Get("Authorization")
	if err = testPiazzaAuth(pzAuth); err != nil {
		if httpError, ok := err.(*pzsvc.HTTPError); ok {
			http.Error(writer, httpError.Message, httpError.Status)
		} else {
			http.Error(writer, "Unable to attempt authentication: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	if eventType, err = pzsvc.EventType(harvestEventTypeRoot, harvestEventTypeMapping(), pzAuth); err == nil {
		writer.Write([]byte(eventType.EventTypeId))
	} else {
		http.Error(writer, "Failed to retrieve Event Type ID: "+err.Error(), http.StatusInternalServerError)
	}
}
