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
	"net/http"
	"os"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-lib"
)

type harvestCallback func(*geojson.FeatureCollection, HarvestOptions) error

const harvestEventTypeRoot = "beachfront:harvest:new-image-harvested"

var (
	domain = os.Getenv("DOMAIN")

	harvestEventTypeID string
)

// HarvestOptions are options for a harvesting operation
type HarvestOptions struct {
	Recurring           bool   `json:"recurring"`
	Event               bool   `json:"event"`
	Reharvest           bool   `json:"reharvest"`
	PlanetKey           string `json:"PL_API_KEY"`
	PiazzaGateway       string `json:"pzGateway"`
	PiazzaAuthorization string `json:"pz-auth"`
	callback            harvestCallback
	EventID             string
	Cap                 bool `json:"cap"`
}

var harvestETMapping map[string]interface{}

func harvestEventTypeMapping() map[string]interface{} {
	if harvestETMapping == nil {
		harvestETMapping = make(map[string]interface{})
		harvestETMapping["imageID"] = "string"
		harvestETMapping["acquiredDate"] = "string"
		harvestETMapping["cloudCover"] = "long"
		harvestETMapping["resolution"] = "long"
		harvestETMapping["sensorName"] = "string"
		harvestETMapping["minx"] = "long"
		harvestETMapping["miny"] = "long"
		harvestETMapping["maxx"] = "long"
		harvestETMapping["maxy"] = "long"
		harvestETMapping["link"] = "string"
	}
	return harvestETMapping
}

var didOnce bool

func issueEvent(options HarvestOptions, imageID string) error {
	var (
		err        error
		eventBytes []byte
	)
	event := pzsvc.Event{
		EventTypeID: options.EventID,
		Data:        make(map[string]interface{})}
	event.Data["ImageID"] = imageID

	if eventBytes, err = json.Marshal(&event); err != nil {
		return err
	}

	_, err = pzsvc.SubmitSinglePart("POST", string(eventBytes), options.PiazzaGateway+"/event", options.PiazzaAuthorization)
	return err
}
func eventTypeIDHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		err       error
		eventType pzsvc.EventType
	)
	pzGateway := request.FormValue("pzGateway")
	pzAuth := request.Header.Get("Authorization")
	if err = testPiazzaAuth(pzGateway, pzAuth); err != nil {
		if httpError, ok := err.(*pzsvc.HTTPError); ok {
			http.Error(writer, httpError.Message, httpError.Status)
		} else {
			http.Error(writer, "Unable to attempt authentication: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if eventType, err = pzsvc.GetEventType(harvestEventTypeRoot, harvestEventTypeMapping(), pzGateway, pzAuth); err == nil {
		writer.Write([]byte(eventType.EventTypeID))
	} else {
		http.Error(writer, "Failed to retrieve Event Type ID: "+err.Error(), http.StatusInternalServerError)
	}
}
