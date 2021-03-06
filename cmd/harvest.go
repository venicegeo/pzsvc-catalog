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
	"log"
	"net/http"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
	"github.com/venicegeo/pzsvc-lib"
)

const harvestCron = "@every 1h"

const harvestEventTypeRoot = "beachfront:harvest:new-image-harvested"

var (
	harvestEventTypeID string
)

var harvestETMapping map[string]interface{}

func harvestEventTypeMapping() map[string]interface{} {
	if harvestETMapping == nil {
		harvestETMapping = make(map[string]interface{})
		harvestETMapping["imageID"] = "string"
		harvestETMapping["acquiredDate"] = "string"
		harvestETMapping["cloudCover"] = "double"
		harvestETMapping["resolution"] = "double"
		harvestETMapping["sensorName"] = "string"
		harvestETMapping["minx"] = "double"
		harvestETMapping["miny"] = "double"
		harvestETMapping["maxx"] = "double"
		harvestETMapping["maxy"] = "double"
		harvestETMapping["link"] = "string"
	}
	return harvestETMapping
}

func eventTypeIDHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		err       error
		eventType pzsvc.EventType
	)
	if pzsvc.Preflight(writer, request) {
		return
	}

	pzGateway := request.FormValue("pzGateway")
	if pzGateway == "" {
		http.Error(writer, "pzGateway is required", http.StatusBadRequest)
		return
	}
	pzAuth := request.Header.Get("Authorization")
	if pzAuth == "" {
		writer.Header().Set("WWW-Authenticate", `Basic realm="MY REALM"`)
		http.Error(writer, "401 Unauthorized", http.StatusUnauthorized)
		return
	}
	if err = pzsvc.TestPiazzaAuth(pzGateway, pzAuth); err != nil {
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

func unharvestHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		err     error
		options *catalog.SearchOptions
		sf      *geojson.Feature
		scenes  catalog.SceneDescriptors
		successes,
		failures int
	)
	if pzsvc.Preflight(writer, request) {
		return
	}

	pzGateway := request.FormValue("pzGateway")
	pzAuth := request.Header.Get("Authorization")
	if err = pzsvc.TestPiazzaAuth(pzGateway, pzAuth); err != nil {
		if httpError, ok := err.(*pzsvc.HTTPError); ok {
			http.Error(writer, httpError.Message, httpError.Status)
		} else {
			http.Error(writer, "Unable to attempt authentication: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	if options, err = searchOptions(request); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	// Never want to involve the cache in the search request
	options.NoCache = true
	// Never want to cap the number of scenes to remove
	options.Count = 0
	if sf, err = searchFeature(request); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	if (sf.PropertyString("acquiredDate") == "") &&
		(sf.PropertyString("maxAcquiredDate") == "") {
		http.Error(writer, "An unharvest request must contain at least one of the following:\n* acquiredDate\n* maxAcquiredDate", http.StatusBadRequest)
		return
	}
	if scenes, _, err = catalog.GetScenes(sf, *options); err == nil {
		if scenes.Scenes == nil {
			log.Printf("nil Scenes")
		} else if scenes.Scenes.Features == nil {
			log.Printf("nil Features")
		}
		for _, scene := range scenes.Scenes.Features {
			if err = catalog.RemoveFeature(scene); err == nil {
				successes++
			} else {
				failures++
				log.Printf("Failed to remove scene %v: %v", scene.ID, err.Error())
			}
		}
		writer.Header().Set("Content-Type", "text/plain")
		writer.Write([]byte(fmt.Sprintf("Removed %v scenes; %v failed.", successes, failures)))
	} else {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}
