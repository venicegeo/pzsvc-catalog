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
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
	"github.com/venicegeo/pzsvc-image-catalog/planet"
	"github.com/venicegeo/pzsvc-lib"
)

func planetHandler(w http.ResponseWriter, r *http.Request) {
	var (
		options   catalog.HarvestOptions
		err       error
		eventType pzsvc.EventType
		eventID   string
		triggerID string
	)
	defer r.Body.Close()
	if _, err = pzsvc.ReadBodyJSON(&options, r.Body); err != nil {
		http.Error(w, "Unable to read planet harvesting options from request: "+err.Error(), http.StatusBadRequest)
		return
	}

	options.URLRoot = r.Host

	options.PiazzaAuthorization = r.Header.Get("Authorization")

	// Let's test the credentials before we do anything else
	if err = pzsvc.TestPiazzaAuth(options.PiazzaGateway, options.PiazzaAuthorization); err != nil {
		if httpError, ok := err.(*pzsvc.HTTPError); ok {
			http.Error(w, httpError.Message, httpError.Status)
		} else {
			http.Error(w, "Unable to attempt authentication: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	if options.Event {
		if eventType, err = pzsvc.GetEventType(harvestEventTypeRoot, harvestEventTypeMapping(), options.PiazzaGateway, options.PiazzaAuthorization); err == nil {
			options.EventTypeID = eventType.EventTypeID
		} else {
			http.Error(w, "Failed to retrieve harvest event type ID: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err = options.Filter.PrepareGeometries(); err == nil {
	} else {
		http.Error(w, "Failed to prepare geometries for harvesting filter: "+err.Error(), http.StatusBadRequest)
		return
	}

	go catalog.HarvestPlanet(options)
	w.Write([]byte("Harvesting started. Check back later."))
	if options.Recurring {
		if eventID, triggerID, err = catalog.PlanetRecurring(r.Host, options); err == nil {
			w.Write([]byte("Recurring harvest initialized.\nEvent ID: " + eventID + "\nTrigger ID:" + triggerID))
		} else {
			http.Error(w, "Failed to initialize recurring harvest: \n"+err.Error(), http.StatusBadRequest)
		}
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
		options := catalog.HarvestOptions{PlanetKey: planetKey}
		catalog.HarvestPlanet(options)
	},
}

func planetRecurringHandler(w http.ResponseWriter, r *http.Request) {
	var (
		options       catalog.HarvestOptions
		err           error
		eventType     pzsvc.EventType
		optionsString string
	)
	vars := mux.Vars(r)
	key := vars["key"]

	// Pull cached options from storage
	if optionsString, err = catalog.GetKey(key); err != nil {
		if err.Error() == "redis: nil" {
			http.Error(w, fmt.Sprintf("Request options not found at %v.", key), http.StatusNotFound)
		} else {
			http.Error(w, "Unable to retrieve request options: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if err = json.Unmarshal([]byte(optionsString), &options); err != nil {
		http.Error(w, "Unable to unmarshal stored harvesting options: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Let's test the credentials before we do anything else
	if err = pzsvc.TestPiazzaAuth(options.PiazzaGateway, options.PiazzaAuthorization); err != nil {
		if httpError, ok := err.(*pzsvc.HTTPError); ok {
			http.Error(w, httpError.Message, httpError.Status)
		} else {
			http.Error(w, "Unable to attempt authentication: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	switch r.Method {
	case "GET", "POST":

		// Event is the only parameter that needs to be overridden from cached options
		options.Event, _ = strconv.ParseBool(r.FormValue("event"))

		if options.Event {
			if eventType, err = pzsvc.GetEventType(harvestEventTypeRoot, harvestEventTypeMapping(), options.PiazzaGateway, options.PiazzaAuthorization); err == nil {
				options.EventTypeID = eventType.EventTypeID
			} else {
				http.Error(w, "Failed to retrieve harvest event type ID: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		if err = options.Filter.PrepareGeometries(); err == nil {
		} else {
			http.Error(w, "Failed to prepare geometries for harvesting filter: "+err.Error(), http.StatusBadRequest)
			return
		}

		go catalog.HarvestPlanet(options)
		w.Write([]byte("Recurring harvest started.\n"))
	case "DELETE":
		if err = catalog.DeleteRecurring(key); err == nil {
			w.Write([]byte("Key " + key + " removed.\n"))
		} else {
			http.Error(w, "Failed to remove recurring harvest: "+err.Error(), http.StatusBadRequest)
		}
	default:
		http.Error(w, "Operation "+r.Method+" not allowed.", http.StatusMethodNotAllowed)
	}
}

func activatePlanetHandler(writer http.ResponseWriter, r *http.Request) {
	var (
		err     error
		context planet.RequestContext
		result  []byte
	)
	vars := mux.Vars(r)
	id := vars["id"]
	if id == "" {
		http.Error(writer, "This operation requires a Planet Labs image ID.", http.StatusBadRequest)
		return
	}
	context.PlanetKey = r.FormValue("PL_API_KEY")

	if context.PlanetKey == "" {
		http.Error(writer, "This operation requires a Planet Labs API key.", http.StatusBadRequest)
		return
	}

	if result, err = planet.Activate(id, context); err == nil {
		writer.Write(result)
	} else {
		http.Error(writer, "Failed to acquire activation information for "+id+": "+err.Error(), http.StatusBadRequest)
	}
}

func init() {
	planetCmd.Flags().StringVarP(&planetKey, "PL_API_KEY", "p", "", "Planet Labs API Key")
}
