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

	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
	"github.com/venicegeo/pzsvc-lib"
)

func planetHandler(w http.ResponseWriter, r *http.Request) {
	var (
		options   catalog.HarvestOptions
		err       error
		eventType pzsvc.EventType
	)
	defer r.Body.Close()
	if _, err = pzsvc.ReadBodyJSON(&options, r.Body); err != nil {
		http.Error(w, "Unable to read planet harvesting options from request: "+err.Error(), http.StatusBadRequest)
		return
	}

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
		if err = catalog.PlanetRecurring(r.Host, options); err == nil {
			w.Write([]byte("Recurring harvest initialized.\n"))
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
		event         bool
		optionsString string
	)
	vars := mux.Vars(r)
	key := vars["id"]
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

		defer r.Body.Close()
		if _, err = pzsvc.ReadBodyJSON(&options, r.Body); err != nil {
			http.Error(w, "Unable to read planet harvesting options from request: "+err.Error(), http.StatusBadRequest)
			return
		}

		// This is the only parameter that needs to be overridden from cached options
		event = options.Event
		if optionsString, err = catalog.GetKey(key); err != nil {
			http.Error(w, "Unable to retrieve request options: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err = json.Unmarshal([]byte(optionsString), &options); err != nil {
			http.Error(w, "Unable to unmarshal stored harvesting options: "+err.Error(), http.StatusBadRequest)
			return
		}
		options.Event = event

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

func init() {
	planetCmd.Flags().StringVarP(&planetKey, "PL_API_KEY", "p", "", "Planet Labs API Key")
}
