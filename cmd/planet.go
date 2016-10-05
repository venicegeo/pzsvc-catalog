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
	"log"
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
	"github.com/venicegeo/pzsvc-lib"
)

const planetRecurringRoot = "beachfront:harvest:planet-recurrence"

func planetHandler(w http.ResponseWriter, r *http.Request) {
	var (
		options       catalog.HarvestOptions
		err           error
		event         bool
		optionsString string
		eventType     pzsvc.EventType
	)
	defer r.Body.Close()
	if _, err = pzsvc.ReadBodyJSON(&options, r.Body); err != nil {
		http.Error(w, "Unable to read planet harvesting options from request: "+err.Error(), http.StatusBadRequest)
		return
	}

	if options.OptionsKey == "" {
		options.PiazzaAuthorization = r.Header.Get("Authorization")
	} else {
		// This is the only parameter that needs to be overridden from cached options
		event = options.Event
		if optionsString, err = catalog.GetKey(planetRecurringRoot + ":" + options.OptionsKey); err != nil {
			http.Error(w, "Unable to retrieve request options: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err = json.Unmarshal([]byte(optionsString), &options); err != nil {
			http.Error(w, "Unable to unmarshal stored harvesting options: "+err.Error(), http.StatusBadRequest)
			return
		}
		options.Event = event
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

	if options.Event {
		if eventType, err = pzsvc.GetEventType(harvestEventTypeRoot, harvestEventTypeMapping(), options.PiazzaGateway, options.PiazzaAuthorization); err == nil {
			options.EventTypeID = eventType.EventTypeID
		} else {
			http.Error(w, "Failed to retrieve harvest event type ID: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	go catalog.HarvestPlanet(options)
	w.Write([]byte("Harvesting started. Check back later."))
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
			http.Error(w, "Failed to retrieve harvest event type ID: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	if err = planetRecurring(r.URL, r.Host, options); err == nil {
		w.Write([]byte("Recurring harvest initialized.\n"))
	} else {
		w.Write([]byte("Faled to initialize recurring harvest: \n" + err.Error()))
	}
}

func planetRecurring(requestURL *url.URL, host string, options catalog.HarvestOptions) error {
	var (
		events        []pzsvc.Event
		eventType     pzsvc.EventType
		event         pzsvc.Event
		eventResponse pzsvc.EventResponse
		matchingEvent *pzsvc.Event
		newEvent      pzsvc.Event
		serviceIn     pzsvc.Service
		serviceOut    pzsvc.ServiceResponse
		triggerOut    pzsvc.TriggerResponse
		trigger       pzsvc.Trigger
		b             []byte
		err           error
	)
	// Register the service
	serviceIn.URL = recurringURL(requestURL, host, options.PiazzaGateway, "").String()
	serviceIn.ContractURL = "whatever"
	serviceIn.Method = "POST"
	b, _ = json.Marshal(serviceIn)
	if b, err = pzsvc.RequestKnownJSON("POST", string(b), options.PiazzaGateway+"/service", options.PiazzaAuthorization, &serviceOut); err != nil {
		return err
	}

	// Update the service with the service ID now that we have it so we can tell ourselves what it is later. Got it?
	serviceIn.URL = recurringURL(requestURL, host, options.PiazzaGateway, serviceOut.Data.ServiceID).String()
	log.Print(serviceIn.URL)
	b, _ = json.Marshal(serviceIn)
	if _, err = pzsvc.RequestKnownJSON("PUT", string(b), options.PiazzaGateway+"/service/"+serviceOut.Data.ServiceID, options.PiazzaAuthorization, &serviceOut); err != nil {
		return err
	}

	b, _ = json.Marshal(options)
	if err = catalog.SetKey(planetRecurringRoot+":"+serviceOut.Data.ServiceID, string(b)); err != nil {
		return err
	}
	// TODO: drop these keys when we drop everything else

	// Get the event type
	mapping := make(map[string]interface{})
	if eventType, err = pzsvc.GetEventType(planetRecurringRoot, mapping, options.PiazzaGateway, options.PiazzaAuthorization); err != nil {
		return pzsvc.ErrWithTrace(fmt.Sprintf("Failed to retrieve event type %v: %v", planetRecurringRoot, err.Error()))
	}

	// Is there an event?
	if events, err = pzsvc.Events(eventType.EventTypeID, options.PiazzaGateway, options.PiazzaAuthorization); err != nil {
		return pzsvc.ErrWithTrace(fmt.Sprintf("Failed to retrieve events for event type %v: %v", eventType.EventTypeID, err.Error()))
	}
	for _, event := range events {
		if event.CronSchedule == harvestCron {
			matchingEvent = &event
			break
		}
	}
	if matchingEvent == nil {
		event = pzsvc.Event{CronSchedule: harvestCron,
			EventTypeID: eventType.EventTypeID,
			Data:        make(map[string]interface{})}
		if eventResponse, err = pzsvc.AddEvent(event, options.PiazzaGateway, options.PiazzaAuthorization); err != nil {
			return pzsvc.ErrWithTrace(fmt.Sprintf("Failed to add event for event type %v: %v", eventType.EventTypeID, err.Error()))
		}
		newEvent = eventResponse.Data
		matchingEvent = &newEvent
	}
	log.Printf("Event: %v", matchingEvent.EventID)

	trigger.Name = "Beachfront Recurring Harvest"
	trigger.EventTypeID = eventType.EventTypeID
	trigger.Enabled = true
	trigger.Job.JobType.Type = "execute-service"
	trigger.Job.JobType.Data.ServiceID = serviceOut.Data.ServiceID
	trigger.Job.JobType.Data.DataInputs = make(map[string]pzsvc.DataType)
	trigger.Job.JobType.Data.DataInputs["foo"] = pzsvc.DataType{MimeType: "text/plain", Type: "text"}
	trigger.Job.JobType.Data.DataOutput = append(trigger.Job.JobType.Data.DataOutput, pzsvc.DataType{MimeType: "text/plain", Type: "text"})

	if triggerOut, err = pzsvc.AddTrigger(trigger, options.PiazzaGateway, options.PiazzaAuthorization); err != nil {
		return pzsvc.ErrWithTrace(fmt.Sprintf("Failed to add trigger %#v: %v", trigger, err.Error()))
	}
	log.Printf("Trigger: %v", triggerOut.Data.TriggerID)
	return err
}

func recurringURL(requestURL *url.URL, host, piazzaGateway, key string) *url.URL {
	var (
		result *url.URL
		err    error
	)
	if result, err = url.Parse("https://" + host + requestURL.String()); err != nil {
		log.Print(pzsvc.TraceErr(err).Error())
	}
	query := make(url.Values)
	query.Add("event", "true")
	if key != "" {
		query.Add("optionsKey", key)
	}
	result.RawQuery = query.Encode()
	return result
}

func init() {
	planetCmd.Flags().StringVarP(&planetKey, "PL_API_KEY", "p", "", "Planet Labs API Key")
}
