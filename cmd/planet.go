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
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
	"github.com/venicegeo/pzsvc-lib"
)

const planetRecurringRoot = "beachfront:harvest:planet-recurrence"

func planetHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		drop          bool
		event         bool
		recurring     bool
		err           error
		eventType     pzsvc.EventType
		options       HarvestOptions
		optionsString string
	)
	optionsKey := request.FormValue("optionsKey")
	if optionsKey == "" {
		options.PiazzaGateway = request.FormValue("pzGateway")
		options.Reharvest, _ = strconv.ParseBool(request.FormValue("reharvest"))
		options.PlanetKey = request.FormValue("PL_API_KEY")
		options.PiazzaAuthorization = request.Header.Get("Authorization")
		options.Cap, _ = strconv.ParseBool(request.FormValue("cap"))
		recurring, _ = strconv.ParseBool(request.FormValue("recurring"))
	} else {
		if optionsString, err = catalog.GetKey(planetRecurringRoot + ":" + optionsKey); err != nil {
			http.Error(writer, "Unable to retrieve requestion options: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err = json.Unmarshal([]byte(optionsString), &options); err != nil {
			http.Error(writer, "Unable to unmarshal request options: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	if options.PiazzaGateway == "" {
		http.Error(writer, "This request requires a 'pzGateway'.", http.StatusBadRequest)
		return
	}

	// This is the only parameter that needs to be overridden from cached options
	event, _ = strconv.ParseBool(request.FormValue("event"))
	options.Event = event

	// Let's test the credentials before we do anything
	if err = testPiazzaAuth(options.PiazzaGateway, options.PiazzaAuthorization); err != nil {
		if httpError, ok := err.(*pzsvc.HTTPError); ok {
			http.Error(writer, httpError.Message, httpError.Status)
		} else {
			http.Error(writer, "Unable to attempt authentication: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	if event {
		if eventType, err = pzsvc.GetEventType(harvestEventTypeRoot, harvestEventTypeMapping(), options.PiazzaGateway, options.PiazzaAuthorization); err == nil {
			options.EventID = eventType.EventTypeID
		} else {
			http.Error(writer, "Failed to retrieve harvest event type ID: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	if drop, _ = strconv.ParseBool(request.FormValue("dropIndex")); drop {
		writer.Write([]byte("Dropping existing index.\n"))
		catalog.DropIndex()
	}

	if recurring {
		writer.Write([]byte("Initializing recurring harvest.\n"))
		if err = planetRecurring(request.URL, request.Host, options); err != nil {
			writer.Write([]byte("\n" + err.Error()))
		}
	}
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
		if options.Cap {
			break
		}
	}
	if err != nil {
		log.Print(err.Error())
	}
	log.Printf("Harvested %v images.", catalog.IndexSize())
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
		if _, err = catalog.StoreFeature(feature, options.Reharvest); err != nil {
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

const harvestCron = "@every 1m"

// const harvestCron = "@every 24h"

func harvestPlanet(options HarvestOptions) {
	// harvestPlanetEndpoint("v0/scenes/ortho/?count=1000", storePlanetOrtho)
	options.callback = storePlanetLandsat
	harvestPlanetEndpoint("v0/scenes/landsat/?count=1000", options)
	// harvestPlanetEndpoint("v0/scenes/rapideye/?count=1000", storePlanetRapidEye)
}

func planetRecurring(requestURL *url.URL, host string, options HarvestOptions) error {
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
	log.Printf("%#v", serviceOut)

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
		log.Printf("created new event %#v", newEvent)
		matchingEvent = &newEvent
	}
	log.Printf("Event: %v", matchingEvent.EventID)

	trigger.Name = "Beachfront Recurring Harvest"
	trigger.EventTypeID = eventType.EventTypeID
	trigger.Enabled = true
	trigger.Job.JobType.Type = "execute-service"
	trigger.Job.JobType.Data.ServiceID = serviceOut.Data.ServiceID
	trigger.Job.JobType.Data.DataInputs = make(map[string]pzsvc.DataType)
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
