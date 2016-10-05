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
	"os"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-lib"
)

type harvestCallback func(*geojson.FeatureCollection, HarvestOptions) error

const harvestCron = "@every 1h"

const harvestEventTypeRoot = "beachfront:harvest:new-image-harvested"

var (
	domain = os.Getenv("DOMAIN")

	harvestEventTypeID string
)

// HarvestOptions are options for a harvesting operation
type HarvestOptions struct {
	Event               bool          `json:"event,omitempty"`
	Reharvest           bool          `json:"reharvest,omitempty"`
	PlanetKey           string        `json:"PL_API_KEY"`
	PiazzaGateway       string        `json:"pzGateway"`
	PiazzaAuthorization string        `json:"pzAuth"`
	Filter              HarvestFilter `json:"filter"`
	Cap                 bool          `json:"cap"`
	OptionsKey          string        `json:"optionsString,omitempty"`
	callback            harvestCallback
	EventTypeID         string
}

// HarvestFilter constrains harvesting
type HarvestFilter struct {
	WhiteList FeatureLayer `json:"whitelist"`
	BlackList FeatureLayer `json:"blacklist"`
}

// FeatureLayer describes features
type FeatureLayer struct {
	WfsURL      string `json:"wfsurl"`
	FeatureType string `json:"featureType"`
	Geometry    interface{}
	GeoJSON     interface{} `json:"geojson"`
}

func issueEvent(options HarvestOptions, feature *geojson.Feature, callback func(error)) error {
	event := pzsvc.Event{
		EventTypeID: options.EventTypeID,
		Data:        make(map[string]interface{})}
	event.Data["imageID"] = feature.ID
	event.Data["minx"] = feature.ForceBbox()[0]
	event.Data["miny"] = feature.ForceBbox()[1]
	event.Data["maxx"] = feature.ForceBbox()[2]
	event.Data["maxy"] = feature.ForceBbox()[3]
	event.Data["acquiredDate"] = feature.PropertyString("acquiredDate")
	event.Data["sensorName"] = feature.PropertyString("sensorName")
	event.Data["link"] = feature.PropertyString("path")
	event.Data["resolution"] = feature.PropertyFloat("resolution")
	event.Data["cloudCover"] = feature.PropertyFloat("cloudCover")

	_, err := pzsvc.AddEvent(event, options.PiazzaGateway, options.PiazzaAuthorization)
	if callback != nil {
		callback(err)
	}
	return err
}
