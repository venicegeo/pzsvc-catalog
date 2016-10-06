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
	"encoding/json"
	"fmt"
	"log"

	"github.com/paulsmith/gogeos/geos"
	"github.com/venicegeo/geojson-geos-go/geojsongeos"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-lib"
)

type harvestCallback func(*geojson.FeatureCollection, HarvestOptions) (int, error)

const recurringRoot = "beachfront:harvest:recurrence"

const harvestCron = "@every 1h"

const harvestEventTypeRoot = "beachfront:harvest:new-image-harvested"

var (
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
	Cap                 int           `json:"cap"`
	Recurring           bool          `json:"recurring"`
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
	WfsURL      string                 `json:"wfsurl"`
	FeatureType string                 `json:"featureType"`
	GeoJSON     map[string]interface{} `json:"geojson"`
	Geos        *geos.Geometry
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

// PrepareGeometries establishes Geos geometries for later processing,
// Returning an error on failure
func (hf *HarvestFilter) PrepareGeometries() error {
	var (
		err error
	)

	if hf.WhiteList.GeoJSON != nil {
		if hf.WhiteList.Geos, err = geojsongeos.GeosFromGeoJSON(geojson.FeatureCollectionFromMap(hf.WhiteList.GeoJSON)); err != nil {
			return pzsvc.ErrWithTrace(fmt.Sprintf("Whitelist filter geometry cannot be parsed. %v", err.Error()))
		}
	}
	if hf.BlackList.GeoJSON != nil {
		if hf.BlackList.Geos, err = geojsongeos.GeosFromGeoJSON(geojson.FeatureCollectionFromMap(hf.BlackList.GeoJSON)); err != nil {
			return pzsvc.ErrWithTrace(fmt.Sprintf("Blacklist filter geometry cannot be parsed. %v", err.Error()))
		}
	}
	return err
}

func passHarvestFilter(options HarvestOptions, feature *geojson.Feature) bool {
	var (
		harvestGeom *geos.Geometry
		err         error
		disjoint    bool
	)
	if harvestGeom, err = geojsongeos.GeosFromGeoJSON(feature); err != nil {
		log.Printf("Harvest geometry cannot be parsed. Dropping from harvest. %v", err.Error())
		return false
	}
	if options.Filter.WhiteList.Geos != nil {
		if disjoint, err = harvestGeom.Disjoint(options.Filter.WhiteList.Geos); err != nil || disjoint {
			return false
		}
	}
	if options.Filter.BlackList.Geos != nil {
		if disjoint, err = harvestGeom.Disjoint(options.Filter.BlackList.Geos); err != nil || !disjoint {
			return false
		}
	}
	return true
}

// DeleteRecurring removes all trace of a recurring harvest from storage
func DeleteRecurring(key string) error {
	red, _ := RedisClient()
	if s1 := red.SIsMember(recurringRoot, key); !s1.Val() {
		return pzsvc.ErrWithTrace("Key " + key + " is not a recurring harvest.")
	}
	red.SRem(recurringRoot, key)
	red.Del(key)
	return nil
}

// StoreRecurring adds the details of a recurring harvest to storage for later retrieval
func StoreRecurring(key string, options HarvestOptions) error {
	red, _ := RedisClient()
	b, _ := json.Marshal(options)
	if r1 := red.SAdd(recurringRoot, key); r1.Err() != nil {
		return r1.Err()
	}
	r2 := red.Set(key, string(b), 0)
	return r2.Err()
}
