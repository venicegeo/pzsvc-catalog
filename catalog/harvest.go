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
	"math"

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
	RequestPageSize     int           `json:"requestPageSize"`
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
	WfsURL      string                    `json:"wfsurl"`
	FeatureType string                    `json:"featureType"`
	GeoJSON     map[string]interface{}    `json:"geojson"`
	TileMap     map[string]*geos.Geometry `json:"-"`
}

func issueEvent(options HarvestOptions, feature *geojson.Feature, callback func(error)) error {
	event := pzsvc.Event{
		EventTypeID: options.EventTypeID,
		Data:        make(map[string]interface{})}
	event.Data["imageID"] = feature.IDStr()
	bbox := feature.ForceBbox()
	if (bbox.Valid() == nil) && (len(bbox) > 3) {
		event.Data["minx"] = feature.ForceBbox()[0]
		event.Data["miny"] = feature.ForceBbox()[1]
		event.Data["maxx"] = feature.ForceBbox()[2]
		event.Data["maxy"] = feature.ForceBbox()[3]
	} else {
		log.Printf("Failed to receive a valid bounding box for feature %v: %v", feature.IDStr(), feature.String())
	}
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
	if err := hf.WhiteList.PrepareGeometries(); err != nil {
		return err
	}
	if len(hf.WhiteList.TileMap) == 0 {
		hf.WhiteList.TileMap = make(map[string]*geos.Geometry)
		hf.WhiteList.TileMap["000000"] = wholeWorld()
	}
	return hf.BlackList.PrepareGeometries()
}

// PrepareGeometries establishes Geos geometries for later processing,
// Returning an error on failure
func (fl *FeatureLayer) PrepareGeometries() error {
	var (
		err error
		fc  *geojson.FeatureCollection
	)
	if fl.TileMap == nil {
		if fl.GeoJSON == nil {
			if fl.WfsURL == "" {
				fc = geojson.NewFeatureCollection(nil)
			} else {
				if fc, err = geojson.FromWFS(fl.WfsURL, fl.FeatureType); err != nil {
					return err
				}
			}
		} else {
			log.Printf("GeoJSON: %#v", fl.GeoJSON)
			fc = geojson.FeatureCollectionFromMap(fl.GeoJSON)
			log.Printf("FC: %#v", fc.String())
		}
		if fl.TileMap, err = tilemapFeatures(fc.Features); err != nil {
			return err
		}
	}
	return nil
}

func passHarvestFilter(options HarvestOptions, feature *geojson.Feature) bool {
	var (
		harvestGeom *geos.Geometry
		err         error
		disjoint    bool
		intersects  bool
	)
	if harvestGeom, err = geojsongeos.GeosFromGeoJSON(feature); err != nil {
		log.Printf("Harvest geometry cannot be parsed. Dropping from harvest. %v", err.Error())
		return false
	}

	if intersects, err = options.Filter.BlackList.Intersects(harvestGeom); err != nil || intersects {
		return false
	}
	if disjoint, err = options.Filter.WhiteList.Disjoint(harvestGeom); err != nil || disjoint {
		return false
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
	fmt.Printf("Attempting to register recurring key of %v", key)
	if r1 := red.SAdd(recurringRoot, key); r1.Err() != nil {
		return r1.Err()
	}
	r2 := red.Set(key, string(b), 0)
	return r2.Err()
}

func wholeWorld() *geos.Geometry {
	shell, _ := geos.NewLinearRing(
		geos.Coord{X: -180, Y: -90},
		geos.Coord{X: 180, Y: -90},
		geos.Coord{X: 180, Y: 90},
		geos.Coord{X: -180, Y: 90},
		geos.Coord{X: -180, Y: -90})
	result, _ := geos.PolygonFromGeom(shell)
	return result
}

func tilemapFeatures(features []*geojson.Feature) (map[string]*geos.Geometry, error) {
	var (
		tiledGeometries [180 * 360][]*geos.Geometry
		geometry        *geos.Geometry
		err             error
	)

	// Put each feature's geometry in the bucket for the right tile
	for _, feature := range features {
		bbox := feature.ForceBbox()
		lonIndex := int(math.Floor(bbox[0]) + 180)
		latIndex := int(math.Floor(bbox[1]) + 90)
		index := lonIndex + (360 * latIndex)
		if geometry, err = geojsongeos.GeosFromGeoJSON(feature); err != nil {
			return nil, err
		}
		tiledGeometries[index] = append(tiledGeometries[index], geometry)
	}
	return combineGeometries(tiledGeometries), nil
}

func combineGeometries(tiles [180 * 360][]*geos.Geometry) map[string]*geos.Geometry {
	result := make(map[string]*geos.Geometry)

	for index, tgs := range tiles {
		latIndex := int(math.Floor(float64(index) / 180.0))
		lonIndex := int(math.Mod(float64(index), 180))
		key := fmt.Sprintf("%03d%03d", lonIndex, latIndex)
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered %v\n%v", r, tgs)
			}
		}()
		switch len(tgs) {
		case 0:
			//noop
			// log.Printf("Index %v was empty.", index)
		case 1:
			result[key] = tgs[0]
			// result[key] = geos.PrepareGeometry(tgs[0])
		default:
			if geometry, err := geos.NewCollection(geos.GEOMETRYCOLLECTION, tgs[:]...); err == nil {
				if geometry, err = geometry.Buffer(0.0); err == nil {
					result[key] = geometry
					// result[key] = geos.PrepareGeometry(geometry)
				} else {
					log.Printf("Received %v when buffering geometry for %v. Continuing.", err.Error(), index)
					continue
				}
			} else {
				log.Printf("Received %v when creating a collection for %v. Continuing", err.Error(), index)
				continue
			}
		}
	}
	return result
}

// Intersects returns true if the layer is disjoint with the geometry provided
func (fl *FeatureLayer) Intersects(input *geos.Geometry) (bool, error) {
	var (
		intersects bool
		err        error
	)
	for _, geom := range fl.TileMap {
		if intersects, err = geom.Intersects(input); err == nil {
			if intersects {
				return true, nil
			}
		} else {
			return false, err
		}
	}
	return false, nil
}

// Disjoint returns true if the layer is disjoint with the geometry provided
func (fl *FeatureLayer) Disjoint(input *geos.Geometry) (bool, error) {
	var (
		disjoint bool
		err      error
	)
	for _, geom := range fl.TileMap {
		if disjoint, err = geom.Disjoint(input); err == nil {
			if !disjoint {
				return false, nil
			}
		} else {
			return false, err
		}
	}
	return true, nil
}
