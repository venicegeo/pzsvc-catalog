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

package tides

import (
	"log"
	"math"
	"time"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-lib"
)

// Context is the context for this operation
type Context struct {
	TidesURL string
}

type tideIn struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
	Dtg string  `json:"dtg"`
}

type tidesIn struct {
	Locations []tideIn                    `json:"locations"`
	Map       map[string]*geojson.Feature `json:"-"`
}

type tideOut struct {
	MinTide  float64 `json:"minimumTide24Hours"`
	MaxTide  float64 `json:"maximumTide24Hours"`
	CurrTide float64 `json:"currentTide"`
}

type tideWrapper struct {
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	Dtg     string  `json:"dtg"`
	Results tideOut `json:"results"`
}

type out struct {
	Locations []tideWrapper `json:"locations"`
}

func toTideIn(bbox geojson.BoundingBox, timeStr string) *tideIn {
	var (
		center  *geojson.Point
		dtgTime time.Time
		err     error
	)
	if center = bbox.Centroid(); center == nil {
		return nil
	}
	if dtgTime, err = time.Parse("2006-01-02T15:04:05Z", timeStr); err != nil {
		return nil
	}
	return &tideIn{Lat: center.Coordinates[1], Lon: center.Coordinates[0], Dtg: dtgTime.Format("2006-01-02-15-04")}
}

func toTidesIn(features []*geojson.Feature) *tidesIn {
	var (
		result     tidesIn
		currTideIn *tideIn
	)
	result.Map = make(map[string]*geojson.Feature)
	for _, feature := range features {
		if feature.PropertyFloat("CurrentTide") != math.NaN() {
			if currTideIn = toTideIn(feature.ForceBbox(), feature.PropertyString("acquiredDate")); currTideIn == nil {
				log.Print(pzsvc.TraceStr(`Could not get tide information from feature ` + feature.IDStr() + ` because required elements did not exist.`))
				continue
			}
			result.Locations = append(result.Locations, *currTideIn)
			result.Map[currTideIn.Dtg] = feature
		}
	}
	switch len(result.Locations) {
	case 0:
		return nil
	default:
		return &result
	}
}

// GetTides returns the tide information for the features provided.
// Features must have a geometry and an acquiredDate property.
func GetTides(fc *geojson.FeatureCollection, context Context) (*geojson.FeatureCollection, error) {
	var (
		err          error
		tin          *tidesIn
		tout         out
		currentScene *geojson.Feature
		result       *geojson.FeatureCollection
	)
	tin = toTidesIn(fc.Features)
	features := make([]*geojson.Feature, len(fc.Features))
	if _, err = pzsvc.ReqByObjJSON("POST", context.TidesURL, "", tin, &tout); err == nil {
		for inx, tideObj := range tout.Locations {
			currentScene = tin.Map[tideObj.Dtg]
			currentScene.Properties["CurrentTide"] = tideObj.Results.CurrTide
			currentScene.Properties["24hrMinTide"] = tideObj.Results.MinTide
			currentScene.Properties["24hrMaxTide"] = tideObj.Results.MaxTide
			features[inx] = currentScene
		}
		result = geojson.NewFeatureCollection(features)
	}
	return result, err
}
