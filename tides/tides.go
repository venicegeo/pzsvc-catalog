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

// TideOut is a single tide result
type TideOut struct {
	MinTide  float64 `json:"minimumTide24Hours"`
	MaxTide  float64 `json:"maximumTide24Hours"`
	CurrTide float64 `json:"currentTide"`
}

// TideWrapper is the object that wraps a single tide result
type TideWrapper struct {
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	Dtg     string  `json:"dtg"`
	Results TideOut `json:"results"`
}

// Out is the output of a GetTides operation
type Out struct {
	Locations []TideWrapper `json:"locations"`
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
func GetTides(features []*geojson.Feature, context Context) (*Out, error) {
	var (
		err    error
		result *Out
	)
	if _, err = pzsvc.ReqByObjJSON("POST", context.TidesURL, "", toTidesIn(features), result); err == nil {
		return result, nil
	}
	return nil, err

}
