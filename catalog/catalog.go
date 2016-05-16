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
	"math"
	"time"

	"github.com/venicegeo/geojson-go/geojson"
)

var imageCatalogPrefix string

// SetImageCatalogPrefix sets the prefix for this instance
// when it is necessary to override the default
func SetImageCatalogPrefix(prefix string) {
	imageCatalogPrefix = prefix
}

// ImageDescriptors is the response to a Discover query
type ImageDescriptors struct {
	Count      int                        `json:"count"`
	StartIndex int                        `json:"startIndex"`
	Images     *geojson.FeatureCollection `json:"images"`
}

// GetImages returns images for the given set matching the criteria in the options
func GetImages(options *geojson.Feature) (ImageDescriptors, string) {
	var (
		result     ImageDescriptors
		bytes      []byte
		resultText string
		fc         *geojson.FeatureCollection
	)
	red, _ := RedisClient()

	bytes, _ = json.Marshal(options)
	key := imageCatalogPrefix + string(bytes)
	queryExists := client.Exists(key)
	if queryExists.Val() {
		// if queryExists.Val() && false { // temporarily disable cache
		resultText = red.Get(key).Val()
		json.Unmarshal([]byte(resultText), &result)
	} else {
		var features []*geojson.Feature
		members := client.SMembers(imageCatalogPrefix)
		for _, curr := range members.Val() {
			var (
				cid      *geojson.Feature
				idString string
				err      error
			)
			idString = red.Get(curr).Val()
			if cid, err = geojson.FeatureFromBytes([]byte(idString)); err == nil {
				if passImageDescriptor(cid, options) {
					features = append(features, cid)
				}
			}
		}
		result.Count = len(features)
		fc = geojson.NewFeatureCollection(features)
		result.Images = fc
		bytes, _ = json.Marshal(result)
		resultText = string(bytes)
		duration, _ := time.ParseDuration("24h")
		client.Set(key, resultText, duration)
	}
	return result, resultText
}

// pass returns true if the receiving object complies
// with all of the properties in the input
func passImageDescriptor(id, test *geojson.Feature) bool {
	if test == nil {
		return true
	}
	testCloudCover := test.PropertyFloat("cloudCover")
	idCloudCover := id.PropertyFloat("cloudCover")
	if testCloudCover != 0 && idCloudCover != 0 && !math.IsNaN(testCloudCover) && !math.IsNaN(idCloudCover) {
		if idCloudCover > testCloudCover {
			return false
		}
	}

	testBitDepth := test.PropertyInt("bitDepth")
	idBitDepth := id.PropertyInt("bitDepth")
	if testBitDepth != 0 && idBitDepth != 0 && (idBitDepth < testBitDepth) {
		return false
	}

	testBeachfrontScore := test.PropertyFloat("beachfrontScore")
	idBeachfrontScore := id.PropertyFloat("beachfrontScore")
	if testBeachfrontScore != 0 && idBeachfrontScore != 0 && !math.IsNaN(testBeachfrontScore) && !math.IsNaN(idBeachfrontScore) {
		if idBeachfrontScore < testBeachfrontScore {
			return false
		}
	}

	testAcquiredDate := test.PropertyString("acquiredDate")
	idAcquiredDate := id.PropertyString("acquiredDate")
	if testAcquiredDate != "" {
		var (
			idTime, testTime time.Time
			err              error
		)
		if idTime, err = time.Parse(time.RFC3339, idAcquiredDate); err == nil {
			if testTime, err = time.Parse(time.RFC3339, testAcquiredDate); err == nil {
				if idTime.Before(testTime) {
					return false
				}
			}
		}
	}

	testBands := test.PropertyStringSlice("bands")
	idBands := id.PropertyStringSlice("bands")
	var bandsIntersection []string
	if len(testBands) > 0 {
		for _, idBand := range idBands {
			for _, testBand := range testBands {
				if idBand == testBand {
					bandsIntersection = append(bandsIntersection, idBand)
					break
				}
			}
		}
		if len(bandsIntersection) < len(testBands) {
			return false
		}
	}

	if (len(test.Bbox) > 0) && !test.Bbox.Overlaps(id.Bbox) {
		return false
	}
	return true
}

// HTTPError represents any HTTP error
type HTTPError struct {
	Status  int
	Message string
}

func (err HTTPError) Error() string {
	return fmt.Sprintf("%d: %v", err.Status, err.Message)
}

// StoreFeature stores a feature into the catalog
// using a key based on the feature's ID
func StoreFeature(feature *geojson.Feature) {
	rc, _ := RedisClient()
	bytes, _ := json.Marshal(feature)
	key := imageCatalogPrefix + ":" + feature.ID
	rc.Set(key, string(bytes), 0)
	rc.SAdd(imageCatalogPrefix, key)
}
