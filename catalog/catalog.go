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
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"gopkg.in/redis.v3"

	"github.com/paulsmith/gogeos/geos"
	"github.com/venicegeo/geojson-geos-go/geojsongeos"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-lib"
)

const maxCacheSize = 1000
const maxCacheTimeout = "1h"

var imageCatalogPrefix string

// SearchOptions is the options for a search request
type SearchOptions struct {
	NoCache      bool
	MinimumIndex int
	MaximumIndex int
	Count        int
	Rigorous     bool
}

// SetImageCatalogPrefix sets the prefix for this instance
// when it is necessary to override the default
func SetImageCatalogPrefix(prefix string) {
	imageCatalogPrefix = prefix
}

// SceneDescriptors is the response to a Discover query
type SceneDescriptors struct {
	Count      int                        `json:"count"`
	TotalCount int                        `json:"totalCount"`
	StartIndex int                        `json:"startIndex"`
	SubIndex   string                     `json:"subIndex"`
	Scenes     *geojson.FeatureCollection `json:"images"` // Changing this to "scenes" may break clients
}

// IndexSize returns the size of the index
func IndexSize() int64 {
	rc, _ := RedisClient()
	result := rc.ZCard(imageCatalogPrefix)
	return result.Val()
}

// GetScenes returns scenes for the given set matching the criteria in the input and options
func GetScenes(input *geojson.Feature, options SearchOptions) (SceneDescriptors, string, error) {

	var (
		result     SceneDescriptors
		resultText string
		fc         *geojson.FeatureCollection
		features   []*geojson.Feature
		ssc        *redis.StringSliceCmd
		sc         *redis.StringCmd
		ic         *redis.IntCmd
	)
	if input == nil {
		return result, "", pzsvc.ErrWithTrace("Input feature must not be nil.")
	}
	if options.NoCache {
		return getResults(input, options)
	}

	features = make([]*geojson.Feature, 0)
	red, _ := RedisClient()
	cacheName := getDiscoverCacheName(input)

	// If the cache does not exist, create it asynchronously
	if cacheExists := red.Exists(cacheName); cacheExists.Err() == nil {
		if !cacheExists.Val() {
			go populateCache(input, cacheName)
		}
	} else {
		RedisError(red, cacheExists.Err())
		return result, "", cacheExists.Err()
	}

	// See if we can complete the requested query
	complete := false
	for !complete {
		complete = completeCache(cacheName, options)
		if !complete {
			time.Sleep(100 * time.Millisecond)
		}
	}

	if ssc = red.ZRange(cacheName, int64(options.MinimumIndex), int64(options.MaximumIndex)); ssc.Err() == nil {
		var (
			cid *geojson.Feature
		)
		for _, curr := range ssc.Val() {
			if curr == "" {
				// This is the terminal element - ignore it.
				continue
			}
			if sc = red.Get(curr); sc.Err() == nil {
				cid, _ = geojson.FeatureFromBytes([]byte(sc.Val()))
				features = append(features, cid)
			} else {
				if sc.Err().Error() == "redis: nil" {
					log.Printf("Member %v was found in cache %v but doesn't exist; removing from cache.", curr, cacheName)
					red.ZRem(cacheName, curr)
					continue
				} else {
					RedisError(red, sc.Err())
					return result, "", sc.Err()
				}
			}
		}

		result.SubIndex = cacheName
		result.Count = len(features)
		if ic = red.ZCard(cacheName); ic.Err() == nil {
			result.TotalCount = int(ic.Val())
			// This implies we have a terminal element
			if result.TotalCount > result.Count {
				result.TotalCount--
			}
		}
		result.StartIndex = options.MinimumIndex
		fc = geojson.NewFeatureCollection(features)
		result.Scenes = fc
		bytes, _ := json.Marshal(result)
		resultText = string(bytes)
		return result, resultText, nil
	}

	RedisError(red, ssc.Err())
	return result, "", ssc.Err()
}

// getDiscoverCacheName returns the name of the index corresponding
// to the search criteria provided
func getDiscoverCacheName(input *geojson.Feature) string {
	bytes, _ := json.Marshal(input)
	// TODO: we may wish to hash this index name
	return imageCatalogPrefix + string(bytes)
}

func completeCache(cacheName string, options SearchOptions) bool {
	complete := false
	red, _ := RedisClient()
	cardCmd := red.ZCard(cacheName)
	card := int(cardCmd.Val())
	totalCount := card - 1 // ignore terminal element
	if cardCmd.Err() != nil {
		RedisError(red, cardCmd.Err())
		complete = true
		// See we have enough results already
	} else if totalCount > options.MaximumIndex {
		complete = true
		// See if the terminal object has been added
	} else {
		var zrbs redis.ZRangeByScore
		zrbs.Min = "0.5"
		zrbs.Max = "1.5"
		ssc := red.ZRangeByScore(cacheName, zrbs)
		if ssc.Err() != nil {
			RedisError(red, ssc.Err())
			complete = true
		} else if len(ssc.Val()) > 0 {
			complete = true
		}
	}
	return complete
}

// getResults returns the results of the requested query without the caching mechanism
func getResults(input *geojson.Feature, options SearchOptions) (SceneDescriptors, string, error) {
	var (
		members         *redis.StringSliceCmd
		cid             *geojson.Feature
		result          SceneDescriptors
		idCmd           *redis.StringCmd
		indexName       string
		features        []*geojson.Feature
		fc              *geojson.FeatureCollection
		acquiredDate    time.Time
		maxAcquiredDate time.Time
		err             error
		red             *redis.Client
	)
	if red, err = RedisClient(); err != nil {
		return result, "", pzsvc.TraceErr(err)
	}

	if acquiredDateStr := input.PropertyString("acquiredDate"); acquiredDateStr != "" {
		if acquiredDate, err = time.Parse(time.RFC3339, acquiredDateStr); err != nil {
			log.Printf("Invalid date %v", acquiredDateStr)
		}
	}

	if maxAcquiredDateStr := input.PropertyString("maxAcquiredDate"); maxAcquiredDateStr != "" {
		if maxAcquiredDate, err = time.Parse(time.RFC3339, maxAcquiredDateStr); err != nil {
			log.Printf("Invalid date %v", maxAcquiredDateStr)
		}
	}

	if subIndex := input.PropertyString("subIndex"); subIndex == "" {
		indexName = imageCatalogPrefix
	} else {
		indexName = subIndex
	}

	if acquiredDate.IsZero() && maxAcquiredDate.IsZero() {
		// Create the cache using a full table scan
		log.Printf("Starting search on %v with no dates", indexName)
		if members = red.ZRange(indexName, 0, -1); members.Err() != nil {
			return result, "", pzsvc.TraceErr(members.Err())
		}
	} else {
		// Use the score to limit the result set
		var opt redis.ZRangeByScore
		if maxAcquiredDate.IsZero() {
			maxAcquiredDate = time.Now()
		}
		opt.Max = strconv.FormatInt(-acquiredDate.Unix(), 10)
		opt.Min = strconv.FormatInt(-maxAcquiredDate.Unix(), 10)
		log.Printf("Starting search on %v: %v", indexName, opt)
		if members = red.ZRangeByScore(indexName, opt); members.Err() != nil {
			return result, "", pzsvc.TraceErr(members.Err())
		}
	}

	for _, curr := range members.Val() {
		// First look at the key - we can often save time by not retrieving the value at all
		if passImageDescriptorKey(curr, input) {
			if idCmd = red.Get(curr); idCmd.Err() != nil {
				if idCmd.Err().Error() == "redis: nil" {
					log.Printf("Key %v was found in cache %v but doesn't exist. Removing", curr, indexName)
					red.ZRem(indexName, curr)
					continue
				} else {
					return result, "", pzsvc.TraceErr(idCmd.Err())
				}
			}
			if cid, err = geojson.FeatureFromBytes([]byte(idCmd.Val())); err == nil {
				if passImageDescriptor(cid, input, options.Rigorous) {
					features = append(features, cid)
					if options.Count > 0 && (len(features) >= options.Count) {
						break
					}
				}
			} else {
				return result, "", pzsvc.TraceErr(err)
			}
		}
	}

	fc = geojson.NewFeatureCollection(features)
	result.Scenes = fc
	result.Count = len(fc.Features)
	result.StartIndex = options.MinimumIndex

	bytes, _ := json.Marshal(result)
	return result, string(bytes), nil
}

// populateCache populates a cache corresponding
// to the search criteria provided
func populateCache(input *geojson.Feature, cacheName string) {
	red, _ := RedisClient()
	var (
		cid             *geojson.Feature
		idString        string
		indexName       string
		err             error
		z               redis.Z
		members         *redis.StringSliceCmd
		count           int
		acquiredDate    time.Time
		maxAcquiredDate time.Time
	)

	// registerCache(cacheName)

	if acquiredDateStr := input.PropertyString("acquiredDate"); acquiredDateStr != "" {
		if acquiredDate, err = time.Parse(time.RFC3339, acquiredDateStr); err != nil {
			log.Printf("Invalid date %v", acquiredDateStr)
		}
	}

	if maxAcquiredDateStr := input.PropertyString("maxAcquiredDate"); maxAcquiredDateStr != "" {
		if maxAcquiredDate, err = time.Parse(time.RFC3339, maxAcquiredDateStr); err != nil {
			log.Printf("Invalid date %v", maxAcquiredDateStr)
		}
	}

	// if subIndex := input.PropertyString("subIndex"); subIndex == "" {
	indexName = imageCatalogPrefix
	// } else {
	// 	indexName = subIndex
	// }

	if acquiredDate.IsZero() && maxAcquiredDate.IsZero() {
		// Create the cache using a full table scan
		if members = red.ZRange(indexName, 0, -1); members.Err() != nil {
			RedisError(red, members.Err())
		}
	} else {
		// Use the score to limit the result set
		var opt redis.ZRangeByScore
		if maxAcquiredDate.IsZero() {
			maxAcquiredDate = time.Now()
		}
		opt.Max = strconv.FormatInt(-acquiredDate.Unix(), 10)
		opt.Min = strconv.FormatInt(-maxAcquiredDate.Unix(), 10)
		if members = red.ZRangeByScore(indexName, opt); members.Err() != nil {
			RedisError(red, members.Err())
		}
	}

	for _, curr := range members.Val() {
		if passImageDescriptorKey(curr, input) {
			// If there are no test properties, there is no point in inspecting the contents
			if len(input.Properties) > 0 {
				idString = red.Get(curr).Val()
				if cid, err = geojson.FeatureFromBytes([]byte(idString)); err == nil {
					if !passImageDescriptor(cid, input, false) {
						continue
					}
				}
			}
			z.Member = curr
			z.Score = red.ZScore(indexName, curr).Val()
			if result := red.ZAdd(cacheName, z); result.Err() != nil {
				RedisError(red, result.Err())
			}
			count++
			// Cap the result sets to a modest amount
			if count >= maxCacheSize {
				break
			}
		}
	}

	// Stick a terminal entry in the index so we know it is done
	// This is the only one with a positive score
	z.Member = ""
	z.Score = 1
	if result := red.ZAdd(cacheName, z); result.Err() != nil {
		RedisError(red, result.Err())
	}

	duration, _ := time.ParseDuration(maxCacheTimeout)
	if result := red.Expire(cacheName, duration); result.Err() != nil {
		RedisError(red, result.Err())
	}
}

// This pass function gets called before retrieving and unmarshaling the value
func passImageDescriptorKey(key string, test *geojson.Feature) bool {
	var (
		idCloudCover = 1.0
	)
	if test == nil {
		return true
	}

	keyParts := strings.Split(key, "&")
	if len(keyParts) > 0 {
		var (
			bbox geojson.BoundingBox
			err  error
		)
		part := keyParts[1]
		keyParts = strings.Split(part, ",")
		if len(keyParts) > 4 {
			idCloudCover, _ = strconv.ParseFloat(keyParts[4], 64) // The 4th "value" is actually cloudCover
			testCloudCover := test.PropertyFloat("cloudCover")
			if !math.IsNaN(testCloudCover) && !math.IsNaN(idCloudCover) {
				if idCloudCover > testCloudCover {
					return false
				}
			}
		}
		if len(keyParts) >= 4 {
			if bbox, err = geojson.NewBoundingBox(keyParts[0:4]); err != nil {
				log.Printf("Expected a valid bounding box but received %v instead", err.Error())
				return false
			}

			if (len(test.Bbox) > 0) && !test.Bbox.Overlaps(bbox) {
				return false
			}
		}
	}
	return true
}

// pass returns true if the receiving object complies
// with all of the properties in the input
// This uses the unmarshaled value for the key
func passImageDescriptor(id, test *geojson.Feature, rigorous bool) bool {
	if test == nil {
		return true
	}
	// pull the actual polygon in case the bounding box is not sufficiently precise
	if rigorous {
		var (
			idGeometry, testGeometry *geos.Geometry
			err                      error
			intersects               bool
		)
		if idGeometry, err = geojsongeos.GeosFromGeoJSON(id.Geometry); err != nil {
			log.Printf("Failed to convert id Geometry. %v", err.Error())
			return false
		}
		if testGeometry, err = geojsongeos.GeosFromGeoJSON(test.Geometry); err != nil {
			log.Printf("Failed to convert id Geometry. %v", err.Error())
			return false
		}
		if intersects, err = testGeometry.Intersects(idGeometry); err != nil {
			log.Printf("Failed to test containment on geometries. %v", err.Error())
			return false
		} else if !intersects {
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

	testBands := test.PropertyStringSlice("bands")
	if len(testBands) > 0 {
		if idBandsIfc, ok := id.Properties["bands"]; ok {
			idBands := idBandsIfc.(map[string]interface{})
			for _, testBand := range testBands {
				if _, ok = idBands[testBand]; !ok {
					return false
				}
			}
		}
	}

	return true
}

// GetSceneMetadata returns the image metadata as a GeoJSON feature
func GetSceneMetadata(id string) (*geojson.Feature, error) {
	red, _ := RedisClient()
	var stringCmd *redis.StringCmd
	key := imageCatalogPrefix + ":" + id
	if stringCmd = red.Get(key); stringCmd.Err() != nil {
		// If it isn't there, try a wildcard search
		// because they key might be missing the adjunct
		key = key + "*"
		var ssc *redis.StringSliceCmd
		if ssc = red.Keys(key); ssc.Err() != nil {
			return nil, stringCmd.Err()
		}
		if len(ssc.Val()) > 0 {
			// We have to strip out the prefix. Annoying!
			parts := strings.SplitN(ssc.Val()[0], ":", 2)
			if len(parts) > 0 {
				return GetSceneMetadata(parts[1])
			}
		}
		return nil, errors.New("redis: nil")
	}
	metadataString := stringCmd.Val()
	return geojson.FeatureFromBytes([]byte(metadataString))
}

func featureKey(feature *geojson.Feature) string {
	return fmt.Sprintf("%v:%v&%v,%v", imageCatalogPrefix, feature.ID, feature.ForceBbox().String(), strconv.FormatFloat(feature.PropertyFloat("cloudCover"), 'f', 6, 64))
}

// StoreFeature stores a feature into the catalog
// using a key based on the feature's ID
func StoreFeature(feature *geojson.Feature, reharvest bool) (string, error) {
	var (
		err error
		b   []byte
	)
	red, _ := RedisClient()
	key := featureKey(feature)
	if b, err = geojson.Write(feature); err != nil {
		return "", err
	}

	if red.Exists(key).Val() {
		message := fmt.Sprintf("Record %v already exists.", key)
		// Unless this flag is set, we don't want to reharvest things we already have
		if reharvest {
			fmt.Print(message + " Reharvesting.")
		} else {
			return "", pzsvc.ErrWithTrace(message)
		}
	}

	red.Set(key, string(b), 0)
	z := redis.Z{Score: calculateScore(feature), Member: key}
	red.ZAdd(imageCatalogPrefix, z)

	return key, nil
}

// RemoveFeature removes a feature from the catalog and any known caches
func RemoveFeature(feature *geojson.Feature) error {
	var ic *redis.IntCmd
	red, _ := RedisClient()
	key := featureKey(feature)
	if results := red.SMembers(imageCatalogPrefix + "-caches"); results.Err() == nil {
		for _, curr := range results.Val() {
			red.ZRem(curr, key)
		}
	} else {
		return pzsvc.TraceErr(results.Err())
	}
	red.ZRem(imageCatalogPrefix, key)

	ic = red.Del(key)
	return ic.Err()
}

// SaveFeatureProperties retrieves the requested feature from the database,
// Updates properties with the contents of the map provided, and re-saves
func SaveFeatureProperties(id string, properties map[string]interface{}) error {
	var (
		feature *geojson.Feature
		err     error
	)
	if feature, err = GetSceneMetadata(id); err == nil {
		for key, property := range properties {
			feature.Properties[key] = property
		}
		if _, err = StoreFeature(feature, true); err != nil {
			return pzsvc.TraceErr(err)
		}
	} else {
		return pzsvc.TraceErr(err)
	}
	return nil
}

func calculateScore(feature *geojson.Feature) float64 {
	var score float64
	acquiredDateStr := feature.PropertyString("acquiredDate")
	if adTime, err := time.Parse(time.RFC3339, acquiredDateStr); err == nil {
		score = float64(-adTime.Unix())
	}
	return score
}

// DropIndex drops the main index containing all known catalog entries,
// deletes the underlying entries, and returns the number of images
func DropIndex() int {
	var (
		count int
		key   string
	)

	red, _ := RedisClient()
	transaction := red.Multi()
	defer transaction.Close()

	// Keys
	if results := transaction.ZRange(imageCatalogPrefix, 0, -1); results.Err() == nil {
		count = len(results.Val())
		fmt.Printf("Dropping %v keys.", len(results.Val()))
		for _, curr := range results.Val() {
			transaction.Del(curr)
		}
	}

	// Caches
	key = imageCatalogPrefix + "-caches"
	if results := transaction.SMembers(key); results.Err() == nil {
		count += len(results.Val())
		fmt.Printf("Dropping %v caches.", len(results.Val()))
		for _, curr := range results.Val() {
			transaction.Del(curr)
		}
		transaction.Del(key)
	}
	transaction.Del(imageCatalogPrefix)

	// Recurrences
	if results := transaction.SMembers(recurringRoot); results.Err() == nil {
		count += len(results.Val())
		fmt.Printf("Dropping %v caches.", len(results.Val()))
		for _, curr := range results.Val() {
			transaction.Del(curr)
		}
		transaction.Del(recurringRoot)
	}
	transaction.Del(imageCatalogPrefix)
	return count
}
