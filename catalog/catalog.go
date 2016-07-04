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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gopkg.in/redis.v3"

	"github.com/venicegeo/geojson-go/geojson"
)

const maxCacheSize = 1000
const maxCacheTimeout = "1h"

var imageCatalogPrefix string

var httpClient *http.Client

// SearchOptions is the options for a search request
type SearchOptions struct {
	NoCache      bool
	MinimumIndex int
	MaximumIndex int
	Count        int
}

// HTTPClient is a factory method for a http.Client suitable for common operations
func HTTPClient() *http.Client {
	if httpClient == nil {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}

		httpClient = &http.Client{Transport: transport}
	}
	return httpClient
}

// SetImageCatalogPrefix sets the prefix for this instance
// when it is necessary to override the default
func SetImageCatalogPrefix(prefix string) {
	imageCatalogPrefix = prefix
}

// ImageDescriptors is the response to a Discover query
type ImageDescriptors struct {
	Count      int                        `json:"count"`
	TotalCount int                        `json:"totalCount"`
	StartIndex int                        `json:"startIndex"`
	Images     *geojson.FeatureCollection `json:"images"`
}

// IndexSize returns the size of the index
func IndexSize() int64 {
	rc, _ := RedisClient()
	result := rc.ZCard(imageCatalogPrefix)
	return result.Val()
}

// GetImages returns images for the given set matching the criteria in the input and options
func GetImages(input *geojson.Feature, options SearchOptions) (ImageDescriptors, string, error) {

	var (
		result     ImageDescriptors
		resultText string
		fc         *geojson.FeatureCollection
		features   []*geojson.Feature
		zrbs       redis.ZRangeByScore
		ssc        *redis.StringSliceCmd
		sc         *redis.StringCmd
	)

	if options.NoCache {
		return getResults(input, options)
	}

	features = make([]*geojson.Feature, 0)
	red, _ := RedisClient()
	cacheName := getDiscoverCacheName(input)

	// If the cache does not exist, create it asynchronously
	if cacheExists := client.Exists(cacheName); cacheExists.Err() == nil {
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
		cardCmd := red.ZCard(cacheName)
		card := int(cardCmd.Val())
		result.TotalCount = card - 1 // ignore terminal element
		if cardCmd.Err() != nil {
			RedisError(red, cardCmd.Err())
			return result, "", cardCmd.Err()
			// See we have enough results already
		} else if card > options.MaximumIndex {
			complete = true
			// See if the terminal object has been added
		} else {
			zrbs.Min = "0.5"
			zrbs.Max = "1.5"
			ssc = red.ZRangeByScore(cacheName, zrbs)
			if ssc.Err() != nil {
				RedisError(red, ssc.Err())
				return result, "", ssc.Err()
			} else if len(ssc.Val()) > 0 {
				complete = true
			}
		}
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
				RedisError(red, sc.Err())
				return result, "", sc.Err()
			}
		}

		result.Count = len(features)
		result.StartIndex = options.MinimumIndex
		fc = geojson.NewFeatureCollection(features)
		result.Images = fc
		bytes, _ := json.Marshal(result)
		resultText = string(bytes)
		return result, resultText, nil
	}

	RedisError(red, ssc.Err())
	return result, "", ssc.Err()
}

// GetDiscoverCacheName returns the name of the index corresponding
// to the search criteria provided
func getDiscoverCacheName(options *geojson.Feature) string {
	bytes, _ := json.Marshal(options)
	// TODO: we may wish to hash this index name
	return imageCatalogPrefix + string(bytes)
}

// getResults returns the results of the requested query without the caching mechanism
func getResults(input *geojson.Feature, options SearchOptions) (ImageDescriptors, string, error) {
	var (
		members  *redis.StringSliceCmd
		cid      *geojson.Feature
		result   ImageDescriptors
		idCmd    *redis.StringCmd
		features []*geojson.Feature
		fc       *geojson.FeatureCollection
		err      error
	)

	red, _ := RedisClient()

	// Perform a full table scan
	// TODO: Consider a ZScore operation especially if we have to/from dates available
	if members = red.ZRange(imageCatalogPrefix, 0, -1); members.Err() != nil {
		RedisError(red, members.Err())
		return result, "", members.Err()
	}
	for _, curr := range members.Val() {
		// First look at the key - we can often save time by not retrieving the value at all
		if passImageDescriptorKey(curr, input) {
			if idCmd = red.Get(curr); idCmd.Err() != nil {
				return result, "", idCmd.Err()
			}
			if cid, err = geojson.FeatureFromBytes([]byte(idCmd.Val())); err == nil {
				if passImageDescriptor(cid, input) {
					features = append(features, cid)
					if options.Count > 0 && (len(features) >= options.Count) {
						break
					}
				}
			} else {
				return result, "", err
			}
		}
	}

	fc = geojson.NewFeatureCollection(features)
	result.Images = fc
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
		err             error
		z               redis.Z
		intCmd          *redis.IntCmd
		members         *redis.StringSliceCmd
		count           int
		acquiredDate    time.Time
		maxAcquiredDate time.Time
	)

	// Make a set of caches in case we want to nuke them later
	if intCmd = red.SAdd(imageCatalogPrefix+"-caches", cacheName); intCmd.Err() != nil {
		RedisError(red, intCmd.Err())
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

	if acquiredDate.IsZero() && maxAcquiredDate.IsZero() {
		// Create the cache using a full table scan
		if members = red.ZRange(imageCatalogPrefix, 0, -1); members.Err() != nil {
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
		if members = red.ZRangeByScore(imageCatalogPrefix, opt); members.Err() != nil {
			RedisError(red, members.Err())
		}
	}

	for _, curr := range members.Val() {
		if passImageDescriptorKey(curr, input) {
			// If there are no test properties, there is no point in inspecting the contents
			if len(input.Properties) > 0 {
				idString = red.Get(curr).Val()
				if cid, err = geojson.FeatureFromBytes([]byte(idString)); err == nil {
					if !passImageDescriptor(cid, input) {
						continue
					}
				}
			}
			z.Member = curr
			z.Score = red.ZScore(imageCatalogPrefix, curr).Val()
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
		idCloudCover, _ := strconv.ParseFloat(keyParts[4], 64) // The 4th "value" is actually cloudCover
		if bbox, err = geojson.NewBoundingBox(keyParts[0:4]); err != nil {
			log.Printf("Expected a valid bounding box but received %v instead", err.Error())
			return false
		}
		testCloudCover := test.PropertyFloat("cloudCover")
		if testCloudCover != 0 && idCloudCover != 0 && !math.IsNaN(testCloudCover) && !math.IsNaN(idCloudCover) {
			if idCloudCover > testCloudCover {
				return false
			}
		}

		if (len(test.Bbox) > 0) && !test.Bbox.Overlaps(bbox) {
			return false
		}

	}
	return true
}

// pass returns true if the receiving object complies
// with all of the properties in the input
// This uses the unmarshaled value for the key
func passImageDescriptor(id, test *geojson.Feature) bool {
	if test == nil {
		return true
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

// GetImageMetadata returns the image metadata as a GeoJSON feature
func GetImageMetadata(id string) (*geojson.Feature, error) {
	rc, _ := RedisClient()
	var stringCmd *redis.StringCmd
	key := imageCatalogPrefix + ":" + id
	if stringCmd = rc.Get(key); stringCmd.Err() != nil {
		return nil, stringCmd.Err()
	}
	metadataString := stringCmd.Val()
	return geojson.FeatureFromBytes([]byte(metadataString))
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
func StoreFeature(feature *geojson.Feature, score float64, reharvest bool) (string, error) {
	rc, _ := RedisClient()
	bytes, _ := json.Marshal(feature)
	key := imageCatalogPrefix + ":" +
		feature.ID + "&" +
		feature.ForceBbox().String() + "," +
		strconv.FormatFloat(feature.PropertyFloat("cloudCover"), 'f', 6, 64)

	// Unless this flag is set, we don't want to reharvest things we already have
	if !reharvest {
		boolCmd := rc.Exists(key)
		if boolCmd.Val() {
			return "", fmt.Errorf("Record %v already exists.", key)
		}
	}

	rc.Set(key, string(bytes), 0)
	z := redis.Z{Score: score, Member: key}
	rc.ZAdd(imageCatalogPrefix, z)
	return key, nil
}

// DropIndex drops the main index containing all known catalog entries
// and deletes the underlying entries
func DropIndex() {
	red, _ := RedisClient()
	transaction, _ := red.Watch(imageCatalogPrefix)
	defer transaction.Close()
	if results := transaction.ZRange(imageCatalogPrefix, 0, -1); results.Err() == nil {
		log.Printf("Dropping %v keys.", len(results.Val()))
		for _, curr := range results.Val() {
			transaction.Del(curr)
		}
	}
	transaction.Del(imageCatalogPrefix)
}

// ImageIOReader returns an io Reader for the requested band
func ImageIOReader(id, band, key string) (io.Reader, error) {
	var (
		feature *geojson.Feature
		err     error
	)
	if feature, err = GetImageMetadata(id); err != nil {
		return nil, err
	}
	return ImageFeatureIOReader(feature, band, key)
}

// SetRecurrence sets a key so that a daemon can determine
// whether to rerun a harvesting operation
// A blank value means "off"
func SetRecurrence(domain string, flag bool, value string) {
	red, _ := RedisClient()
	key := imageCatalogPrefix + ":" + domain + ":recur"
	if flag {
		red.Set(key, value, 0)
	} else {
		red.Del(key)
	}
}

// Recurrence returns the recurrence flag for the specified domain
// A blank response indicates unset
func Recurrence(domain string) string {
	red, _ := RedisClient()
	key := imageCatalogPrefix + ":" + domain + ":recur"
	if bc := red.Exists(key); bc.Err() == nil && bc.Val() {
		sc := red.Get(key)
		return sc.Val()
	}
	return ""
}

// ImageFeatureIOReader returns an io Reader for the requested band
func ImageFeatureIOReader(feature *geojson.Feature, band string, key string) (io.Reader, error) {
	var (
		result    io.Reader
		err       error
		bandsMap  map[string]interface{}
		urlIfc    interface{}
		urlString string
		ok        bool
		response  *http.Response
	)
	if bandsMap, ok = feature.Properties["bands"].(map[string]interface{}); ok {
		if urlIfc, ok = bandsMap[band]; ok {
			if urlString, ok = urlIfc.(string); ok {
				// This now works for PlanetLabs and Landsat.
				// Others will require additional code.
				if strings.HasPrefix(feature.ID, "pl:") {
					if response, err = DoPlanetRequest("GET", urlString, key); err != nil {
						return nil, err
					}
				} else {
					var request *http.Request
					if request, err = http.NewRequest("GET", urlString, nil); err != nil {
						return nil, err
					}
					if response, err = HTTPClient().Do(request); err != nil {
						return nil, err
					}
				}
				result = response.Body
			}
		}
	}
	if ok {
		return result, nil
	}
	return nil, fmt.Errorf("Requested band \"%v\" not found in image %v.", band, feature.ID)
}
