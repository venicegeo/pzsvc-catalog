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

// GetImages returns images for the given set matching the criteria in the options
func GetImages(options *geojson.Feature, start int64, end int64) (ImageDescriptors, string, error) {

	var (
		result     ImageDescriptors
		resultText string
		fc         *geojson.FeatureCollection
		features   []*geojson.Feature
		zrbs       redis.ZRangeByScore
		ssc        *redis.StringSliceCmd
		sc         *redis.StringCmd
	)

	features = make([]*geojson.Feature, 0)
	red, _ := RedisClient()
	cacheName := getDiscoverCacheName(options)

	// If the cache does not exist, create it asynchronously
	if cacheExists := client.Exists(cacheName); cacheExists.Err() == nil {
		if !cacheExists.Val() {
			go populateCache(options, cacheName)
		}
	} else {
		RedisError(red, cacheExists.Err())
		return result, "", cacheExists.Err()
	}

	// See if we can complete the requested query
	complete := false
	for !complete {
		card := red.ZCard(cacheName)
		result.TotalCount = int(card.Val()) - 1 // ignore terminal element
		if card.Err() != nil {
			RedisError(red, card.Err())
			return result, "", card.Err()
			// See we have enough results already
		} else if card.Val() > end {
			complete = true
			// See if the terminal object has been added
		} else {
			zrbs.Min = "0.5"
			zrbs.Max = "1.5"
			ssc = red.ZRangeByScore(cacheName, zrbs)
			if ssc.Err() != nil {
				RedisError(red, card.Err())
				return result, "", card.Err()
			} else if len(ssc.Val()) > 0 {
				complete = true
			}
		}
		if !complete {
			time.Sleep(100 * time.Millisecond)
		}
	}

	if ssc = red.ZRange(cacheName, start, end); ssc.Err() == nil {
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
		result.StartIndex = int(start)
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

// populateCache populates a cache corresponding
// to the search criteria provided
func populateCache(options *geojson.Feature, cacheName string) {
	red, _ := RedisClient()
	var (
		cid      *geojson.Feature
		idString string
		err      error
		z        redis.Z
		intCmd   *redis.IntCmd
		members  *redis.StringSliceCmd
		count    int
	)

	// Make a set of caches in case we want to nuke them later
	if intCmd = red.SAdd(imageCatalogPrefix+"-caches", cacheName); intCmd.Err() != nil {
		RedisError(red, intCmd.Err())
	}
	// Create the cache using a full table scan
	if members = red.ZRange(imageCatalogPrefix, 0, -1); members.Err() != nil {
		RedisError(red, intCmd.Err())
	}
	for _, curr := range members.Val() {
		if passImageDescriptorKey(curr, options) {
			// If there are no test properties, there is no point in inspecting the contents
			if len(options.Properties) > 0 {
				idString = red.Get(curr).Val()
				if cid, err = geojson.FeatureFromBytes([]byte(idString)); err == nil {
					if !passImageDescriptor(cid, options) {
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
		part := keyParts[1]
		bbox := geojson.NewBoundingBox(part)
		testCloudCover := test.PropertyFloat("cloudCover")
		idCloudCover := bbox[4] // The 4th "value" is actually cloudCover
		bbox = bbox[0:4]
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
func StoreFeature(feature *geojson.Feature, score float64, reharvest bool) error {
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
			return fmt.Errorf("Record %v already exists.", key)
		}
	}

	rc.Set(key, string(bytes), 0)
	z := redis.Z{Score: score, Member: key}
	rc.ZAdd(imageCatalogPrefix, z)
	return nil
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
