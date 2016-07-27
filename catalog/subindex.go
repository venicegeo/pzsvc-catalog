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
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"

	"github.com/paulsmith/gogeos/geos"
	"github.com/venicegeo/geojson-geos-go/geojsongeos"
	"github.com/venicegeo/geojson-go/geojson"
	"gopkg.in/redis.v3"
)

var subindexMap map[string]Subindex

// Subindex represents a sub-index for the image catalog
type Subindex struct {
	WfsURL      string                    `json:"wfsurl"`
	FeatureType string                    `json:"featureType"`
	Key         string                    `json:"key"`
	Name        string                    `json:"name"`
	TileMap     map[string]*geos.Geometry `json:"-"`
}

// ResolveKey determines the SubIndexID
// based on the other parameters and returns it
func (si *Subindex) ResolveKey() string {
	si.Key = imageCatalogPrefix + ":" + si.WfsURL + ":" + si.FeatureType
	return si.Key
}

// Register registers the cache in the repository for later access
func (si *Subindex) Register() {
	red, _ := RedisClient()
	red.SAdd(imageCatalogPrefix+"-caches", si.ResolveKey())
}

// Subindexes returns the map of available subindexes
func Subindexes() map[string]Subindex {
	if subindexMap == nil {
		subindexMap = make(map[string]Subindex)
	}
	return subindexMap
}

// // SetSubindex sets a filter geometry for an index
// func SetSubindex(key string, geometries map[string]*geos.Geometry) {
// 	// func SetSubIndex(name string, geometries map[string]*geos.PGeometry) {
// 	if subindexMap == nil {
// 		subindexMap = make(map[string]map[string]*geos.Geometry)
// 		// subindexMap = make(map[string]map[string]*geos.PGeometry)
// 	}
// 	subindexMap[key] = geometries
// 	registerCache(key)
// }

// CacheSubindex populates a sub-index for later use
func CacheSubindex(subindex Subindex) int64 {
	var (
		intersects bool
		z          redis.Z
		intCmd     *redis.IntCmd
		flag       bool
	)
	red, _ := RedisClient()
	ids, _, _ := getResults(geojson.NewFeature(nil, "", nil), SearchOptions{})

	for _, feature := range ids.Images.Features {
		geos, _ := geojsongeos.GeosFromGeoJSON(feature)
		for _, geos2 := range subindexMap[subindex.Key].TileMap {
			if intersects, _ = geos2.Intersects(geos); intersects {
				z.Score = calculateScore(feature)
				if math.IsNaN(z.Score) {
					if !flag {
						log.Printf("%v", feature.Properties)
						flag = true
					}
				} else {
					z.Member = imageCatalogPrefix + ":" +
						feature.ID + "&" +
						feature.ForceBbox().String() + "," +
						strconv.FormatFloat(feature.PropertyFloat("cloudCover"), 'f', 6, 64)
					red.ZAdd(subindex.Key, z)
					// log.Printf("added %v with %v", z.Member, z.Score)
				}
			}
		}
	}
	intCmd = red.ZCard(subindex.Key)
	return intCmd.Val()
}

// CreateSubindex performs the actual subindex calculations
func CreateSubindex(subindex Subindex) {
	var (
		request  *http.Request
		response *http.Response
		bytes    []byte
		gjIfc    interface{}
		fc       *geojson.FeatureCollection
		ok       bool
		geometry *geos.Geometry
		err      error
	)

	v := url.Values{}
	v.Set("maxFeatures", "9999")
	v.Set("outputFormat", "application/json")
	v.Set("version", "2.0.0")
	v.Set("request", "GetFeature")
	v.Set("typeName", subindex.FeatureType)

	qurl := subindex.WfsURL + "?" + v.Encode()

	log.Printf("Creating subindex based on WFS: %v", qurl)
	request, _ = http.NewRequest("GET", qurl, nil)
	response, _ = HTTPClient().Do(request)

	// Check for HTTP errors
	if response.StatusCode < 200 || response.StatusCode > 299 {
		log.Printf("Received %v: \"%v\" when performing a GetFeature request on %v\n%#v", response.StatusCode, response.Status, subindex.WfsURL, request)
		return
	}

	defer response.Body.Close()
	bytes, _ = ioutil.ReadAll(response.Body)

	gjIfc, _ = geojson.Parse(bytes)
	if fc, ok = gjIfc.(*geojson.FeatureCollection); ok {
		log.Printf("%v returned %v responses.", qurl, len(fc.Features))
		var (
			tiles           [180 * 360]*geos.Geometry
			tiledGeometries [180 * 360][]*geos.Geometry
			coords          [5]geos.Coord
		)

		// Make some tiles to put the geometries into
		for lonIndex := 0; lonIndex < 360; lonIndex++ {
			for latIndex := 0; latIndex < 180; latIndex++ {
				coords[0] = geos.NewCoord(float64(-180.0+lonIndex), float64(-90.0+latIndex))
				coords[1] = geos.NewCoord(float64(-180.0+lonIndex+1), float64(-90.0+latIndex))
				coords[2] = geos.NewCoord(float64(-180.0+lonIndex+1), float64(-90.0+latIndex+1))
				coords[3] = geos.NewCoord(float64(-180.0+lonIndex), float64(-90.0+latIndex+1))
				coords[4] = geos.NewCoord(float64(-180.0+lonIndex), float64(-90.0+latIndex))
				tiles[lonIndex+(360*latIndex)], _ = geos.NewPolygon(coords[:])
			}
		}

		// Put each feature's geometry in the bucket for the right tile
		for _, feature := range fc.Features {
			bbox := feature.ForceBbox()
			lonIndex := int(math.Floor(bbox[0]) + 180)
			latIndex := int(math.Floor(bbox[1]) + 90)
			index := lonIndex + (360 * latIndex)
			if geometry, err = geojsongeos.GeosFromGeoJSON(feature); err != nil {
				log.Print(err.Error())
				return
			}
			tiledGeometries[index] = append(tiledGeometries[index], geometry)
		}
		// 	for index, box := range tiles {
		// 		var (
		// 			intersection *geos.Geometry
		// 			intersects   bool
		// 		)
		// 		if intersects, err = box.Contains(geometry); err != nil {
		// 			log.Printf("Error in Intersects on %v: %v", index, err.Error())
		// 			return
		// 		} else if intersects {
		// 			if intersection, err = box.Intersection(geometry); err != nil {
		// 				log.Printf("Error performing intersection on %v: %v Trying boundary instead", index, err.Error())
		// 				if geometry, err = geometry.Boundary(); err != nil {
		// 					log.Printf("Can't retrieve boundary either: %v. Continuing.", err.Error())
		// 					continue
		// 				}
		// 				if intersection, err = box.Intersection(geometry); err != nil {
		// 					log.Printf("Still can't perform intersection, even on boundary: %v. Continuing.", err.Error())
		// 					continue
		// 				}
		// 			} else if intersection == nil {
		// 				log.Printf("Received null intersection on %v", index)
		// 				continue
		// 			}
		// 			tiledGeometries[index] = append(tiledGeometries[index], intersection)
		// 			continue
		// 		}
		// 	}
		// }

		tileMap := tileGeometries(tiledGeometries)
		log.Printf("Geometry map has %v tiles", len(tileMap))
		subindex.TileMap = tileMap
		// This will ensure the sub-index is considered in subsequent operations
		Subindexes()[subindex.Key] = subindex
		// Let's keep track of the subindex so we can nuke it later if needed
		subindex.Register()
		// This will ensure the sub-index is considered with already-harvested images
		count := CacheSubindex(subindex)
		log.Printf("Added %v entries to %v.", count, subindex.Key)
	}
}

// func tileGeometries(tiles [180 * 360][]*geos.Geometry) map[string]*geos.PGeometry {
func tileGeometries(tiles [180 * 360][]*geos.Geometry) map[string]*geos.Geometry {
	result := make(map[string]*geos.Geometry)
	// result := make(map[string]*geos.PGeometry)
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
