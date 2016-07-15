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

package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"

	"github.com/paulsmith/gogeos/geos"
	"github.com/venicegeo/geojson-geos-go/geojsongeos"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

// SubIndex represents a sub-index for the image catalog
type SubIndex struct {
	WfsURL      string `json:"wfsurl"`
	FeatureType string `json:"featureType"`
	SubIndexID  string `json:"subIndexID"`
}

// ResolveSubIndexID determines the SubIndexID
// based on the other parameters and returns it
func (si *SubIndex) ResolveSubIndexID() string {
	si.SubIndexID = catalog.ImageCatalogPrefix() + ":" + si.WfsURL + ":" + si.FeatureType
	return si.SubIndexID
}

func subIndexHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		bytes    []byte
		err      error
		subIndex SubIndex
	)

	if origin := request.Header.Get("Origin"); origin != "" {
		writer.Header().Set("Access-Control-Allow-Origin", origin)
		writer.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		writer.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if request.Method == "OPTIONS" {
		return
	}

	if request.Method == "POST" {
		defer request.Body.Close()
		if bytes, err = ioutil.ReadAll(request.Body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
		}

		if err = json.Unmarshal(bytes, &subIndex); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
		}
	}
	if subIndex.WfsURL == "" {
		subIndex.WfsURL = request.FormValue("wfsurl")
	}
	if subIndex.WfsURL == "" {
		http.Error(writer, "Calls to /subindex must contain a WFS URL.", http.StatusBadRequest)
	}

	subIndex.ResolveSubIndexID()
	go createSubindex(subIndex)

	writer.Header().Set("Content-Type", "application/json")
	bytes, _ = json.Marshal(subIndex)
	writer.Write(bytes)
}

func createSubindex(subIndex SubIndex) {
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
	v.Set("typeName", subIndex.FeatureType)

	qurl := subIndex.WfsURL + "?" + v.Encode()

	request, _ = http.NewRequest("GET", qurl, nil)
	response, _ = catalog.HTTPClient().Do(request)

	// Check for HTTP errors
	if response.StatusCode < 200 || response.StatusCode > 299 {
		log.Printf("Received %v: \"%v\" when performing a GetFeature request on %v\n%#v", response.StatusCode, response.Status, subIndex.WfsURL, request)
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
		// for inx := 0; (inx < 100) && (inx < len(fc.Features)); inx++ {
		// 	feature := fc.Features[inx]
		for _, feature := range fc.Features {
			if geometry, err = geojsongeos.GeosFromGeoJSON(feature); err != nil {
				log.Print(err.Error())
				return
			}
			for index, box := range tiles {
				var (
					intersection *geos.Geometry
					intersects   bool
				)
				if intersects, err = box.Contains(geometry); err != nil {
					log.Printf("Error in Intersects on %v: %v", index, err.Error())
					return
				} else if intersects {
					if intersection, err = box.Intersection(geometry); err != nil {
						log.Printf("Error performing intersection on %v: %v Trying boundary instead", index, err.Error())
						if geometry, err = geometry.Boundary(); err != nil {
							log.Printf("Can't retrieve boundary either: %v. Continuing.", err.Error())
							continue
						}
						if intersection, err = box.Intersection(geometry); err != nil {
							log.Printf("Still can't perform intersection, even on boundary: %v. Continuing.", err.Error())
							continue
						}
					} else if intersection == nil {
						log.Printf("Received null intersection on %v", index)
						continue
					}
					tiledGeometries[index] = append(tiledGeometries[index], intersection)
				}
			}
		}
		log.Printf("Found %v tiled geometries", len(tiledGeometries))
		tileMap := tileGeometries(tiledGeometries)
		log.Printf("geometry map has %v tiles", len(tileMap))
		// // This will ensure the sub-index is considered in subsequent operations
		catalog.SetSubIndex(subIndex.SubIndexID, tileMap)
		// // This will ensure the sub-index is considered with already-harvested images
		count := catalog.PopulateSubIndex(subIndex.SubIndexID)
		log.Printf("Added %v entries to %v.", count, subIndex.SubIndexID)
		// counter := 0
		// for _, value := range tileMap {
		// 	counter++
		// 	log.Printf("How about: %v", value.String())
		// 	if counter > 5 {
		// 		break
		// 	}
		// }
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
					log.Printf("Index from collection %v: %v", index, geometry.String())
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
