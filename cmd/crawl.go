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
	"log"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/paulsmith/gogeos/geos"
	"github.com/spf13/cobra"
	"github.com/venicegeo/geojson-geos-go/geojsongeos"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

var crawlCmd = &cobra.Command{
	Use:   "crawl",
	Short: "Crawl Catalog",
	Long: `
Crawl the image catalog for images matching the inputs`,
	Run: func(cmd *cobra.Command, args []string) {
		var (
			err error
			gj  interface{}
		)
		for _, arg := range os.Args[2:] {
			if gj, err = geojson.ParseFile(arg); err == nil {
				err = crawl(gj)
			}
		}
		if err != nil {
			log.Print(err.Error())
		}
	},
}

func crawlHandler(writer http.ResponseWriter, request *http.Request) {
	// var (
	// 	bytes     []byte
	// 	err       error
	// 	errorText string
	// )
	//
	// if origin := request.Header.Get("Origin"); origin != "" {
	// 	writer.Header().Set("Access-Control-Allow-Origin", origin)
	// 	writer.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	// 	writer.Header().Set("Access-Control-Allow-Headers",
	// 		"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	// }
	// // Stop here if its Preflighted OPTIONS request
	// if request.Method == pzsvc.MethodOptions {
	// 	return
	// }
	//
	// switch request.Method {
	// case pzsvc.MethodPost:
	// 	defer request.Body.Close()
	// 	if bytes, err = ioutil.ReadAll(request.Body); err != nil {
	// 		errorText += err.Error() + "\n"
	// 	}
	//
	// case "GET":
	// 	subindexes := catalog.Subindexes()
	// 	if bytes, err = json.Marshal(subindexes); err != nil {
	// 		errorText += err.Error() + "\n"
	// 	}
	// }
	//
	// if errorText == "" {
	// 	writer.Header().Set("Content-Type", "application/json")
	// 	writer.Write(bytes)
	// } else {
	// 	http.Error(writer, errorText, http.StatusBadRequest)
	// }
}

func crawl(gjIfc interface{}) error {
	var (
		err error
		geosLineString,
		currentGeometry,
		polygon,
		point *geos.Geometry
		pointCount int
		contains   bool
		bestImage  *geojson.Feature
		bestImages catalog.ImageDescriptors
	)

	switch gj := gjIfc.(type) {
	case *geojson.FeatureCollection:
		for _, feature := range gj.Features {
			if err = crawl(feature); err != nil {
				return err
			}
		}
	case *geojson.Feature:
		bestImages.Images = geojson.NewFeatureCollection(nil)
		if geosLineString, err = geojsongeos.GeosFromGeoJSON(gjIfc); err != nil {
			return err
		}
		log.Print(geosLineString.String())
		if polygon, err = geos.EmptyPolygon(); err != nil {
			return err
		}
		if pointCount, err = geosLineString.NPoint(); err != nil {
			return err
		}
		for inx := 0; inx < pointCount; inx++ {
			if point, err = geosLineString.Point(inx); err != nil {
				return err
			}
			if contains, err = polygon.Contains(point); err != nil {
				return err
			} else if contains {
				log.Printf("Skipping point %v", point.String())
				continue
			}
			if bestImage = getBestImage(point); bestImage == nil {
				log.Print("Didn't get a candidate image.")
			} else {
				bestImages.Images.Features = append(bestImages.Images.Features, bestImage)
				if currentGeometry, err = geojsongeos.GeosFromGeoJSON(bestImage.Geometry); err != nil {
					return err
				}
				polygon, err = polygon.Union(currentGeometry)
			}
		}
		var gjGeometry interface{}
		if gjGeometry, err = geojsongeos.GeoJSONFromGeos(polygon); err == nil {
			geojson.WriteFile(gjGeometry, "polygon.geojson")
		} else {
			log.Print(err.Error())
		}

		geojson.WriteFile(bestImages.Images, "out.geojson")
	}

	return err
}

func getBestImage(point *geos.Geometry) *geojson.Feature {
	var (
		options catalog.SearchOptions
		feature,
		currentImage,
		bestImage *geojson.Feature
		geometry interface{}
		currentScore,
		bestScore,
		cloudCover float64
		acquiredDate     time.Time
		acquiredDateUnix int64
		err              error
		imageDescriptors catalog.ImageDescriptors
	)
	now := time.Now().Unix()
	options.NoCache = true
	options.Rigorous = true
	log.Print(point.String())
	geometry, _ = geojsongeos.GeoJSONFromGeos(point)
	feature = geojson.NewFeature(geometry, "", nil)
	feature.Bbox = feature.ForceBbox()
	if imageDescriptors, _, err = catalog.GetImages(feature, options); err != nil {
		log.Printf("Failed to get images from image catalog: %v", err.Error())
		return nil
	}
	for _, currentImage = range imageDescriptors.Images.Features {
		cloudCover = currentImage.PropertyFloat("cloudCover")
		acquiredDateString := currentImage.PropertyString("acquiredDate")
		if acquiredDate, err = time.Parse(time.RFC3339, acquiredDateString); err != nil {
			return nil
		}
		acquiredDateUnix = acquiredDate.Unix()
		currentScore = 1 - math.Sqrt(cloudCover/100.0) - float64((now-acquiredDateUnix)/(60*60*24*365*10))
		if currentScore > bestScore {
			bestImage = currentImage
			bestImage.Properties["score"] = currentScore
			bestScore = currentScore
		}
	}
	log.Printf("Best Image: %v, Score: %v", bestImage.String(), bestScore)
	return bestImage
}
