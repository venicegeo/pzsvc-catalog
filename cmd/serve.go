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
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"

	"gopkg.in/redis.v3"
)

func serve() {

	portStr := ":8080"
	var (
		client *redis.Client
		err    error
	)
	if client, err = catalog.RedisClient(); err != nil {
		log.Fatalf("Failed to create Redis client: %v", err.Error())
	}
	defer client.Close()
	if info := client.Info(); info.Err() == nil {
		router := mux.NewRouter()

		router.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
			fmt.Fprintf(writer, "Hi")
		})
		router.HandleFunc("/discover", discoverHandler)
		router.HandleFunc("/planet", planetHandler)
		router.HandleFunc("/provision/{id}/{band}", provisionHandler)
		// 	case "/help":
		// 		fmt.Fprintf(writer, "We're sorry, help is not yet implemented.\n")
		// 	default:
		// 		fmt.Fprintf(writer, "Command undefined. \n")
		// 	}
		// })
		http.Handle("/", router)
	} else {
		message := fmt.Sprintf("Failed to connect to Redis: %v", info.Err().Error())
		log.Print(message)
		http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
			http.Error(writer, message, http.StatusInternalServerError)
		})
	}

	go recurrentHandling()

	log.Fatal(http.ListenAndServe(portStr, nil))
}

func provisionHandler(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	id := vars["id"]
	band := vars["band"]
	if metadata, err := catalog.GetImageMetadata(id); err != nil {
		message := fmt.Sprintf("Unable to retrieve metadata for %v: %v", id, err.Error())
		http.Error(writer, message, http.StatusBadRequest)
	} else {
		bytes, _ := json.Marshal(metadata)
		gj, _ := geojson.FeatureFromBytes(bytes)
		bandValue := gj.Properties["bands"].(map[string]interface{})[band].(string)
		writer.Write([]byte(bandValue))
	}
}

func discoverHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		count           int
		startIndex      int
		err             error
		bitDepth        int64
		fileSize        int64
		parsedCount     int64
		startIndexI64   int64
		acquiredDate    string
		maxAcquiredDate string
		bandsString     string
		bboxString      string
		cloudCover      float64
		beachfrontScore float64
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

	nocache, _ := strconv.ParseBool(request.FormValue("nocache"))

	// Give ourselves a resonable default and maximum count (when caching)
	if parsedCount, err = strconv.ParseInt(request.FormValue("count"), 0, 32); err == nil {
		if !nocache {
			count = int(math.Min(float64(parsedCount), 1000))
		}
	} else {
		count = 20
	}

	if startIndexI64, err = strconv.ParseInt(request.FormValue("startIndex"), 0, 32); err == nil {
		startIndex = int(startIndexI64)
	}

	// Put most of the parameters into a properties map
	properties := make(map[string]interface{})

	if fileFormat := request.FormValue("fileFormat"); fileFormat != "" {
		properties["fileFormat"] = fileFormat
	}

	if acquiredDate = request.FormValue("acquiredDate"); acquiredDate != "" {
		if _, err = time.Parse(time.RFC3339, acquiredDate); err != nil {
			http.Error(writer, "Format of acquiredDate is invalid:  "+err.Error(), http.StatusBadRequest)
			return
		}
		properties["acquiredDate"] = acquiredDate
	}

	if maxAcquiredDate = request.FormValue("maxAcquiredDate"); maxAcquiredDate != "" {
		if _, err = time.Parse(time.RFC3339, maxAcquiredDate); err != nil {
			http.Error(writer, "Format of maxAcquiredDate is invalid:  "+err.Error(), http.StatusBadRequest)
			return
		}
		properties["maxAcquiredDate"] = maxAcquiredDate
	}

	if bitDepth, err = strconv.ParseInt(request.FormValue("bitDepth"), 0, 32); err == nil {
		properties["bitDepth"] = int(bitDepth)
	}

	if fileSize, err = strconv.ParseInt(request.FormValue("fileSize"), 0, 64); err == nil {
		properties["fileSize"] = fileSize
	}

	if cloudCover, err = strconv.ParseFloat(request.FormValue("cloudCover"), 64); err == nil {
		properties["cloudCover"] = cloudCover
	}

	if beachfrontScore, err = strconv.ParseFloat(request.FormValue("beachfrontScore"), 64); err == nil {
		properties["beachfrontScore"] = beachfrontScore
	}

	if sensorName := request.FormValue("sensorName"); sensorName != "" {
		properties["sensorName"] = sensorName
	}

	if bandsString = request.FormValue("bands"); bandsString != "" {
		bands := strings.Split(bandsString, ",")
		properties["bands"] = bands
	}

	searchFeature := geojson.NewFeature(nil, "", properties)

	bboxString = request.FormValue("bbox")
	if !nocache && (bboxString == "") && (acquiredDate == "") && (maxAcquiredDate == "") {
		http.Error(writer, "A discovery request must contain at least one of the following:\n* bounding box\n* acquiredDate\n* maxAcquiredDate", http.StatusBadRequest)
		return
	}
	if searchFeature.Bbox, err = geojson.NewBoundingBox(bboxString); err == nil {
		if searchFeature.Bbox.Antimeridian() {
			http.Error(writer, "Bounding Box must not cross the antimeridian.", http.StatusBadRequest)
			return
		}
	} else {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	options := catalog.SearchOptions{
		MinimumIndex: startIndex,
		Count:        count,
		MaximumIndex: startIndex + count - 1,
		NoCache:      nocache}

	if _, responseString, err := catalog.GetImages(searchFeature, options); err == nil {
		writer.Header().Set("Content-Type", "application/json")
		writer.Write([]byte(responseString))
	} else {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Serve Catalog",
	Long: `
Serve the image catalog`,
	Run: func(cmd *cobra.Command, args []string) {
		serve()
	},
}
