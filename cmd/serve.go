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

func planetHandler(writer http.ResponseWriter, request *http.Request) {
	if drop, err := strconv.ParseBool(request.FormValue("dropIndex")); (err == nil) && drop {
		writer.Write([]byte("Dropping existing index.\n"))
		catalog.DropIndex()
	}
	go harvestPlanet(request.FormValue("PL_API_KEY"))
	writer.Write([]byte("Harvesting started. Check back later."))
}

func discoverHandler(writer http.ResponseWriter, request *http.Request) {
	var count int64
	properties := make(map[string]interface{})

	// Give ourselves a resonable default and maximum
	if parsedCount, err := strconv.ParseInt(request.FormValue("count"), 0, 32); err == nil {
		count = int64(math.Min(float64(parsedCount), 1000))
	} else {
		count = 20
	}

	if fileFormat := request.FormValue("fileFormat"); fileFormat != "" {
		properties["fileFormat"] = fileFormat
	}

	if acquiredDate := request.FormValue("acquiredDate"); acquiredDate != "" {
		properties["acquiredDate"] = acquiredDate
	}

	if bitDepth, err := strconv.ParseInt(request.FormValue("bitDepth"), 0, 32); err == nil {
		properties["bitDepth"] = int(bitDepth)
	}

	if fileSize, err := strconv.ParseInt(request.FormValue("fileSize"), 0, 64); err == nil {
		properties["fileSize"] = fileSize
	}

	if cloudCover, err := strconv.ParseFloat(request.FormValue("cloudCover"), 64); err == nil {
		properties["cloudCover"] = cloudCover
	}

	if beachfrontScore, err := strconv.ParseFloat(request.FormValue("beachfrontScore"), 64); err == nil {
		properties["beachfrontScore"] = beachfrontScore
	}

	if sensorName := request.FormValue("sensorName"); sensorName != "" {
		properties["sensorName"] = sensorName
	}

	if bandsString := request.FormValue("bands"); bandsString != "" {
		bands := strings.Split(bandsString, ",")
		properties["bands"] = bands
	}

	searchFeature := geojson.NewFeature(nil, "", properties)

	if bboxString := request.FormValue("bbox"); bboxString != "" {
		searchFeature.Bbox = geojson.NewBoundingBox(bboxString)
	}

	startIndex := int64(0)
	if resp, err := strconv.ParseInt(request.FormValue("startIndex"), 0, 64); err == nil {
		startIndex = resp
	}
	_, responseString := catalog.GetImages(searchFeature, startIndex, startIndex+count-1)

	writer.Write([]byte(responseString))
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
