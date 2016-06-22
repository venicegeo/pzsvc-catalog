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

func planetHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		drop, recurring, reharvest bool
	)
	if drop, _ = strconv.ParseBool(request.FormValue("dropIndex")); drop {
		writer.Write([]byte("Dropping existing index.\n"))
		catalog.DropIndex()
	}
	reharvest, _ = strconv.ParseBool(request.FormValue("reharvest"))
	planetKey := request.FormValue("PL_API_KEY")
	go harvestPlanet(planetKey, reharvest)
	writer.Write([]byte("Harvesting started. Check back later."))

	recurring, _ = strconv.ParseBool(request.FormValue("recurring"))
	if recurring {
		catalog.SetRecurrence("pl", planetKey)
		log.Print("This thing should recur.")
	} else {
		catalog.SetRecurrence("pl", "")
	}
}

func recurrentHandling() {
	for {
		if planetKey := catalog.Recurrence("pl"); planetKey != "" {
			harvestPlanet(planetKey, false)
		}
		time.Sleep(24 * time.Hour)
	}
}

func discoverHandler(writer http.ResponseWriter, request *http.Request) {
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

	if bboxString := request.FormValue("bbox"); bboxString == "" {
		http.Error(writer, "A discovery request must contain a bounding box.", http.StatusBadRequest)
	} else {
		var count int64

		// Give ourselves a resonable default and maximum
		if parsedCount, err := strconv.ParseInt(request.FormValue("count"), 0, 32); err == nil {
			count = int64(math.Min(float64(parsedCount), 1000))
		} else {
			count = 20
		}

		startIndex := int64(0)
		if resp, err := strconv.ParseInt(request.FormValue("startIndex"), 0, 64); err == nil {
			startIndex = resp
		}

		// Put most of the parameters into a properties map
		properties := make(map[string]interface{})

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

		searchFeature.Bbox = geojson.NewBoundingBox(bboxString)

		if _, responseString, err := catalog.GetImages(searchFeature, startIndex, startIndex+count-1); err == nil {
			writer.Write([]byte(responseString))
		} else {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
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
