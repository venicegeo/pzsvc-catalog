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

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		request.ParseForm()
		switch request.URL.Path {
		case "/":
			fmt.Fprintf(writer, "Hi")
		case "/discover":
			discoverFunc(writer, request, client)
		case "/help":
			fmt.Fprintf(writer, "We're sorry, help is not yet implemented.\n")
		default:
			fmt.Fprintf(writer, "Command undefined. \n")
		}
	})

	log.Fatal(http.ListenAndServe(portStr, nil))
}

func discoverFunc(writer http.ResponseWriter, request *http.Request, client *redis.Client) {
	var (
		responseBytes []byte
		count         int
	)
	properties := make(map[string]interface{})

	// Give ourselves a resonable default and maximum
	if parsedCount, err := strconv.ParseInt(request.FormValue("count"), 0, 32); err == nil {
		count = int(math.Min(float64(parsedCount), 1000))
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

	images, responseString := catalog.GetImages(searchFeature)
	// We may wish to return only a subset of available images
	if count < images.Count {
		startIndex := 0
		if resp, err := strconv.ParseInt(request.FormValue("startIndex"), 0, 32); err == nil {
			startIndex = int(resp)
		}
		images.StartIndex = startIndex
		endIndex := int(math.Min(float64(startIndex+int(count)), float64(images.Count)))
		images.Images.Features = images.Images.Features[startIndex:endIndex]
		images.Count = count
		responseBytes, _ = json.Marshal(images)
	} else {
		responseBytes = []byte(responseString)
	}

	writer.Write(responseBytes)
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
