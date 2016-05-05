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
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/venicegeo/pzsvc-catalog/catalog"

	"gopkg.in/redis.v3"
)

func serve() {

	var portStr string
	// var args = os.Args[1:]
	// if len(args) > 0 {
	// 	portStr = ":" + args[0]
	// } else {
	portStr = ":8080"
	// }

	var options redis.Options
	options.Addr = "127.0.0.1:6379"
	client := redis.NewClient(&options)
	defer client.Close()

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		request.ParseForm()
		switch request.URL.Path {
		case "/":
			fmt.Fprintf(writer, "Hi")
		case "/discover":
			discoverFunc(writer, request, client)
		case "/select":
			selectFunc(writer, request, client)
		case "/help":
			fmt.Fprintf(writer, "We're sorry, help is not yet implemented.\n")
		default:
			fmt.Fprintf(writer, "Command undefined. \n")
		}
	})

	log.Fatal(http.ListenAndServe(portStr, nil))
}

// DiscoverResponse is the response to a Discover query
type DiscoverResponse struct {
	Count  int                       `json:"count"`
	Images []catalog.ImageDescriptor `json:"images"`
}

func discoverFunc(writer http.ResponseWriter, request *http.Request, client *redis.Client) {
	id := catalog.ImageDescriptor{
		FileFormat:   request.FormValue("fileFormat"),
		AcquiredDate: request.FormValue("acquiredDate")}

	if bitDepth, err := strconv.ParseInt(request.FormValue("bitDepth"), 0, 32); err == nil {
		id.BitDepth = int(bitDepth)
	}

	if fileSize, err := strconv.ParseInt(request.FormValue("fileSize"), 0, 64); err == nil {
		id.FileSize = fileSize
	}

	if cloudCover, err := strconv.ParseFloat(request.FormValue("cloudCover"), 64); err == nil {
		id.CloudCover = cloudCover
	}

	if beachfrontScore, err := strconv.ParseFloat(request.FormValue("beachfrontScore"), 64); err == nil {
		id.BeachfrontScore = beachfrontScore
	}
	images := catalog.GetImages("test-images", &id)
	responseDoc := DiscoverResponse{Count: len(images), Images: images}
	bytes, _ := json.Marshal(responseDoc)
	writer.Write(bytes)
}

func selectFunc(writer http.ResponseWriter, request *http.Request, client *redis.Client) {
	query := request.FormValue("q")
	result := client.Get(query)
	fmt.Fprintf(writer, result.Val())
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
