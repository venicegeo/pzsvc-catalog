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

	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
	"github.com/venicegeo/pzsvc-lib"

	"gopkg.in/redis.v3"
)

func serve(redisClient *redis.Client) {

	portStr := ":8080"
	var (
		err error
	)
	if redisClient == nil {
		if redisClient, err = catalog.RedisClient(); err != nil {
			log.Fatalf("Failed to create Redis client: %v", err.Error())
		}
	}
	defer redisClient.Close()
	if info := redisClient.Info(); info.Err() == nil {
		router := mux.NewRouter()

		router.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
			fmt.Fprintf(writer, "Hi")
		})
		router.HandleFunc("/dropIndex", dropIndexHandler)
		router.HandleFunc("/eventTypeID", eventTypeIDHandler)
		router.HandleFunc("/image/{id}", imageHandler)
		router.HandleFunc("/discover", discoverHandler)
		router.HandleFunc("/planet", planetHandler)
		router.HandleFunc("/planet/{key}", planetRecurringHandler)
		router.HandleFunc("/unharvest", unharvestHandler)
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

func dropIndexHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		pzGateway string
		pzAuth    string
	)
	pzGateway = r.FormValue("pzGateway")
	pzAuth = r.Header.Get("Authorization")
	if err = pzsvc.TestPiazzaAuth(pzGateway, pzAuth); err != nil {
		if httpError, ok := err.(*pzsvc.HTTPError); ok {
			http.Error(w, httpError.Message, httpError.Status)
		} else {
			http.Error(w, "Unable to attempt authentication: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	response := catalog.DropIndex()
	w.Write([]byte(fmt.Sprintf("Dropped existing index, deleted %v entries.\n", response)))
}

func imageHandler(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	id := vars["id"]
	if metadata, err := catalog.GetSceneMetadata(id); err == nil {
		bytes, _ := json.Marshal(metadata)
		writer.Write(bytes)
	} else {
		switch err.Error() {
		case "redis: nil":
			message := fmt.Sprintf("Scene %v not found.", id)
			http.Error(writer, message, http.StatusNotFound)
		default:
			message := fmt.Sprintf("Unable to retrieve metadata for %v: %v", id, err.Error())
			http.Error(writer, message, http.StatusBadRequest)
		}
	}
}

func provisionHandler(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	id := vars["id"]
	band := vars["band"]
	if metadata, err := catalog.GetSceneMetadata(id); err != nil {
		message := fmt.Sprintf("Unable to retrieve metadata for %v: %v", id, err.Error())
		http.Error(writer, message, http.StatusBadRequest)
	} else {
		bytes, _ := json.Marshal(metadata)
		gj, _ := geojson.FeatureFromBytes(bytes)
		bandValue := gj.Properties["bands"].(map[string]interface{})[band].(string)
		writer.Write([]byte(bandValue))
	}
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Serve Catalog",
	Long: `
Serve the image catalog`,
	Run: func(cmd *cobra.Command, args []string) {
		serve(nil)
	},
}
