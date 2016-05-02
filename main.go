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

package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"gopkg.in/redis.v3"
)

func main() {

	var portStr string
	var args = os.Args[1:]
	if len(args) > 0 {
		portStr = ":" + args[0]
	} else {
		portStr = ":8080"
	}

	var options redis.Options
	options.Addr = "127.0.0.1:6379"
	client := redis.NewClient(&options)
	defer client.Close()

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		request.ParseForm()
		switch request.URL.Path {
		case "/":
			fmt.Fprintf(writer, "Hi")
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

func selectFunc(writer http.ResponseWriter, request *http.Request, client *redis.Client) {
	query := request.FormValue("q")
	result := client.Get(query)
	fmt.Fprintf(writer, result.Val())
}
