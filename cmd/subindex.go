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
	"io/ioutil"
	"net/http"

	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

func subindexHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		bytes     []byte
		err       error
		subindex  catalog.Subindex
		errorText string
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

	switch request.Method {
	case "POST":
		defer request.Body.Close()
		if bytes, err = ioutil.ReadAll(request.Body); err != nil {
			errorText += err.Error() + "\n"
		}

		if err = json.Unmarshal(bytes, &subindex); err != nil {
			errorText += err.Error() + "\n"
		}
		if subindex.WfsURL == "" {
			subindex.WfsURL = request.FormValue("wfsurl")
		}
		if subindex.WfsURL == "" {
			errorText += "Posts to /subindex must contain a WFS URL.\n"
		}

		if subindex.FeatureType == "" {
			subindex.FeatureType = request.FormValue("featureType")
		}
		if subindex.WfsURL == "" {
			errorText += "Posts to /subindex must contain a Feature Type.\n"
		}

		if subindex.Name == "" {
			subindex.Name = request.FormValue("name")
		}
		if subindex.Name == "" {
			errorText += "Posts to /subindex must contain a name.\n"
		}

		subindex.ResolveKey()
		if err := catalog.CreateSubindex(subindex); err == nil {
			bytes, _ = json.Marshal(subindex)
		} else {
			errorText += err.Error() + "\n"
		}
	case "GET":
		subindexes := catalog.Subindexes()
		if bytes, err = json.Marshal(subindexes); err != nil {
			errorText += err.Error() + "\n"
		}
	}

	if errorText == "" {
		writer.Header().Set("Content-Type", "application/json")
		writer.Write(bytes)
	} else {
		http.Error(writer, errorText, http.StatusBadRequest)
	}
}
