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

package planet

import (
	"fmt"
	"os"
	"testing"

	"github.com/venicegeo/geojson-go/geojson"
)

func TestPlanet(t *testing.T) {
	var (
		options  SearchOptions
		err      error
		response string
		pi       interface{}
	)

	options.PlanetKey = os.Getenv("PL_API_KEY")

	body := `{ 
   "item_types":[  
      "REOrthoTile"
   ],
   "filter":{  
      "type":"AndFilter",
      "config":[
        ]
      }
}`

	if _, err = doRequest(doRequestInput{method: "POST", inputURL: "data/v1/quick-search", body: []byte(body), contentType: "application/json"}, RequestContext{PlanetKey: options.PlanetKey}); err != nil {
		t.Errorf("Expected request to succeed; received: %v", err.Error())
	}
	if pi, err = geojson.ParseFile("test/polygon.geojson"); err == nil {
		if _, err = GetScenes(geojson.NewFeature(pi, "", nil), options); err != nil {
			t.Errorf("Expected GetScenes to succeed; received: %v", err.Error())
		}
	} else {
		t.Errorf("Expected to read test polygon; received: %v", err.Error())
	}
	feature := geojson.NewFeature(nil, "", nil)
	if feature.Bbox, err = geojson.NewBoundingBox("139,50,140,51"); err != nil {
		t.Errorf("Expected NewBoundingBox to succeed; received: %v", err.Error())
	}
	if response, err = GetScenes(feature, options); err != nil {
		t.Errorf("Expected GetScenes to succeed; received: %v", err.Error())
	}
	feature.Properties["cloudCover"] = 0.01
	if response, err = GetScenes(feature, options); err != nil {
		t.Errorf("Expected GetScenes to succeed; received: %v", err.Error())
	}
	feature.Properties["acquiredDate"] = "2016-01-01T00:00:00Z"
	if response, err = GetScenes(feature, options); err != nil {
		t.Errorf("Expected GetScenes to succeed; received: %v", err.Error())
	}
	options.Tides = true
	options.TidesURL = "https://bf-tideprediction.int.geointservices.io/tides"
	if response, err = GetScenes(feature, options); err != nil {
		t.Errorf("Expected GetScenes to succeed; received: %v", err.Error())
	}
	fmt.Print(response)
}

func TestActivation(t *testing.T) {
	var (
		context  RequestContext
		response []byte
		err      error
	)
	context.PlanetKey = os.Getenv("PL_API_KEY")
	id := "20161203_021824_5462311_RapidEye-1"
	if response, err = Activate(id, context); err != nil {
		t.Errorf("Failed to activate; received: %v", err.Error())
	}
	fmt.Print(string(response))

}
