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
	"os"
	"testing"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

func TestPlanet(t *testing.T) {
	var (
		options catalog.SearchOptions
		err     error
		pi      interface{}
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

	if _, err = doRequest(doRequestInput{method: "POST", inputURL: "data/v1/quick-search", body: []byte(body), contentType: "application/json"}, doRequestContext{planetKey: options.PlanetKey}); err != nil {
		t.Errorf("Expected request to succeed; received: %v", err.Error())
	}
	if pi, err = geojson.ParseFile("test/polygon.geojson"); err == nil {
		if _, err = GetScenes(geojson.NewFeature(pi, "", nil), options); err != nil {
			t.Errorf("Expected GetScenes to succeed; received: %v", err.Error())
		}
	} else {
		t.Errorf("Expected to read test polygon; received: %v", err.Error())
	}
}
