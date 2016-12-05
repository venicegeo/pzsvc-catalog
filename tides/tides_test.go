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

package tides

import "testing"
import "github.com/venicegeo/geojson-go/geojson"

func TestTides(t *testing.T) {
	var (
		err     error
		fci     interface{}
		context Context
	)
	context.TidesURL = "https://bf-tideprediction.int.geointservices.io/"
	if fci, err = geojson.ParseFile("test/fc.geojson"); err != nil {
		t.Errorf("Expected to load file but received: %v", err.Error())
	}
	GetTides(fci.(*geojson.FeatureCollection).Features, context)
}
