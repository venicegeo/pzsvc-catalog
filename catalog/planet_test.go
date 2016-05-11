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

package catalog

import (
	"net/http"
	"testing"

	"github.com/venicegeo/geojson-go/geojson"
)

func TestPlanet(t *testing.T) {
	var (
		planetResponse PlanetResponse
		err            error
		response       *http.Response
		fc             *geojson.FeatureCollection
	)

	if response, err = DoPlanetRequest("GET", "v0/scenes/ortho/"); err != nil {
		t.Error(err)
	}
	if planetResponse, fc, err = UnmarshalPlanetResponse(response); err != nil {
		t.Error(err)
	}

	switch {
	case len(fc.Features) > 0:
		t.Logf("Length: %v\n", len(fc.Features))
	default:
		t.Errorf("%#v\n", planetResponse)
	}
}
