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
	"net/url"
	"testing"

	"github.com/venicegeo/geojson-go/geojson"
)

func TestPlanet(t *testing.T) {
	var (
		planetResponse PlanetResponse
		err            error
		response       *http.Response
		fc             *geojson.FeatureCollection
		options        HarvestOptions
	)

	if response, err = doPlanetRequest("GET", "v0/scenes/landsat/", ""); err != nil {
		t.Error(err)
	}
	if planetResponse, fc, err = unmarshalPlanetResponse(response); err != nil {
		t.Error(err)
	}

	switch {
	case len(fc.Features) > 0:
		t.Logf("Length: %v\n", len(fc.Features))
	default:
		t.Errorf("%#v\n", planetResponse)
	}
	if _, err = storePlanetLandsat(fc, options); err != nil {
		t.Error(err)
	}
	options.Cap = 100
	options.URLRoot = "localhost:8080"
	HarvestPlanet(options)
	DropIndex()
}

func TestRecurringURL(t *testing.T) {
	const testRecurringURL = "https://localhost:8080/planet/12345?event=true"
	u := recurringURL("localhost:8080", "12345")
	if u.String() != testRecurringURL {
		t.Errorf("Expected %v, got %v", testRecurringURL, u.String())
	}
}

func TestLandsatIDToS3Path(t *testing.T) {
	u, _ := url.Parse(landsatIDToS3Path("LC81490342016259LGN00") + "index.html")
	resp, err := http.Get(u.String())
	if err != nil {
		t.Error(err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200, got %v from %v", resp.StatusCode, u.String())
	}
}
