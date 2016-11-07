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
	"encoding/json"
	"github.com/paulsmith/gogeos/geos"
	//	"github.com/venicegeo/geojson-geos-go/geojsongeos"
	"github.com/venicegeo/geojson-go/geojson"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
)

func TestPlanet(t *testing.T) {
	var (
		planetResponse PlanetResponse
		err            error
		response       *http.Response
		fc             *geojson.FeatureCollection
	)

	if response, err = doPlanetRequest("GET", "v0/scenes/ortho/", ""); err != nil {
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

func TestHarvestPlanet(t *testing.T) {
	var harvestOptionsHolder HarvestOptions
	var harvestFilterHolder HarvestFilter
	var whitelist FeatureLayer
	var blacklist FeatureLayer
	var line1, line2 *geos.Geometry
	line1, _ = geos.FromWKT("LINESTRING (0 0, 10 10, 20 20)")
	line2, _ = geos.FromWKT("LINESTRING (5 0, 15 15, 17 17)")
	whitelist.FeatureType = "test"
	whitelist.GeoJSON = map[string]interface{}{"acquiredDate": "today", "sensorName": "2", "resolution": 3, "classification": "UNCLASSIFIED"}
	whitelist.TileMap = map[string]*geos.Geometry{"Testing": line2}
	whitelist.WfsURL = "Test.com"
	blacklist.FeatureType = "test"
	blacklist.GeoJSON = map[string]interface{}{"acquiredDate": "today", "sensorName": "2", "resolution": 3, "classification": "UNCLASSIFIED"}
	blacklist.TileMap = map[string]*geos.Geometry{"Testing": line1}
	blacklist.WfsURL = "Test.com"

	harvestFilterHolder.BlackList = blacklist
	harvestFilterHolder.WhiteList = whitelist

	harvestOptionsHolder.Event = true
	harvestOptionsHolder.Reharvest = true
	harvestOptionsHolder.PlanetKey = "aaaaaaaaaaaaaaaaaaaaaa1111"
	harvestOptionsHolder.PiazzaGateway = "Test.com"
	harvestOptionsHolder.PiazzaAuthorization = "Test123"
	harvestOptionsHolder.Filter = harvestFilterHolder
	harvestOptionsHolder.Cap = 25
	harvestOptionsHolder.URLRoot = "systems"
	harvestOptionsHolder.Recurring = false
	harvestOptionsHolder.RequestPageSize = 10
	harvestOptionsHolder.callback = nil
	harvestOptionsHolder.EventTypeID = "abc123"

	HarvestPlanet(harvestOptionsHolder)
}

func TestStorePlanetLandsat(t *testing.T) {
	var harvestOptionsHolder HarvestOptions
	var pathHolder [3]string
	//pathHolder[0] = "../data/harvest_options1.json"
	pathHolder[0] = "../data/harvest_options2.json"
	pathHolder[2] = "../data/harvest_options3.json"
	for _, path := range pathHolder {

		bytes, _ := ioutil.ReadFile(path)

		_ = json.Unmarshal(bytes, &harvestOptionsHolder)

		_ = harvestOptionsHolder.Filter.PrepareGeometries()
		var geoCollectionHolder *geojson.FeatureCollection
		geoCollectionHolder, _ = geojson.FeatureCollectionFromBytes([]byte(`{"type": "FeatureCollection","features":[{"type":"Feature","geometry":{"coordinates":[[-41.68380384,-3.86901559],[-41.68344951,-3.86733807],[-41.68361042,-3.86726774],[-41.68384764,-3.86719616],[-41.68413582,-3.86716065],[-41.68444963,-3.86719857],[-41.68476372,-3.86734723],[-41.68505276,-3.86764398],[-41.68529141,-3.86812615],[-41.68537007,-3.86836772],[-41.68542289,-3.8685737],[-41.68544916,-3.86875115],[-41.68544817,-3.86890711],[-41.6854192,-3.86904862],[-41.68536156,-3.86918273],[-41.68527452,-3.86931649],[-41.68515738,-3.86945693],[-41.68495458,-3.86964114],[-41.68475013,-3.86975328],[-41.68454967,-3.8697952],[-41.68435881,-3.86976873],[-41.68418317,-3.86967571],[-41.68402839,-3.86951795],[-41.68390007,-3.8692973],[-41.68380384,-3.86901559]],"type":"LineString"},"properties":{"24hrMaxTide":"4.272558868170382","24hrMinTide":"2.4257490639311676","algoCmd":"ossim-cli shoreline --image img1.TIF,img2.TIF --projection geo-scaled --prop 24hrMinTide:2.4257490639311676 --prop resolution:30 --prop classification:Unclassified --prop dataUsage:Not_to_be_used_for_navigational_or_targeting_purposes. --prop sensorName:Landsat8 --prop 24hrMaxTide:4.272558868170382 --prop currentTide:3.4136017245233523 --prop sourceID:landsat:LC82190622016285LGN00 --prop dateTimeCollect:2016-10-11T12:59:05.157475+00:00 shoreline.geojson","algoName":"BF_Algo_NDWI","algoProcTime":"20161031.133058.4026","algoVersion":"0.0","classification":"Unclassified","currentTide":"3.4136017245233523","dataUsage":"Not_to_be_used_for_navigational_or_targeting_purposes.","dateTimeCollect":"2016-10-11T12:59:05.157475+00:00","resolution":"30","sensorName":"Landsat8","sourceID":"landsat:LC82190622016285LGN00"}}]}`))
		t.Log(geoCollectionHolder)
		_, _ = storePlanetLandsat(geoCollectionHolder, harvestOptionsHolder)
	}
}

func TestPlanetRecurring(t *testing.T) {
	var harvestOptionsHolder HarvestOptions
	var pathHolder [3]string
	pathHolder[0] = "../data/harvest_options1.json"
	pathHolder[1] = "../data/harvest_options2.json"
	pathHolder[2] = "../data/harvest_options3.json"
	for _, path := range pathHolder {

		bytes, _ := ioutil.ReadFile(path)

		_ = json.Unmarshal(bytes, &harvestOptionsHolder)

		_ = harvestOptionsHolder.Filter.PrepareGeometries()

		_, _, _ = PlanetRecurring("test.com", harvestOptionsHolder)
	}

}
