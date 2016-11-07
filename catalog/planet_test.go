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
	"github.com/paulsmith/gogeos/geos"
	//	"github.com/venicegeo/geojson-geos-go/geojsongeos"
	"github.com/venicegeo/geojson-go/geojson"
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
	var harvestFilterHolder HarvestFilter
	var whitelist FeatureLayer
	var blacklist FeatureLayer
	var line1, line2 *geos.Geometry

	var geoCollectionHolder *geojson.FeatureCollection
	geoCollectionHolder, _ = geojson.FeatureCollectionFromBytes([]byte(`{"type": "FeatureCollection","features":[{"type": "Feature",   "properties": {},"geometry":{"type":"Polygon","coordinates":[[[1,5],[2,5],[2,6],[1,6],[1,5]]]}}]}`))

	//var err error
	line1, _ = geos.FromWKT("LINESTRING(5 0, 15 15, 17 17)")
	line2, _ = geos.FromWKT("LINESTRING(5 0, 15 15, 17 17)")
	whitelist.FeatureType = "test"
	whitelist.GeoJSON = geoCollectionHolder.Map()
	whitelist.TileMap = map[string]*geos.Geometry{"Testing": line1}
	whitelist.WfsURL = "Test.com"
	blacklist.FeatureType = "test"
	blacklist.GeoJSON = geoCollectionHolder.Map()
	blacklist.TileMap = map[string]*geos.Geometry{"Testing": line2}
	blacklist.WfsURL = "Test.com"

	harvestFilterHolder.BlackList = blacklist
	harvestFilterHolder.WhiteList = whitelist

	harvestOptionsHolder.Event = false
	harvestOptionsHolder.Reharvest = false
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

	/*
		var tester bool
		var err error
		var harvestGeom *geos.Geometry
		for _, curr := range geoCollectionHolder.Features {
		curr.Properties["cloud_cover"].(map[string]interface{})["estimated"] = 6.63
		curr.Properties["image_statistics"].(map[string]interface{})["gsd"] = 24.
		curr.Properties["acquired"] = "yes"
		curr.ID = "123abc"
		tester = passHarvestFilter(harvestOptionsHolder, curr)
		t.Log(tester)
		harvestGeom, _ = geojsongeos.GeosFromGeoJSON(curr)
		tester, err = whitelist.Disjoint(harvestGeom)
		t.Log(tester)
		t.Log(err)
		tester, err = blacklist.Intersects(harvestGeom)
		t.Log(tester)
		t.Log(err)

	}*/
	_, _ = storePlanetLandsat(geoCollectionHolder, harvestOptionsHolder)

}

func TestPlanetRecurring(t *testing.T) {
	var harvestOptionsHolder HarvestOptions
	var harvestFilterHolder HarvestFilter
	var whitelist FeatureLayer
	var blacklist FeatureLayer
	var line1, line2 *geos.Geometry

	var geoCollectionHolder *geojson.FeatureCollection
	geoCollectionHolder, _ = geojson.FeatureCollectionFromBytes([]byte(`{"type": "FeatureCollection","features":[{"type": "Feature",   "properties": {},"geometry":{"type":"Polygon","coordinates":[[[1,5],[2,5],[2,6],[1,6],[1,5]]]}}]}`))

	//var err error
	line1, _ = geos.FromWKT("LINESTRING(5 0, 15 15, 17 17)")
	line2, _ = geos.FromWKT("LINESTRING(5 0, 15 15, 17 17)")
	whitelist.FeatureType = "test"
	whitelist.GeoJSON = geoCollectionHolder.Map()
	whitelist.TileMap = map[string]*geos.Geometry{"Testing": line1}
	whitelist.WfsURL = "Test.com"
	blacklist.FeatureType = "test"
	blacklist.GeoJSON = geoCollectionHolder.Map()
	blacklist.TileMap = map[string]*geos.Geometry{"Testing": line2}
	blacklist.WfsURL = "Test.com"

	harvestFilterHolder.BlackList = blacklist
	harvestFilterHolder.WhiteList = whitelist

	harvestOptionsHolder.Event = false
	harvestOptionsHolder.Reharvest = false
	harvestOptionsHolder.PlanetKey = "aaaaaaaaaaaaaaaaaaaaaa1111"
	harvestOptionsHolder.PiazzaGateway = "https://pz-gateway.geointservices.io"
	harvestOptionsHolder.PiazzaAuthorization = "Test123"
	harvestOptionsHolder.Filter = harvestFilterHolder
	harvestOptionsHolder.Cap = 25
	harvestOptionsHolder.URLRoot = "systems"
	harvestOptionsHolder.Recurring = false
	harvestOptionsHolder.RequestPageSize = 10
	harvestOptionsHolder.callback = nil
	harvestOptionsHolder.EventTypeID = "abc123"

	_, _, _ = PlanetRecurring("test.com", harvestOptionsHolder)

}
