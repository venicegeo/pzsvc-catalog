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
	"io/ioutil"
	"log"
	"testing"

	"github.com/venicegeo/geojson-go/geojson"
)

func TestHarvest(t *testing.T) {
	testHarvestProcess(t, "../data/harvest_options1.json", true)
	testHarvestProcess(t, "../data/harvest_options2.json", true)
	testHarvestProcess(t, "../data/harvest_options3.json", false)
}

func testHarvestProcess(t *testing.T, filename string, doWeCare bool) {
	var (
		bytes []byte
		err   error
		ho    HarvestOptions
		c3    [][][]float64
		c2    [][]float64
		c11   []float64
		c12   []float64
		c13   []float64
		c14   []float64
	)
	if bytes, err = ioutil.ReadFile(filename); err != nil {
		//t.Error(err.Error())
	}
	if err = json.Unmarshal(bytes, &ho); err != nil {
		//t.Error(err.Error())
	}
	if err = ho.Filter.PrepareGeometries(); err != nil {
		//t.Error(err.Error())
	}
	// Verify that preparing the geometries did something useful
	if doWeCare && len(ho.Filter.WhiteList.TileMap) == 0 {
		//t.Error("Created no tiles in WhiteList tilemap")
		t.FailNow()
	}
	if doWeCare && len(ho.Filter.BlackList.TileMap) == 0 {
		//t.Error("Created no tiles in BlackList tilemap")
		t.FailNow()
	}
	c3 = append(c3, append(c2, append(c11, -180, -90), append(c12, 180, 90), append(c13, -180, 90), append(c14, -180, -90)))
	feature := geojson.NewFeature(geojson.NewPolygon(c3), "99999", nil)
	if passHarvestFilter(ho, feature) && doWeCare {
		//t.Errorf("Expected harvest filter to fail (blacklist). %v", filename)
		log.Printf("BL: %v", ho.Filter.BlackList.TileMap)
		log.Printf("f: %v", feature.String())
	}
	c3[0][0][1] = -50
	c3[0][1][1] = -40
	c3[0][2][1] = -40
	c3[0][3][1] = -50
	feature = geojson.NewFeature(geojson.NewPolygon(c3), "99999", nil)
	if !passHarvestFilter(ho, feature) && doWeCare {
		//t.Errorf("Expected harvest filter to succeed. %v", filename)
		log.Printf("f: %v", feature.String())
	}
	c3[0][0][0] = -160
	c3[0][0][1] = 0
	c3[0][1][0] = -159
	c3[0][1][1] = 2
	c3[0][2][0] = -160
	c3[0][2][1] = 2
	c3[0][3][0] = -160
	c3[0][3][1] = 0
	feature = geojson.NewFeature(geojson.NewPolygon(c3), "99999", nil)
	if passHarvestFilter(ho, feature) && doWeCare {
		//t.Errorf("Expected harvest filter to fail (whitelist). %v", filename)
		log.Printf("WL: %v", ho.Filter.WhiteList.TileMap)
		log.Printf("f: %v", feature.String())
	}
}

func TestRecurring(t *testing.T) {
	var err error
	ho := HarvestOptions{Recurring: true, Reharvest: true}
	recurringKey := prefix + ":" + recurringRoot
	if err = StoreRecurring(recurringKey, ho); err != nil {
		//t.Errorf("Failed to store recurring: %v", err.Error())
	}
	if err = DeleteRecurring(recurringKey); err != nil {
		//t.Errorf("Failed to store recurring: %v", err.Error())
	}
}

func TestIssueEvent(t *testing.T) {
	var harvOptionHolder HarvestOptions
	var callback func(error)

	var geoCollectionHolder *geojson.FeatureCollection
	geoCollectionHolder, _ = geojson.FeatureCollectionFromBytes([]byte(`{"type": "FeatureCollection","features":[{"type":"Feature","geometry":{"coordinates":[[-41.68380384,-3.86901559],[-41.68344951,-3.86733807],[-41.68361042,-3.86726774],[-41.68384764,-3.86719616],[-41.68413582,-3.86716065],[-41.68444963,-3.86719857],[-41.68476372,-3.86734723],[-41.68505276,-3.86764398],[-41.68529141,-3.86812615],[-41.68537007,-3.86836772],[-41.68542289,-3.8685737],[-41.68544916,-3.86875115],[-41.68544817,-3.86890711],[-41.6854192,-3.86904862],[-41.68536156,-3.86918273],[-41.68527452,-3.86931649],[-41.68515738,-3.86945693],[-41.68495458,-3.86964114],[-41.68475013,-3.86975328],[-41.68454967,-3.8697952],[-41.68435881,-3.86976873],[-41.68418317,-3.86967571],[-41.68402839,-3.86951795],[-41.68390007,-3.8692973],[-41.68380384,-3.86901559]],"type":"LineString"},"properties":{"24hrMaxTide":"4.272558868170382","24hrMinTide":"2.4257490639311676","algoCmd":"ossim-cli shoreline --image img1.TIF,img2.TIF --projection geo-scaled --prop 24hrMinTide:2.4257490639311676 --prop resolution:30 --prop classification:Unclassified --prop dataUsage:Not_to_be_used_for_navigational_or_targeting_purposes. --prop sensorName:Landsat8 --prop 24hrMaxTide:4.272558868170382 --prop currentTide:3.4136017245233523 --prop sourceID:landsat:LC82190622016285LGN00 --prop dateTimeCollect:2016-10-11T12:59:05.157475+00:00 shoreline.geojson","algoName":"BF_Algo_NDWI","algoProcTime":"20161031.133058.4026","algoVersion":"0.0","classification":"Unclassified","currentTide":"3.4136017245233523","dataUsage":"Not_to_be_used_for_navigational_or_targeting_purposes.","dateTimeCollect":"2016-10-11T12:59:05.157475+00:00","resolution":"30","sensorName":"Landsat8","sourceID":"landsat:LC82190622016285LGN00"}}]}`))
	geoFeatureArray := geoCollectionHolder.Features
	for _, feature := range geoFeatureArray {
		harvOptionHolder.Event = true
		harvOptionHolder.Reharvest = false
		harvOptionHolder.PlanetKey = "Enter API Key"
		harvOptionHolder.PiazzaGateway = "test.com"
		harvOptionHolder.PiazzaAuthorization = "none"
		harvOptionHolder.Cap = 2
		harvOptionHolder.URLRoot = "test.com"
		harvOptionHolder.Recurring = false
		harvOptionHolder.RequestPageSize = 20
		harvOptionHolder.EventTypeID = "123"

		issueEvent(harvOptionHolder, feature, callback)
	}
}
