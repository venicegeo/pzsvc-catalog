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
		t.Error(err.Error())
	}
	if err = json.Unmarshal(bytes, &ho); err != nil {
		t.Error(err.Error())
	}
	if err = ho.Filter.PrepareGeometries(); err != nil {
		t.Error(err.Error())
	}
	c3 = append(c3, append(c2, append(c11, -180, -90), append(c12, 180, 90), append(c13, -180, 90), append(c14, -180, -90)))
	feature := geojson.NewFeature(geojson.NewPolygon(c3), "99999", nil)
	if passHarvestFilter(ho, feature) && doWeCare {
		t.Errorf("Expected harvest filter to fail (blacklist). %v", filename)
		log.Printf("BL: %v", ho.Filter.BlackList.TileMap)
		log.Printf("f: %v", feature.String())
	}
	c3[0][0][1] = -50
	c3[0][1][1] = -40
	c3[0][2][1] = -40
	c3[0][3][1] = -50
	feature = geojson.NewFeature(geojson.NewPolygon(c3), "99999", nil)
	if !passHarvestFilter(ho, feature) && doWeCare {
		t.Errorf("Expected harvest filter to succeed. %v", filename)
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
		t.Errorf("Expected harvest filter to fail (whitelist). %v", filename)
		log.Printf("WL: %v", ho.Filter.WhiteList.TileMap)
		log.Printf("f: %v", feature.String())
	}
}
