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
	//"github.com/venicegeo/pzsvc-image-catalog/catalog"
	//"github.com/venicegeo/pzsvc-lib"
	//"net"
	"net/http"
	//"os"
	"github.com/venicegeo/pzsvc-lib"
	"testing"
)

func TestPlanetHandler(t *testing.T) {
	w, outStr, outInt := pzsvc.GetMockResponseWriter()
	r := http.Request{}
	r.Method = "POST"
	testBodyStr := `{"algoType":"pzsvc-ossim","svcURL":"https://pzsvc-ossim/execute","pzAuthToken":"1a2q3w4r5t6y7u","pzAddr":"https://pz-gateway.io","bandMergeType":"","bandMergeURL":"","tideURL":"https://tideprediction.io/","dbAuthToken":"","bands":["coastal","swir1"],"metaDataJSON":{"type": "Feature","geometry": {"type": "Polygon","coordinates": [[[35.0552646979563, -20.6249573123582],[36.814147099668, -20.9863928375569],[36.4165176126861, -22.719959598174],[34.6366754134012, -22.3522722379786],[35.0552646979563, -20.6249573123582]]]},"properties": {"acquiredDate": "2016-06-18T07:36:07.536703+00:00","bands": {"blue": "http://landsat_B2.TIF","cirrus": "http://landsat_B9.TIF","coastal": "http://landsat_B1.TIF","green": "http://landsat_B3.TIF","nir": "http://landsat_B5.TIF","panchromatic": "http://landsat_B8.TIF","red": "http://landsat_B4.TIF","swir1": "http://landsat_B6.TIF","swir2": "http://landsat_B7.TIF","tirs1": "http://landsat_B10.TIF","tirs2": "http://landsat_B11.TIF"},"cloudCover": 8.6,"path": "http://landsat.com/index.html","resolution": 30,"sensorName": "Landsat8","thumb_large": "http://landsat_thumb_large.jpg","thumb_small": "http://landsat_thumb_small.jpg"},"id": "landsat:LC81660752016170LGN00","bbox": [34.6366754134012, -22.719959598174, 36.814147099668, -20.6249573123582]}}`
	r.Body = pzsvc.GetMockReadCloser(testBodyStr)
	w.Header().Set("Authorization", "1a2q3w4r5t6y7u")
	r.Host = "https://pz-gateway.io"
	*outStr = ""
	*outInt = 200
	outStrs := []string{`{"test":"blah"}`}
	pzsvc.SetMockClient(outStrs, 200)
	planetHandler(w, &r)
	r.Method = "POST"
	testBodyStr = `{"algoType":"pzsvc-ossim","svcURL":"https://pzsvc-ossim/execute","pzAuthToken":"1a2q3w4r5t6y7u","pzAddr":"https://pz-gateway.io","bandMergeType":"","bandMergeURL":"","tideURL":"https://tideprediction.io/","dbAuthToken":"","bands":["coastal","swir1"],"metaDataJSON":{"type": "Feature","geometry": {"type": "Polygon","coordinates": [[[35.0552646979563, -20.6249573123582],[36.814147099668, -20.9863928375569],[36.4165176126861, -22.719959598174],[34.6366754134012, -22.3522722379786],[35.0552646979563, -20.6249573123582]]]},"properties": {"acquiredDate": "2016-06-18T07:36:07.536703+00:00","bands": {"blue": "http://landsat_B2.TIF","cirrus": "http://landsat_B9.TIF","coastal": "http://landsat_B1.TIF","green": "http://landsat_B3.TIF","nir": "http://landsat_B5.TIF","panchromatic": "http://landsat_B8.TIF","red": "http://landsat_B4.TIF","swir1": "http://landsat_B6.TIF","swir2": "http://landsat_B7.TIF","tirs1": "http://landsat_B10.TIF","tirs2": "http://landsat_B11.TIF"},"cloudCover": 8.6,"path": "http://landsat.com/index.html","resolution": 30,"sensorName": "Landsat8","thumb_large": "http://landsat_thumb_large.jpg","thumb_small": "http://landsat_thumb_small.jpg"},"id": "landsat:LC81660752016170LGN00","bbox": [34.6366754134012, -22.719959598174, 36.814147099668, -20.6249573123582]}}`
	r.Body = pzsvc.GetMockReadCloser(testBodyStr)
	//r.Header.Add("Authorization", "1a2q3w4r5t6y7u")
	*outStr = ""
	*outInt = 400
	planetHandler(w, &r)
}
