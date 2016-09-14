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
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
	"github.com/venicegeo/pzsvc-lib"
)

func discoverHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		responseString string
		err            error
		options        *catalog.SearchOptions
		sf             *geojson.Feature
	)
	if pzsvc.Preflight(writer, request) {
		return
	}

	if options, err = searchOptions(request); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	if sf, err = searchFeature(request); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	if !options.NoCache &&
		(len(sf.Bbox) == 0) &&
		(sf.PropertyString("acquiredDate") == "") &&
		(sf.PropertyString("maxAcquiredDate") == "") {
		http.Error(writer, "A discovery request must contain at least one of the following:\n* bounding box\n* acquiredDate\n* maxAcquiredDate", http.StatusBadRequest)
		return
	}
	if _, responseString, err = catalog.GetScenes(sf, *options); err == nil {
		writer.Header().Set("Content-Type", "application/json")
		writer.Write([]byte(responseString))
	} else {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func searchOptions(request *http.Request) (*catalog.SearchOptions, error) {
	var (
		count         int
		startIndex    int
		err           error
		parsedCount   int64
		startIndexI64 int64
	)

	nocache, _ := strconv.ParseBool(request.FormValue("nocache"))

	// Give ourselves a resonable default and maximum count (when caching)
	if parsedCount, err = strconv.ParseInt(request.FormValue("count"), 0, 32); err == nil {
		if !nocache {
			count = int(math.Min(float64(parsedCount), 1000))
		}
	} else {
		count = 20
	}

	if startIndexI64, err = strconv.ParseInt(request.FormValue("startIndex"), 0, 32); err == nil {
		startIndex = int(startIndexI64)
	}

	options := catalog.SearchOptions{
		MinimumIndex: startIndex,
		Count:        count,
		MaximumIndex: startIndex + count - 1,
		NoCache:      nocache}
	return &options, nil
}

func searchFeature(request *http.Request) (*geojson.Feature, error) {
	var (
		err             error
		bitDepth        int64
		fileSize        int64
		acquiredDate    string
		maxAcquiredDate string
		bandsString     string
		bboxString      string
		subIndex        string
		cloudCover      float64
		resolution      float64
		beachfrontScore float64
	)
	// Put most of the parameters into a properties map
	properties := make(map[string]interface{})

	if fileFormat := request.FormValue("fileFormat"); fileFormat != "" {
		properties["fileFormat"] = fileFormat
	}

	if acquiredDate = request.FormValue("acquiredDate"); acquiredDate != "" {
		if _, err = time.Parse(time.RFC3339, acquiredDate); err != nil {
			return nil, pzsvc.ErrWithTrace("Format of acquiredDate is invalid:  " + err.Error())
		}
		properties["acquiredDate"] = acquiredDate
	}

	if maxAcquiredDate = request.FormValue("maxAcquiredDate"); maxAcquiredDate != "" {
		if _, err = time.Parse(time.RFC3339, maxAcquiredDate); err != nil {
			return nil, pzsvc.ErrWithTrace("Format of maxAcquiredDate is invalid:  " + err.Error())
		}
		properties["maxAcquiredDate"] = maxAcquiredDate
	}

	if bitDepth, err = strconv.ParseInt(request.FormValue("bitDepth"), 0, 32); err == nil {
		properties["bitDepth"] = int(bitDepth)
	}

	if resolution, err = strconv.ParseFloat(request.FormValue("resolution"), 32); err == nil {
		properties["resolution"] = resolution
	}

	if fileSize, err = strconv.ParseInt(request.FormValue("fileSize"), 0, 64); err == nil {
		properties["fileSize"] = fileSize
	}

	if cloudCover, err = strconv.ParseFloat(request.FormValue("cloudCover"), 64); err == nil {
		properties["cloudCover"] = cloudCover
	}

	if beachfrontScore, err = strconv.ParseFloat(request.FormValue("beachfrontScore"), 64); err == nil {
		properties["beachfrontScore"] = beachfrontScore
	}

	if sensorName := request.FormValue("sensorName"); sensorName != "" {
		properties["sensorName"] = sensorName
	}

	if bandsString = request.FormValue("bands"); bandsString != "" {
		bands := strings.Split(bandsString, ",")
		properties["bands"] = bands
	}

	if subIndex = request.FormValue("subIndex"); subIndex != "" {
		properties["subIndex"] = subIndex
	}
	result := geojson.NewFeature(nil, "", properties)

	bboxString = request.FormValue("bbox")
	if result.Bbox, err = geojson.NewBoundingBox(bboxString); err == nil {
		if result.Bbox.Antimeridian() {
			return nil, pzsvc.ErrWithTrace("Bounding Box must not cross the antimeridian.")
		}
	} else {
		return nil, pzsvc.ErrWithTrace("Unable to parse Bounding Box: " + err.Error())
	}
	return result, nil
}
