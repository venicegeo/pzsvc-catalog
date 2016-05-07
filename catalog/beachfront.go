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
	"time"

	"github.com/venicegeo/geojson-go/geojson"
)

// ImageDescriptors is the response to a Discover query
type ImageDescriptors struct {
	Count      int               `json:"count"`
	StartIndex int               `json:"startIndex"`
	Images     []ImageDescriptor `json:"images"`
}

// ImageDescriptor is the descriptor of a specific catalog entry
type ImageDescriptor struct {
	ID              string              `json:"id"`
	Path            string              `json:"path"` //URI
	ThumbnailPath   string              `json:"thumbnailPath,omitempty"`
	PreviewPath     string              `json:"previewPath,omitempty"`
	FileFormat      string              `json:"fileFormat"`
	BoundingBox     geojson.BoundingBox `json:"bbox"`
	AcquiredDate    string              `json:"acquiredDate,omitempty"`
	CloudCover      float64             `json:"cloudCover,omitempty"`
	BitDepth        int                 `json:"bitDepth,omitempty"`
	BeachfrontScore float64             `json:"beachfrontScore,omitempty"`
	FileSize        int64               `json:"fileSize,omitempty"`
}

// GetImages returns images for the given set matching the criteria in the options
func GetImages(set string, options *ImageDescriptor) (ImageDescriptors, string) {
	var (
		result     ImageDescriptors
		bytes      []byte
		resultText string
	)
	red := RedisClient(nil)

	bytes, _ = json.Marshal(options)
	key := set + string(bytes)
	queryExists := client.Exists(key)
	if queryExists.Val() {
		resultText = red.Get(key).Val()
		json.Unmarshal([]byte(resultText), &result)
	} else {
		members := client.SMembers(set)
		for _, curr := range members.Val() {
			var (
				cid      ImageDescriptor
				idString string
			)
			idString = red.Get(curr).Val()
			json.Unmarshal([]byte(idString), &cid)
			if cid.pass(options) {
				result.Images = append(result.Images, cid)
			}
		}
		result.Count = len(result.Images)
		bytes, _ = json.Marshal(result)
		resultText = string(bytes)
		duration, _ := time.ParseDuration("24h")
		client.Set(key, resultText, duration)
	}
	return result, resultText
}

// pass returns true if the receiving object complies
// with all of the properties in the input
func (id *ImageDescriptor) pass(test *ImageDescriptor) bool {
	if test == nil {
		return false
	}
	if test.CloudCover != 0 && id.CloudCover != 0 {
		if id.CloudCover > test.CloudCover {
			return false
		}
	}
	if test.BitDepth != 0 && id.BitDepth != 0 {
		if id.BitDepth < test.BitDepth {
			return false
		}
	}
	if test.BeachfrontScore != 0 && id.BeachfrontScore != 0 {
		if id.BeachfrontScore < test.BeachfrontScore {
			return false
		}
	}
	if test.AcquiredDate != "" {
		var (
			idTime, testTime time.Time
			err              error
		)
		if idTime, err = time.Parse(time.RFC3339, id.AcquiredDate); err == nil {
			if testTime, err = time.Parse(time.RFC3339, test.AcquiredDate); err == nil {
				if idTime.Before(testTime) {
					return false
				}
			}
		}
	}
	if (len(test.BoundingBox) > 0) && !test.BoundingBox.Overlaps(id.BoundingBox) {
		return false
	}
	return true
}
