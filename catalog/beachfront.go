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

	"github.com/venicegeo/geojson-go/geojson"
)

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
	NumberOfBands   int                 `json:"numBands,omitempty"`
	BeachfrontScore float64             `json:"beachfrontScore,omitempty"`
	FileSize        int64               `json:"fileSize,omitempty"`
}

// GetImages returns images for the given set matching the criteria in the options
func GetImages(set string, options *ImageDescriptor) []ImageDescriptor {
	var (
		result   []ImageDescriptor
		idm      string
		idmBytes []byte
	)
	red := RedisClient(nil)
	defer red.Close()

	members := client.SMembers(set)
	for _, curr := range members.Val() {
		var cid ImageDescriptor

		idm = red.Get(curr).Val()
		idmBytes = []byte(idm)
		json.Unmarshal(idmBytes, &cid)
		if cid.compare(options) {
			result = append(result, cid)
		}
	}
	return result
}

func (id *ImageDescriptor) compare(test *ImageDescriptor) bool {
	result := true
	if test != nil {
		switch {
		case test.ID != "":
			if id.ID != test.ID {
				result = false
				break
			}
			// 	fallthrough
			// case test.Name != "":
			// 	if strings.Contains(id.Name, test.Name) {
			// 		result = false
			// 		break
			// 	}
		}
	}
	return result
}
