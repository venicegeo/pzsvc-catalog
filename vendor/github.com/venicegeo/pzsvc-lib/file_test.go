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

package pzsvc

import (
	//"encoding/json"
	//"fmt"
	//"io"
	"io/ioutil"
	//"mime"
	//"net/http"
	"os"
	"testing"
)

func TestDownloadBytes(t *testing.T) {
	t.Log(HTTPClient()) //testing the HTTPClient when cient is nil
	outStrs := []string{`{"test":"blah"}`}
	SetMockClient(outStrs, 250)

	url := "http://testURL.net"
	authKey := "testAuthKey"
	dataID := "1234ID"

	byts, err := DownloadBytes(dataID, url, authKey)
	if err != nil {
		t.Error(`TestDownloadBytes: failed on base call: ` + err.Error())
	}
	if string(byts) != `{"test":"blah"}` {
		t.Error(`TestDownloadBytes: Incorrect string.  "` + string(byts) + `"`)
	}
}

func TestDownloadByID(t *testing.T) {
	outStrs := []string{`{"test":"blah"}`, `{"test":"blah"}`, `{"test":"blah"}`}
	SetMockClient(outStrs, 250)
	url := "http://testURL.net"
	authKey := "testAuthKey"
	dataID := "1234ID"
	fileName := "tempTestFile.tmp"
	subFold := "folderName"

	os.Mkdir(subFold, 0777)
	_, err := DownloadByID(dataID, fileName, subFold, url, authKey)
	if err != nil {
		t.Error(`TestDownloadByID: failed on subfolder-yes call: ` + err.Error())
	}
	os.RemoveAll(subFold)

	_, err = DownloadByID(dataID, fileName, "", url, authKey)
	if err != nil {
		t.Error(`TestDownloadByID: failed on subfolder-no call: ` + err.Error())
	}
	os.Remove(locString("", fileName))

	_, err = DownloadByID(dataID, "", "", url, authKey)
	if err == nil {
		t.Error(`TestDownloadByID: passed a filename-no call: ` + err.Error())
	}
}

func TestIngestFile(t *testing.T) {
	outStrs := []string{
		`{"Data":{"JobID":"testID1"}}`,
		`{"Data":{"Status":"Success", "Result":{"Message":"testStatus1"}}}`,
		`{"Data":{"JobID":"testID2"}}`,
		`{"Data":{"Status":"Success", "Result":{"Message":"testStatus2"}}}`,
		`{"Data":{"JobID":"testID3"}}`,
		`{"Data":{"Status":"Success", "Result":{"Message":"testStatus3"}}}`}
	SetMockClient(outStrs, 250)
	url := "http://testURL.net"
	authKey := "testAuthKey"
	fileName := "tempTestFile.tmp"
	subFold := "folderName"

	os.Mkdir(subFold, 0777)
	err := ioutil.WriteFile("./"+subFold+"/"+fileName, []byte(fileName), 0666)
	if err != nil {
		t.Error(`TestIngestFile: error on file creation: ` + err.Error())
	}
	_, err = IngestFile(fileName, subFold, "text", url, "tester", "0.0", authKey, map[string]string{"prop1": "1", "prop2": "2"})
	if err != nil {
		t.Error(`TestIngestFile: error on text ingest: ` + err.Error())
	}
	_, err = IngestFile(fileName, subFold, "geojson", url, "tester", "0.0", authKey, map[string]string{"prop1": "1", "prop2": "2"})
	if err != nil {
		t.Error(`TestIngestFile: error on geojson ingest: ` + err.Error())
	}
	_, err = IngestFile(fileName, subFold, "raster", url, "tester", "0.0", authKey, map[string]string{"prop1": "1", "prop2": "2"})
	if err != nil {
		t.Error(`TestIngestFile: error on raster ingest: ` + err.Error())
	}
	os.RemoveAll(subFold)
}

func TestGetFileMeta(t *testing.T) {
	SetMockClient(nil, 250)
	url := "http://testURL.net"
	authKey := "testAuthKey"
	dataID := "1234ID"

	_, err := GetFileMeta(dataID, url, authKey)
	if err != nil {
		t.Error(`TestGetFileMeta: error: ` + err.Error())
	}
}

func TestUpdateFileMeta(t *testing.T) {
	SetMockClient(nil, 250)
	url := "http://testURL.net"
	authKey := "testAuthKey"
	dataID := "1234ID"
	metaData := map[string]string{"prop1": "1", "prop2": "2", "prop3": "3"}

	err := UpdateFileMeta(dataID, url, authKey, metaData)
	if err != nil {
		t.Error(`TestGetFileMeta: error: ` + err.Error())
	}
}

func TestDeployToGeoServer(t *testing.T) {
	outStrs := []string{`{"Data":{"JobID":"testID"}}`, `{"Data":{"Status":"Success", "Result":{"Message":"Job Found."}}}`}
	SetMockClient(outStrs, 250)
	url := "http://testURL.net"
	authKey := "testAuthKey"
	dataID := "1234ID"
	deplID := "5678ID"

	_, err := DeployToGeoServer(dataID, deplID, url, authKey)
	if err != nil {
		t.Error(`TestDeployToGeoServer: error: ` + err.Error())
	}
}

func TestAddGeoServerLayerGroup(t *testing.T) {
	outStrs := []string{`{"type":"typ", "data":{"deploymentGroupId":"deplG", "createdBy":"me", "hasGisServerLayer":false}}`}
	SetMockClient(outStrs, 250)
	url := "http://testURL.net"
	authKey := "testAuthKey"

	_, err := AddGeoServerLayerGroup(url, authKey)
	if err != nil {
		t.Error(`TestDeployToGeoServer: error: ` + err.Error())
	}
}
