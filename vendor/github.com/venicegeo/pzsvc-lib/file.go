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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
)

// locString simplifies certain local processes that wish to interact with
// files that may or may not be in a subfolder.
func locString(subFold, fname string) string {
	if subFold == "" {
		return fmt.Sprintf(`./%s`, fname)
	}
	return fmt.Sprintf(`./%s/%s`, subFold, fname)
}

// DownloadBytes retrieves a file from Pz using the file access API and then
// returns the results as a byte slice
func DownloadBytes(dataID, pzAddr, authKey string) ([]byte, error) {

	resp, err := SubmitSinglePart("GET", "", pzAddr+"/file/"+dataID, authKey)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, TraceErr(err)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, TraceErr(err)
	}

	return b, nil
}

// DownloadByID retrieves a file from Pz using the file access API
func DownloadByID(dataID, filename, subFold, pzAddr, authKey string) (string, error) {
	fName, err := DownloadByURL(pzAddr+"/file/"+dataID, filename, subFold, authKey)
	if err == nil && fName == "" {
		return "", ErrWithTrace(`File for DataID ` + dataID + ` unnamed.  Probable ingest error.`)
	}
	return fName, err
}

// DownloadByURL retrieves a file from the given URL
func DownloadByURL(url, filename, subFold, authKey string) (string, error) {

	var (
		params map[string]string
	)
	resp, err := SubmitSinglePart("GET", "", url, authKey)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return "", TraceErr(err)
	}
	if filename == "" {
		contDisp := resp.Header.Get("Content-Disposition")
		_, params, err = mime.ParseMediaType(contDisp)
		if err != nil {
			return "", TraceErr(err)
		}
		filename = params["filename"]
		if filename == "" {
			return "", ErrWithTrace(`Input file from URL "` + url + `" was not given a name.`)
		}
	}
	out, err := os.Create(locString(subFold, filename))
	if err != nil {
		return "", TraceErr(err)
	}

	defer out.Close()
	io.Copy(out, resp.Body)

	return filename, nil
}

// Ingest ingests the given bytes to Piazza.
func Ingest(fName, fType, pzAddr, sourceName, version, authKey string,
	ingData []byte,
	props map[string]string) (string, error) {

	var fileData []byte
	var resp *http.Response

	desc := fmt.Sprintf("%s uploaded by %s.", fType, sourceName)
	rMeta := ResMeta{
		Name:        fName,
		Format:      fType,
		ClassType:   ClassType{"UNCLASSIFIED"},
		Version:     version,
		Description: desc,
		Metadata:    make(map[string]string)}

	for key, val := range props {
		rMeta.Metadata[key] = val
	}

	dType := DataType{Type: fType}

	switch fType {
	case "raster":
		{
			//dType.MimeType = "image/tiff"
			fileData = ingData
		}
	case "geojson":
		{
			dType.MimeType = "application/vnd.geo+json"
			fileData = ingData
		}
	case "text":
		{
			dType.MimeType = "application/text"
			dType.Content = string(ingData)
			fileData = nil
		}
	}

	dRes := DataDesc{"", dType, rMeta, nil}
	jType := IngestReq{dRes, true, "ingest"}
	bbuff, err := json.Marshal(jType)
	if err != nil {
		return "", TraceErr(err)
	}

	if fileData != nil {
		resp, err = SubmitMultipart(string(bbuff), (pzAddr + "/data/file"), fName, authKey, fileData)
	} else {
		resp, err = SubmitSinglePart("POST", string(bbuff), (pzAddr + "/data"), authKey)
	}
	if err != nil {
		return "", TraceErr(err)
	}

	jobID, err := GetJobID(resp)
	if err != nil {
		return "", TraceErr(err)
	}

	result, err := GetJobResponse(jobID, pzAddr, authKey)
	if err != nil {
		return "", TraceErr(err)
	}

	return result.DataID, TraceErr(err)
}

// IngestFile ingests the given file to Piazza
func IngestFile(fName, subFold, fType, pzAddr, sourceName, version, authKey string,
	props map[string]string) (string, error) {

	path := locString(subFold, fName)

	fData, err := ioutil.ReadFile(path)
	if err != nil {
		return "", TraceErr(err)
	}
	if len(fData) == 0 {
		return "", ErrWithTrace(`File "` + fName + `" read as empty.`)
	}
	return Ingest(fName, fType, pzAddr, sourceName, version, authKey, fData, props)
}

// GetFileMeta retrieves the metadata for a given dataID in the S3 bucket
func GetFileMeta(dataID, pzAddr, authKey string) (*DataDesc, error) {

	url := fmt.Sprintf(`%s/data/%s`, pzAddr, dataID)
	var respObj struct{ Data DataDesc }
	_, err := RequestKnownJSON("GET", "", url, authKey, &respObj)
	if err != nil {
		return nil, TraceErr(err)
	}

	return &respObj.Data, nil
}

// UpdateFileMeta updates the metadata for a given dataID in the S3 bucket
func UpdateFileMeta(dataID, pzAddr, authKey string, newMeta map[string]string) error {

	var meta struct {
		Metadata map[string]string `json:"metadata"`
	}
	meta.Metadata = newMeta
	jbuff, err := json.Marshal(meta)
	if err != nil {
		return TraceErr(err)
	}

	_, err = SubmitSinglePart("POST", string(jbuff), fmt.Sprintf(`%s/data/%s`, pzAddr, dataID), authKey)
	return TraceErr(err)
}

// SearchFileMeta takes a search string, Pz address, and Pz Auth, and returns
// a list of all file metadata such that the search string appears somewhere
// in the metadata.  It was useful once and may be useful again, but it is not
// useful now.
/*
func SearchFileMeta(searchString, pzAddr, authKey string) ([]DataDesc, error) {
	url := fmt.Sprintf(`%s/data?keyword=%s&perPage=1000`, pzAddr, searchString)
	var respObj FileDataList
	_, err := RequestKnownJSON("GET", "", url, authKey, &respObj)
	if err != nil {
		return nil, TraceErr(err)
	}

	return respObj.Data, nil
}*/

// DeployToGeoServer calls the Pz "provision" endpoint - causing the file indicated
// by dataId to be deployed to the local GeoServer instance, and returning the ID of
// the new layer.  If lGroupID is included, the layer is also added to the layer
// group with that ID.
func DeployToGeoServer(dataID, lGroupID, pzAddr, authKey string) (*DeplStrct, error) {
	outJSON := `{"dataId":"` + dataID + `","deploymentGroupId":"` + lGroupID + `","deploymentType":"geoserver","type":"access"}`

	resp, err := SubmitSinglePart("POST", outJSON, pzAddr+"/deployment", authKey)
	if err != nil {
		return nil, TraceErr(err)
	}

	jobID, err := GetJobID(resp)
	if err != nil {
		return nil, TraceErr(err)
	}

	result, err := GetJobResponse(jobID, pzAddr, authKey)
	if err != nil {
		return nil, TraceErr(err)
	}

	return &result.Deployment, nil
}

// AddGeoServerLayerGroup takes the bare-bones contact information for the local Piazza
// instance, submits a request for a new geoserver layer group, and returns the identifying
// uuid for that layer group (or an error).
func AddGeoServerLayerGroup(pzAddr, authKey string) (string, error) {

	type dataStruct struct {
		DeploymentGroupID string `json:"deploymentGroupId,omitempty"`
		CreatedBy         string `json:"createdBy,omitempty"`
		HasGeoServerLayer bool   `json:"hasGisServerLayer,omitempty"`
	}

	var respObj struct {
		Type string     `json:"type,omitempty"`
		Data dataStruct `json:"data,omitempty"`
	}

	_, err := RequestKnownJSON("POST", "", pzAddr+"/deployment/group", authKey, &respObj)

	return respObj.Data.DeploymentGroupID, TraceErr(err)
}
