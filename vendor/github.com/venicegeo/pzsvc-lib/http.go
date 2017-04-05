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
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"time"
)

// HTTPError represents any HTTP error
type HTTPError struct {
	Status  int
	Message string
}

func (err HTTPError) Error() string {
	return fmt.Sprintf("%d: %v", err.Status, err.Message)
}

var httpClient *http.Client

// HTTPClient is a factory method for a http.Client suitable for common operations
func HTTPClient() *http.Client {
	if httpClient == nil {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}

		httpClient = &http.Client{Transport: transport}
	}
	return httpClient
}

// SetHTTPClient is used to set the current http client.  This is mostly useful
// for testing purposes
func SetHTTPClient(newClient *http.Client) {
	httpClient = newClient
}

// RequestKnownJSON submits an http request where the response is assumed to be JSON
// for which the format is known.  Given an object of the appropriate format for
// said response JSON, an address to call and an authKey to send, it will submit
// the get request, unmarshal the result into the given object, and return. It
// returns the response buffer, in case it is needed for debugging purposes.
func RequestKnownJSON(method, bodyStr, address, authKey string, outpObj interface{}) ([]byte, error) {

	resp, err := SubmitSinglePart(method, bodyStr, address, authKey)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		if resp != nil {
			errByt, _ := ioutil.ReadAll(resp.Body)
			return errByt, TraceErr(err)
		}
		return nil, TraceErr(err)
	}
	return ReadBodyJSON(outpObj, resp.Body)
}

// ReqByObjJSON is much like RequestKnownJSON, except that it takes an interface (which
// it then json-marshals) as its input, rather than an already-marshaled string
func ReqByObjJSON(method, addr, authKey string, inpObj, outpObj interface{}) ([]byte, error) {
	byts, err := json.Marshal(inpObj)
	if err != nil {
		return nil, TraceErr(err)
	}
	byts, err = RequestKnownJSON(method, string(byts), addr, "", outpObj)
	if err != nil {
		return nil, TraceErr(err)
	}
	return byts, nil
}

// SubmitMultipart sends a multi-part POST call, including an optional uploaded file,
// and returns the response.  Primarily intended to support Ingest calls.
func SubmitMultipart(bodyStr, address, filename, authKey string, fileData []byte) (*http.Response, error) {

	var (
		body   = &bytes.Buffer{}
		writer = multipart.NewWriter(body)
		client = HTTPClient()
		err    error
	)

	err = writer.WriteField("data", bodyStr)
	fmt.Println(TraceStr("file upload initiated"))
	if err != nil {
		return nil, TraceErr(err)
	}

	if fileData != nil {
		var part io.Writer
		part, err = writer.CreateFormFile("file", filename)
		if err != nil {
			return nil, TraceErr(err)
		}
		if part == nil {
			return nil, ErrWithTrace("Failure in Form File Creation.")
		}

		_, err = io.Copy(part, bytes.NewReader(fileData))
		if err != nil {
			return nil, TraceErr(err)
		}
	}

	err = writer.Close()
	if err != nil {
		return nil, TraceErr(err)
	}

	fileReq, err := http.NewRequest("POST", address, body)
	if err != nil {
		return nil, TraceErr(err)
	}

	fileReq.Header.Add("Content-Type", writer.FormDataContentType())
	fileReq.Header.Add("Authorization", authKey)

	resp, err := client.Do(fileReq)
	if err != nil {
		return nil, TraceErr(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer resp.Body.Close()
		errByt, _ := ioutil.ReadAll(resp.Body)
		return resp, ErrWithTrace("Failed to POST multipart to " + address + " Status: " + resp.Status + "\n" + string(errByt))
	}
	return resp, nil
}

// SubmitSinglePart sends a single-part GET/POST/PUT/DELETE call to the target URL
// and returns the result.  Includes the necessary headers.
func SubmitSinglePart(method, bodyStr, url, authKey string) (*http.Response, error) {

	var (
		fileReq *http.Request
		err     error
		client  = HTTPClient()
	)

	if method == "" || url == "" {
		return nil, ErrWithTrace(`method:"` + method + `", url:"` + url + `".  You must have both.`)
	}

	if bodyStr != "" {
		fileReq, err = http.NewRequest(method, url, bytes.NewBuffer([]byte(bodyStr)))
		if err != nil {
			return nil, TraceErr(err)
		}
		fileReq.Header.Add("Content-Type", "application/json")
	} else {
		fileReq, err = http.NewRequest(method, url, nil)
		if err != nil {
			return nil, TraceErr(err)
		}
	}

	fileReq.Header.Add("Authorization", authKey)

	resp, err := client.Do(fileReq)
	if err != nil {
		return nil, TraceErr(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer resp.Body.Close()
		errByt, _ := ioutil.ReadAll(resp.Body)
		return resp, ErrWithTrace("Failed in " + method + " call to " + url + ".  Status : " + resp.Status + "\nRequest: " + bodyStr + "\nResponse: " + string(errByt))
	}

	return resp, nil
}

// GetJobResponse will repeatedly poll the job status on the given job Id
// until job completion, then acquires and returns the DataResult.
func GetJobResponse(jobID, pzAddr, authKey string) (*DataResult, error) {

	if jobID == "" {
		return nil, fmt.Errorf(`JobID not provided.  Cannot acquire DataResult.`)
	}

	for i := 0; i < 300; i++ { // will wait up to 5 minutes

		var outpObj struct {
			Data JobStatusResp `json:"data,omitempty"`
		}
		respBuf, err := RequestKnownJSON("GET", "", pzAddr+"/job/"+jobID, authKey, &outpObj)
		if err != nil {
			return nil, TraceErr(err)
		}

		respObj := &outpObj.Data
		if respObj.Status == "Submitted" ||
			respObj.Status == "Running" ||
			respObj.Status == "Pending" ||
			(respObj.Status == "Success" && respObj.Result == nil) ||
			(respObj.Status == "Error" && respObj.Result.Message == "Job Not Found.") {
			time.Sleep(time.Second)
		} else {
			if respObj.Status == "Success" {
				return respObj.Result, nil
			}
			if respObj.Status == "Fail" {
				return nil, ErrWithTrace("Piazza failure when acquiring DataId.  Response json: " + string(respBuf))
			}
			if respObj.Status == "Error" {
				return nil, ErrWithTrace("Piazza error when acquiring DataId.  Response json: " + string(respBuf))
			}
			return nil, ErrWithTrace(`Unknown status "` + respObj.Status + `" when acquiring DataId.  Response json: ` + string(respBuf))
		}
	}

	return nil, ErrWithTrace("Never completed.  JobId: " + jobID)
}

// GetJobID is a simple function to extract the job ID from
// the standard response to job-creating Pz calls
func GetJobID(resp *http.Response) (string, error) {
	var respObj JobInitResp
	byts, err := ReadBodyJSON(&respObj, resp.Body)
	err = TraceErr(err)
	if respObj.Data.JobID == "" && err == nil {
		err = ErrWithTrace("GetJobID: response did not contain Job ID.  initial response: " + string(byts))
	}
	return respObj.Data.JobID, err
}

// ReadBodyJSON takes the body of either a request object or a response
// object, pulls out the body, and attempts to interpret it as JSON into
// the given interface format.  It's mostly there as a minor simplifying
// function.
func ReadBodyJSON(output interface{}, body io.ReadCloser) ([]byte, error) {
	rBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, TraceErr(err)
	}
	err = json.Unmarshal(rBytes, output)
	if err != nil {
		return nil, ErrWithTrace("Unmarshal failed: " + err.Error() + ".  Original input: " + string(rBytes) + ".")
	}
	return rBytes, TraceErr(err)
}

// HTTPOut outputs the given string on the given responseWriter
// with the given http code.  It is nearly identical in behavior
// to http.Error, except that it doesn't modify the headers
// otherwise, allowing us to maintain the Content-Type of
// application/json, and make things a bit easier for our
// service consumers to digest.
func HTTPOut(w http.ResponseWriter, output string, code int) {
	w.WriteHeader(code)
	w.Write([]byte(output))
}

// Preflight sets up the CORS stuff and
// returns TRUE if this is an OPTIONS request
func Preflight(w http.ResponseWriter, r *http.Request) bool {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}

	return (r.Method == "OPTIONS")
}

// PrintJSON marshals the given object, turns it into a string, and feeds it to
// the given ResponseWriter.
func PrintJSON(w http.ResponseWriter, output interface{}, httpStatus int) []byte {
	outBuf, err := json.Marshal(output)
	if err != nil {
		HTTPOut(w, `{"Errors":"JSON marshal failure: `+TraceStr(err.Error())+`"}`, http.StatusInternalServerError)
	} else {
		HTTPOut(w, string(outBuf), httpStatus)
	}
	return outBuf
}
