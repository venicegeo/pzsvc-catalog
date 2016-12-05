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
	"net/http"
	//"os"
	//"github.com/gorilla/mux"
	"github.com/venicegeo/pzsvc-lib"
	"testing"
)

/*
func TestServe(t *testing.T) {
	catalog.SetMockConnCount(0)
	outputs := []string{catalog.RedisConvString("Alrite"),
		catalog.RedisConvString("Alrite")}
	redisClient := catalog.MakeMockRedisCli(outputs)
	//taskChan = make(chan string)
	os.Setenv("VCAP_SERVICES", "{\"p-redis\":[{\"credentials\":{\"host\":\"127.0.0.1\",\"port\":6379}}]}")
	os.Setenv("PL_API_KEY", "a1fa3d8df30545468052e45ae9e4520e")

	conn, _ := net.Dial("tcp", "127.0.0.1:8080")
	log.
	serve(redisClient)
	conn.Close()
*/

func TestDropIndexHandler(t *testing.T) {
	w, _, _ := pzsvc.GetMockResponseWriter()
	r := http.Request{}
	r.Method = "POST"
	r.Body = pzsvc.GetMockReadCloser(`{"pzGateway":"http://test.com",}`)
	//r.Header.Add("Authorization", "1A2B3C4D5E")
	dropIndexHandler(w, &r)

}

/*
func TestImageHandler(t *testing.T) {
	w, _, _ := pzsvc.GetMockResponseWriter()
	r := http.Request{}
	r.Method = "POST"
	r.Body = pzsvc.GetMockReadCloser(`{"pzGateway":"http://test.com",}`)
	x := mux.NewRouter()
	x.HandleFunc("/id/{id}/", imageHandler)
	imageHandler(w, &r/)
}
*/
