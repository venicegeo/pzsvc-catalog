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
	"fmt"
	"gopkg.in/redis.v3"
	"net"
	"time"
)

var mockConnOutpBytes [][]byte

type mockAddr struct{}

func (ma mockAddr) Network() string {
	return "Network"
}

func (ma mockAddr) String() string {
	return "String"
}

type mockConn struct {
	readCount *int
}

var mockConnCount int
var mockConnInst = mockConn{readCount: &mockConnCount}

//GetMockConnCount returns mockConnCount
func GetMockConnCount() (count int) {
	return mockConnCount
}

//SetMockConnCount sets mockConnCount
func SetMockConnCount(count int) {
	mockConnCount = count
}

/*
now we need a series of functions that will populate the
outpBytes (iterating writeCount with each line) based on
the ints, strings, and errors we want to see in each case.

For that, we first need the byte format objective.
Digging commences.

In particular, we're interested in what all touches the
Read port of the net.Conn from teh Dialer in the options.

- all of it (other htan a couple of tests) goes through redis.getDialer in redis.v3/options.go
- That *only* gets referenced in options.go/newConnPool
- This, in turn, only sees the light fo day as the return for `(p *ConnPool) dial()`,
  ...which feeds straight into `(p *ConnPool) NewConn()`
- In the end, it essentially just gets pumped right back out of the redis.Conn Read and Write functions.


*/

func (mCn mockConn) Read(b []byte) (n int, err error) {
	fmt.Printf("reading: %d of %d.\n", *mCn.readCount, len(mockConnOutpBytes))
	if *mCn.readCount < len(mockConnOutpBytes) {
		copy(b, mockConnOutpBytes[*mCn.readCount])
		*mCn.readCount = *mCn.readCount + 1
	}
	return len(b), nil
}

func (mCn mockConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (mCn mockConn) Close() error {
	return nil
}

func (mCn mockConn) LocalAddr() net.Addr {
	return mockAddr{}
}

func (mCn mockConn) RemoteAddr() net.Addr {
	return mockAddr{}
}

func (mCn mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (mCn mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (mCn mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

//MockDialer sets up a mock dialer for the mock redis
func MockDialer() (net.Conn, error) {
	//build correct net.Conn here.
	return mockConnInst, nil
}

//MakeMockRedisCli creates the mock redis client
func MakeMockRedisCli(outputs []string) *redis.Client {
	opt := redis.Options{Dialer: MockDialer}
	cli := redis.NewClient(&opt)
	mockConnOutpBytes = make([][]byte, len(outputs), len(outputs))
	for i, output := range outputs {
		mockConnOutpBytes[i] = []byte(output)
	}
	return cli
}

//RedisConvInt converts an int for redis use
func RedisConvInt(val int) string {
	return fmt.Sprintf(":%d\r\n", val)
}

//RedisConvStatus converts a status string for redis use
func RedisConvStatus(val string) string {
	return fmt.Sprintf("+%s\r\n", val)
}

//RedisConvString converts a string for redis use
func RedisConvString(val string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
}

//RedisConvErrStr converts an error string for redis use
func RedisConvErrStr(val string) string {
	return fmt.Sprintf("-%s\r\n", val)
}
func RedusConvArray() string {
	return "*0\r\n"
}
