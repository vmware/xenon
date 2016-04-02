// Copyright (c) 2015-2016 VMware, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, without warranties or
// conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
// specific language governing permissions and limitations under the License.

package client

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"
	"xenon/operation"
)

type Client interface {
	Send(o *operation.Operation) *operation.Operation
}

var DefaultTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	Dial: NewDialer(&net.Dialer{
		Timeout: 10 * time.Second,
	}, MaximumConnections).Dial,
	DisableKeepAlives:   true,
	MaxIdleConnsPerHost: MaximumConnections,
	TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true, // TODO: config option
	},
}

var defaultClient = &http.Client{Transport: DefaultTransport}

// responseToError checks the status code of a response for errors.
func responseToError(req *http.Request, res *http.Response) error {
	if res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusBadRequest {
		return nil
	}

	b, _ := ioutil.ReadAll(res.Body)
	res.Body.Close()

	return fmt.Errorf("dcp client %s %s: %d (%s) %s",
		req.Method, req.URL, res.StatusCode, res.Status, string(b))
}

func doSend(req *http.Request) (*http.Response, error) {
	res, err := defaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	// Figure out if the response should be interpreted as error.
	err = responseToError(req, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func isNetError(err error) bool {
	if uerr, ok := err.(*url.Error); ok {
		if _, ok := uerr.Err.(*net.OpError); ok {
			return true
		}
	}

	return false
}

// Send executes the specified operation, while observing context cancellation.
func Send(o *operation.Operation) *operation.Operation {
	req, err := o.CreateRequest()
	if err != nil {
		o.Fail(err)
		return o
	}

	if !DefaultTransport.DisableKeepAlives {
		req.Header.Set("Connection", "keep-alive")
	}

	resc := make(chan *http.Response, 1)
	errc := make(chan error, 1)
	o.Start()

	for {
		// Execute operation in separate routine.
		go func() {
			res, err := doSend(req)
			resc <- res
			errc <- err
		}()

		// Wait for the context to be cancelled or the operation to be done.
		select {
		case <-o.Done():
			// Timeout
			DefaultTransport.CancelRequest(req)
			<-errc // Wait for goroutine to return.
			<-resc
			return o
		case err = <-errc:
			res := <-resc
			if err == nil {
				o.SetResponse(res)
				o.Complete()
				return o
			}

			shouldRetry := isNetError(err)
			if !shouldRetry || o.DecrementRetriesRemaining() <= 0 {
				o.Fail(err)
				return o
			}

			o.WaitForRetryDelay(err)
		}
	}
}
