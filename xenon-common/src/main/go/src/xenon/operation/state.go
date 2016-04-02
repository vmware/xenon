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

package operation

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"xenon/uri"
)

var (
	ErrBodyNotSet           = errors.New("operation: body is not set")
	ErrBodyIncompatibleType = errors.New("operation: body has incompatible type")
	ContentType             = "Content-Type"
	ApplicationJSON         = "application/json"
)

type State struct {
	method string
	uri    uri.URI
	body   interface{}

	statusCode int

	// Request side of the operation for inbound requests.
	httpRequest *http.Request

	// Response side of the operation for outbound requests.
	httpResponse *http.Response

	isStreamResponse bool
	err              error
}

func (o *Operation) SetMethod(m string) *Operation {
	o.state.method = m
	return o
}

func (o *Operation) GetMethod() string {
	return o.state.method
}

func (o *Operation) SetURI(u uri.URI) *Operation {
	o.state.uri = u
	return o
}

func (o *Operation) GetURI() uri.URI {
	return o.state.uri
}

func (o *Operation) SetBody(b interface{}) *Operation {
	o.state.body = b
	return o
}

func (o *Operation) GetBody() interface{} {
	return o.state.body
}

func (o *Operation) SetStatusCode(code int) *Operation {
	o.state.statusCode = code
	return o
}

func (o *Operation) GetStatusCode() int {
	return o.state.statusCode
}

func (o *Operation) SetRequest(req *http.Request) *Operation {
	o.state.httpRequest = req
	o.SetBody(req.Body)
	return o
}

func (o *Operation) GetRequest() *http.Request {
	return o.state.httpRequest
}

// CreateRequest creates an http.Request for this Operation.
// It is intended for clients who need to set request headers.
func (o *Operation) CreateRequest() (*http.Request, error) {
	if o.state.httpRequest != nil {
		return o.state.httpRequest, nil
	}

	var buf io.Reader
	var err error

	// Encode body if the operation has one.
	if o.GetBody() != nil {
		buf, err = o.EncodeBodyAsBuffer()
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(o.GetMethod(), o.GetURI().String(), buf)
	if err != nil {
		return nil, err
	}

	if buf != nil {
		req.Header.Set(ContentType, ApplicationJSON)
	}

	token := GetAuthorizationToken(o.vars)
	if token != "" {
		req.Header.Set(RequestAuthTokenHeader, token)
	}

	o.state.httpRequest = req

	return req, nil
}

func (o *Operation) SetResponse(res *http.Response) *Operation {
	o.state.httpResponse = res

	// Caller does not want the response read into memory,
	// for example when downloading a large .iso file.
	if o.state.isStreamResponse {
		o.SetBody(res.Body)
		return o
	}

	// Copy the response body into memory, then Body.Close
	// so the caller doesn't have to worry about leaking connections.
	body := &bytes.Buffer{}
	_, o.state.err = io.Copy(body, res.Body)
	_ = res.Body.Close()

	if o.state.err == nil {
		o.SetBody(body)
	}

	return o
}

func (o *Operation) GetResponse() *http.Response {
	return o.state.httpResponse
}

func (o *Operation) GetResponseHeader(key string) string {
	return o.state.httpResponse.Header.Get(key)
}

// SetIsStreamResponse flags the Operation not to read the response body into memory.
// It then becomes the caller's responsiblity to call Operation.GetResponse().Body.Close()
func (o *Operation) SetIsStreamResponse(f bool) *Operation {
	o.state.isStreamResponse = f
	return o
}

func (o *Operation) IsStreamResponse() bool {
	return o.state.isStreamResponse
}

// DecodeBody deserializes the body.
func (o *Operation) DecodeBody(dst interface{}) error {
	err := o.state.err

	if err != nil {
		return err
	}

	switch typ := o.state.body.(type) {
	case nil:
		return ErrBodyNotSet
	case io.ReadCloser:
		defer typ.Close()
		dec := json.NewDecoder(typ)
		err = dec.Decode(dst)

		// Can only use an io.Reader once
		o.state.body = nil
	case *bytes.Buffer:
		dec := json.NewDecoder(typ)
		err = dec.Decode(dst)

		typ.Reset()
	default:
		bv := reflect.ValueOf(o.state.body)
		bvInner := bv
		bvInnerType := bvInner.Type()
		for bvInner.Kind() == reflect.Ptr {
			bvInner = bvInner.Elem()
			bvInnerType = bvInnerType.Elem()
		}

		dv := reflect.ValueOf(dst)
		dvInner := dv
		dvInnerType := dvInner.Type()
		for dvInner.Kind() == reflect.Ptr {
			dvInner = dvInner.Elem()
			dvInnerType = dvInnerType.Elem()
		}

		if bvInnerType == dvInnerType {
			// Create and dereference pointers as needed
			for dv.Type() != dvInnerType {
				if dv.IsNil() {
					dv.Set(reflect.New(dv.Type().Elem()))
				}
				dv = dv.Elem()
			}

			dv.Set(bvInner)
			return nil
		}

		return ErrBodyIncompatibleType
	}

	return err
}

// EncodeBody serializes the body with the specified io.Writer as target.
func (o *Operation) EncodeBody(w io.Writer) error {
	switch typ := o.state.body.(type) {
	case nil:
		return ErrBodyNotSet
	case io.Reader:
		_, err := io.Copy(w, typ)
		return err
	default:
		enc := json.NewEncoder(w)
		return enc.Encode(typ)
	}
}

// EncodeBodyAsBuffer serialized the body into a bytes.Buffer.
func (o *Operation) EncodeBodyAsBuffer() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	err := o.EncodeBody(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
