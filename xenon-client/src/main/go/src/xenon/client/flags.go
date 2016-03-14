/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package main

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrInvalidFlag       = errors.New("invalid flag")
	ErrExpectedParameter = errors.New("expected parameter to flag")
	ErrEOF               = errors.New("no more flags")
)

type flags struct {
	s []string
}

func (f *flags) Next() (string, string, error) {
	if len(f.s) == 0 {
		return "", "", ErrEOF
	}

	k := f.s[0]
	f.s = f.s[1:]
	if k[0:2] != "--" {
		return "", "", ErrInvalidFlag
	}

	// Strip --
	k = k[2:]

	// --k=v
	ks := strings.SplitN(k, "=", 2)
	if len(ks) > 1 {
		return ks[0], ks[1], nil
	}

	// Expect the value in the next argument
	if len(f.s) == 0 {
		return "", "", ErrExpectedParameter
	}

	v := f.s[0]
	f.s = f.s[1:]
	return k, v, nil
}

func Map(s []string) (map[string]interface{}, error) {
	f := &flags{s}

	m := make(map[string]interface{})
	for {
		k, v, err := f.Next()
		if err != nil {
			if err == ErrEOF {
				return m, nil
			}

			return nil, err
		}

		// Translate key into reflect.Value that we can set.
		_, err = assign(reflect.ValueOf(m), k, v)
		if err != nil {
			return nil, err
		}
	}
}

var sliceAssignment = regexp.MustCompile(`^\[(\d+)?\](.*)`)
var mapAssignment = regexp.MustCompile(`^\.?(\w+)(.*)`)

var stringType = reflect.TypeOf("")
var sliceType = reflect.TypeOf(make([]interface{}, 0))
var interfaceType = sliceType.Elem()
var mapType = reflect.MapOf(stringType, interfaceType)

func assign(rdata reflect.Value, k, v string) (reflect.Value, error) {
	var rret reflect.Value

	// Check for string assignment
	if k == "" {
		// Create new if not valid
		if !rdata.IsValid() {
			rdata = reflect.ValueOf(v)
		} else {
			// Assign if nil
			if rdata.IsNil() {
				rdata.Set(reflect.ValueOf(v))
			}
		}

		return rdata, nil
	}

	// Check for slice assignment: "[0]"
	if m := sliceAssignment.FindStringSubmatch(k); m != nil {
		mIndex := m[1]
		mTail := m[2]

		index, err := strconv.Atoi(mIndex)
		if err != nil {
			return rret, err
		}

		// Create new if not valid
		if !rdata.IsValid() {
			rdata = reflect.MakeSlice(sliceType, index+1, index+1)
		} else {
			// Assign if nil
			if rdata.IsNil() {
				rdata.Set(reflect.MakeSlice(sliceType, index+1, index+1))
			}
		}

		// Dereference interface
		if rdata.Kind() == reflect.Interface {
			rdata = rdata.Elem()
		}

		if rdata.Kind() != reflect.Slice {
			return rret, fmt.Errorf("expected slice, got %s", rdata.Kind())
		}

		// Grow slice if necessary
		if rdata.Len()-1 < index {
			rnew := reflect.MakeSlice(sliceType, index+1, index+1)
			reflect.Copy(rnew, rdata)
			rdata = rnew
		}

		_, err = assign(rdata.Index(index), mTail, v)
		if err != nil {
			return rret, err
		}

		return rdata, nil
	}

	// Check for map assignment: ".key"
	if m := mapAssignment.FindStringSubmatch(k); m != nil {
		mKey := m[1]
		mTail := m[2]

		// Create new if not valid
		if !rdata.IsValid() {
			rdata = reflect.MakeMap(reflect.MapOf(stringType, interfaceType))
		} else {
			// Assign if nil
			if rdata.IsNil() {
				rdata.Set(reflect.MakeMap(mapType))
			}
		}

		// Dereference interface
		if rdata.Kind() == reflect.Interface {
			rdata = rdata.Elem()
		}

		if rdata.Kind() != reflect.Map {
			return rret, fmt.Errorf("expected map, got %s", rdata.Kind())
		}

		rk := reflect.ValueOf(mKey)
		rv := rdata.MapIndex(rk)
		rv, err := assign(rv, mTail, v)
		if err != nil {
			return rret, err
		}

		rdata.SetMapIndex(rk, rv)
		return rdata, nil
	}

	return rret, fmt.Errorf("don't know what to do for key: %s", k)
}
