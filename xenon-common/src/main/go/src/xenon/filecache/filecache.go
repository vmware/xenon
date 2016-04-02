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

package filecache

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/golang/groupcache/singleflight"
)

var downloadGroup singleflight.Group
var downloadPath string

func init() {
	downloadPath = os.TempDir()
}

// Use hash to name resulting file to prevent basename conflicts.
func Path(url string) string {
	sum := sha256.Sum256([]byte(url))
	name := fmt.Sprintf("sha256-%032x%s", sum, filepath.Ext(url))
	return filepath.Join(downloadPath, name)
}

func Download(url string) (string, error) {
	finalPath := Path(url)
	tmpPath := finalPath + ".tmp"

	iface, err := downloadGroup.Do(url, func() (interface{}, error) {
		fi, err := os.Stat(finalPath)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}

		// Add If-Modified-Since header if file exists.
		if fi != nil {
			req.Header.Set("If-Modified-Since", fi.ModTime().UTC().Format(http.TimeFormat))
		}

		// Execute HTTP request.
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		defer res.Body.Close()

		if res.StatusCode == http.StatusNotModified {
			// File hasn't changed, return existing file.
			return finalPath, nil
		}

		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected http status %d", res.StatusCode)
		}

		f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return nil, err
		}

		defer f.Close()

		_, err = io.Copy(f, res.Body)
		if err != nil {
			return nil, err
		}

		// Completed download, move file to its final destination.
		err = os.Rename(tmpPath, finalPath)
		if err != nil {
			return nil, err
		}

		return finalPath, nil
	})

	if iface != nil {
		return iface.(string), err
	}

	return "", err
}
