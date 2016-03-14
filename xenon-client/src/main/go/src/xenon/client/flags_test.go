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

import "testing"

func TestFlags(t *testing.T) {
	flags := []string{
		"--key=value",
		"--mapA.k1=value",
		"--mapA.k2=value",
		"--sliceA[0].k1=value",
		"--sliceA[0].k2=value",
		"--sliceA[1].k1=value",
		"--sliceA[1].k2=value",
	}

	v, err := Map(flags)
	if err != nil {
		panic(err)
	}

	t.Logf("%#v", v)
}
