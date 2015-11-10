/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.dcp.common.serialization;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.reflect.TypeToken;

/**
 * A set of {@link Type} constants to assist with deserialization of some common generic types.
 */
public interface TypeTokens {
    /*
     * Common Collection Types
     */

    Type LIST_OF_STRINGS = new TypeToken<List<String>>() {}.getType();

    Type SET_OF_STRINGS = new TypeToken<Set<String>>() {}.getType();

    Type MAP_OF_STRINGS_BY_STRING = new TypeToken<Map<String, String>>() {}.getType();

    Type MAP_OF_OBJECTS_BY_STRING = new TypeToken<Map<String, Object>>() {}.getType();
}
