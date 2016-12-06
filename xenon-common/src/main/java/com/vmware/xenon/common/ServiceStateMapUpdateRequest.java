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

package com.vmware.xenon.common;

import java.util.Collection;
import java.util.Map;

/**
 * Request used for adding and removing elements from a map
 */
public class ServiceStateMapUpdateRequest {
    public static final String KIND = Utils.buildKind(ServiceStateMapUpdateRequest.class);

    /**
     * Key is the field name of the map.
     * Value is the key-value pairs that need to be added to the map.
     */
    public Map<String, Map<Object, Object>> entriesToAdd;

    /**
     * Key is the field name of the map.
     * Value is the keys of the map that need to be removed.
     */
    public Map<String, Collection<Object>> keysToRemove;

    public final String kind = KIND;

    private ServiceStateMapUpdateRequest(Map<String, Map<Object, Object>> entriesToAdd,
            Map<String, Collection<Object>> keysToRemove) {
        this.entriesToAdd = entriesToAdd;
        this.keysToRemove = keysToRemove;
    }

    public static ServiceStateMapUpdateRequest create(Map<String, Map<Object, Object>> entriesToAdd,
            Map<String, Collection<Object>> keysToRemove) {
        return new ServiceStateMapUpdateRequest(entriesToAdd, keysToRemove);
    }
}
