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

package com.vmware.xenon.common;

import java.util.Collection;
import java.util.Map;

/**
 * Request used for adding and removing elements from a collection
 */
public class ServiceStateCollectionUpdateRequest {
    public static final String KIND = Utils.buildKind(ServiceStateCollectionUpdateRequest.class);
    /**
     * Key is the field name of the collection
     * Value is the elements of the collection that needs to be added.
     */
    public Map<String, Collection<Object>> itemsToAdd;

    /**
     * Key is the field name of the collection
     * Value is the elements of the collection that needs to be removed.
     */
    public Map<String, Collection<Object>> itemsToRemove;

    public String kind;

    private ServiceStateCollectionUpdateRequest(Map<String, Collection<Object>> itemsToAdd,
            Map<String, Collection<Object>> itemsToRemove) {
        this.itemsToAdd = itemsToAdd;
        this.itemsToRemove = itemsToRemove;
        this.kind = KIND;
    }

    public static ServiceStateCollectionUpdateRequest create(Map<String, Collection<Object>> itemsToAdd,
            Map<String, Collection<Object>> itemsToRemove) {
        return new ServiceStateCollectionUpdateRequest(itemsToAdd, itemsToRemove);
    }
}
