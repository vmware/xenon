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

public class ODataFactoryQueryResult extends ServiceDocumentQueryResult {

    public static final String FIELD_NAME_TOTAL_COUNT = "totalCount";

    /**
     * Set to the total number of documents that satisfy the query.
     */
    public Long totalCount;

    /**
     * Returns whether or not the {@code name} is a built-in field.
     *
     * @param name Field name
     * @return true/false
     */
    public static boolean isBuiltInField(String name) {
        switch (name) {
        case FIELD_NAME_TOTAL_COUNT:
            return true;
        default:
            return ServiceDocumentQueryResult.isBuiltInField(name);
        }
    }
}
