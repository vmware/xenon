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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ServiceDocumentQueryResult extends ServiceDocument {
    public static final String FIELD_NAME_DOCUMENT_LINKS = "documentLinks";
    /**
     * Collection of self links associated with each document found. The self link acts as the
     * primary key for a document.
     */
    public List<String> documentLinks = new ArrayList<>();

    /**
     * If the query included an expand directory, this map contains the JSON serialized service
     * state document associated with each link
     */
    public Map<String, Object> documents;

    /**
     * Set to the number of documents that satisfy the query.
     */
    public Long documentCount;

    /**
     * Valid only if QuerySpecification.resultLimit is specified.
     * In which case, a GET request to this link will retrieve the previous page of query results.
     * This link will expire roughly at the same time as the original QueryTask.
     */
    public String prevPageLink;

    /**
     * Valid only if QuerySpecification.resultLimit is specified.
     * In which case, a GET request to this link will retrieve the next page of query results.
     * This link will expire roughly at the same time as the original QueryTask.
     */
    public String nextPageLink;

    /**
     * Duration of the query execution.
     */
    public Long queryTimeMicros;
}
