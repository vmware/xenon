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
    public static final String FIELD_NAME_DOCUMENTS = "documents";
    public static final String FIELD_NAME_DOCUMENT_COUNT = "documentCount";
    public static final String FIELD_NAME_PREV_PAGE_LINK = "prevPageLink";
    public static final String FIELD_NAME_NEXT_PAGE_LINK = "nextPageLink";
    public static final String FIELD_NAME_QUERY_TIME_MICROS = "queryTimeMicros";

    /**
     * Collection of self links associated with each document found. The self link acts as the
     * primary key for a document.
     */
    public List<String> documentLinks = new ArrayList<>();

    /**
     * If the query included QueryOption.EXPAND, this map populated with the JSON serialized service
     * state document associated with each link
     */
    public Map<String, Object> documents;

    /**
     * If the query included QueryOption.SELECT_LINKS, this map is populated with the link
     * names and values, for each link in the results. For example, if the query results
     * include a document link /services/one, with a document that has a field "parentLink"
     * and value "/parents/two", this map will look like so:
     * { "selectedLinks" : { "/services/one" : {"parentLink" : "parents/two" } } }
     */
    public Map<String, Map<String, String>> selectedLinks;

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

    /**
     * Returns whether or not the {@code name} is a built-in field.
     *
     * @param name Field name
     * @return true/false
     */
    public static boolean isBuiltInField(String name) {
        switch (name) {
        case FIELD_NAME_DOCUMENT_LINKS:
        case FIELD_NAME_DOCUMENTS:
        case FIELD_NAME_DOCUMENT_COUNT:
        case FIELD_NAME_PREV_PAGE_LINK:
        case FIELD_NAME_NEXT_PAGE_LINK:
        case FIELD_NAME_QUERY_TIME_MICROS:
            return true;
        default:
            return false;
        }
    }
}
