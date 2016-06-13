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

import java.util.Collections;
import java.util.Set;

import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.UriUtils.ODataOrder;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.SortOrder;
import com.vmware.xenon.services.common.QueryTask.QueryTerm;

public class ODataUtils {

    /**
     * Used in OData filter, represents a property term that should includes all fields
     * in the filter.
     */
    public static final String FILTER_VALUE_ALL_FIELDS = "ALL_FIELDS";

    /**
     * Builds a {@code QueryTask} with a fully formed query and options, from the operation URI
     * query parameters
     */
    public static QueryTask toQuery(Operation op, boolean validate) {
        return toQuery(op, validate, Collections.emptySet());
    }

    /**
     * Builds a {@code QueryTask} with a fully formed query and options, from the operation URI
     * query parameters.
     * @param wildcardFilterUnfoldPropertyNames is an optional set of property names that will
     * be used when forming the query. In this case when the filter contains
     * {@link UriUtils#URI_WILDCARD_CHAR} for a property name of the query will be expanded with
     * multiple OR sub-queries for each property name of this set instead of the wildcard.
     */
    public static QueryTask toQuery(Operation op, boolean validate, Set<String> wildcardFilterUnfoldPropertyNames) {

        QueryTask task = new QueryTask();
        task.setDirect(true);
        task.querySpec = new QueryTask.QuerySpecification();

        boolean count = UriUtils.getODataCountParamValue(op.getUri());
        if (count) {
            task.querySpec.options.add(QueryOption.COUNT);
        } else {
            task.querySpec.options.add(QueryOption.EXPAND_CONTENT);
        }

        String filter = UriUtils.getODataFilterParamValue(op.getUri());
        if (filter != null) {
            Query q = new ODataQueryVisitor(wildcardFilterUnfoldPropertyNames).toQuery(filter);
            if (q != null) {
                task.querySpec.query.addBooleanClause(q);
            }
        }

        UriUtils.ODataOrderByTuple orderBy = UriUtils.getODataOrderByParamValue(op.getUri());
        if (orderBy != null) {
            if (count && validate) {
                op.fail(new IllegalArgumentException(UriUtils.URI_PARAM_ODATA_COUNT
                        + " cannot be used together with " + UriUtils.URI_PARAM_ODATA_ORDER_BY));
                return null;
            }
            task.querySpec.options.add(QueryOption.SORT);
            task.querySpec.sortOrder = orderBy.order == ODataOrder.ASC ? SortOrder.ASC
                    : SortOrder.DESC;
            task.querySpec.sortTerm = new QueryTerm();

            TypeName typeName = TypeName.STRING;
            if (orderBy.propertyType != null) {
                try {
                    typeName = Enum.valueOf(TypeName.class, orderBy.propertyType);
                } catch (IllegalArgumentException ex) {
                    op.fail(new IllegalArgumentException("Type name " + orderBy.propertyType +
                            " is not supported for " + UriUtils.URI_PARAM_ODATA_ORDER_BY_TYPE));
                    return null;
                }
            }

            task.querySpec.sortTerm.propertyType = typeName;
            task.querySpec.sortTerm.propertyName = orderBy.propertyName;
        }

        Integer top = UriUtils.getODataTopParamValue(op.getUri());
        if (top != null) {
            if (count && validate) {
                op.fail(new IllegalArgumentException(UriUtils.URI_PARAM_ODATA_COUNT
                        + " cannot be used together with " + UriUtils.URI_PARAM_ODATA_TOP));
                return null;
            }
            task.querySpec.options.add(QueryOption.TOP_RESULTS);
            task.querySpec.resultLimit = top;
        }

        Integer skip = UriUtils.getODataSkipParamValue(op.getUri());
        if (skip != null) {
            op.fail(new IllegalArgumentException(
                    UriUtils.URI_PARAM_ODATA_SKIP + " is not supported, see skipto"));
            return null;
        }

        Integer limit = UriUtils.getODataLimitParamValue(op.getUri());
        if (limit != null && limit > 0) {
            if (count && validate) {
                op.fail(new IllegalArgumentException(UriUtils.URI_PARAM_ODATA_COUNT
                        + " cannot be used together with " + UriUtils.URI_PARAM_ODATA_LIMIT));
                return null;
            }
            if (top != null) {
                op.fail(new IllegalArgumentException(UriUtils.URI_PARAM_ODATA_TOP
                        + " cannot be used together with " + UriUtils.URI_PARAM_ODATA_LIMIT));
                return null;
            }
            task.querySpec.resultLimit = limit;
        }

        return task;
    }
}
