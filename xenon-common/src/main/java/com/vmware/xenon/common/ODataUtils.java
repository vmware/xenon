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

import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.UriUtils.ODataOrder;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.SortOrder;
import com.vmware.xenon.services.common.QueryTask.QueryTerm;

public class ODataUtils {

    /**
     * Builds a {@code QueryTask} with a fully formed query and options, from the operation URI
     * query parameters
     */
    public static QueryTask toQuery(Operation op) {
        String oDataFilterParam = UriUtils.getODataFilterParamValue(op.getUri());
        if (oDataFilterParam == null) {
            op.fail(new IllegalArgumentException("filter is required: " + op.getUri().getQuery()));
            return null;
        }

        Query q = new ODataQueryVisitor().toQuery(oDataFilterParam);

        Integer top = UriUtils.getODataTopParamValue(op.getUri());
        Integer skip = UriUtils.getODataSkipParamValue(op.getUri());
        Integer limit = UriUtils.getODataLimitParamValue(op.getUri());
        UriUtils.ODataOrderByTuple orderBy = UriUtils.getODataOrderByParamValue(op.getUri());

        QueryTask task = new QueryTask();
        task.setDirect(true);
        task.querySpec = new QueryTask.QuerySpecification();
        task.querySpec.query.addBooleanClause(q);

        task.querySpec.options.add(QueryOption.EXPAND_CONTENT);

        if (orderBy != null) {
            task.querySpec.options.add(QueryOption.SORT);
            task.querySpec.sortOrder = orderBy.order == ODataOrder.ASC ? SortOrder.ASC
                    : SortOrder.DESC;
            task.querySpec.sortTerm = new QueryTerm();
            // we only support ordering of string fields, through the ODATA service
            task.querySpec.sortTerm.propertyType = TypeName.STRING;
            task.querySpec.sortTerm.propertyName = orderBy.propertyName;
        }

        if (top != null) {
            task.querySpec.options.add(QueryOption.TOP_RESULTS);
            task.querySpec.resultLimit = top;
        }

        if (skip != null) {
            op.fail(new IllegalArgumentException(
                    UriUtils.URI_PARAM_ODATA_SKIP + " is not supported, see skipto"));
            return null;
        }

        if (limit != null && limit > 0) {
            if (top != null) {
                op.fail(new IllegalArgumentException(UriUtils.URI_PARAM_ODATA_TOP
                        + " cannot be used together with " + UriUtils.URI_PARAM_ODATA_LIMIT));
                return null;
            }
            task.querySpec.resultLimit = limit;
        }

        if (q == null) {
            op.fail(new IllegalArgumentException(UriUtils.URI_PARAM_ODATA_FILTER + " is required"
                    + op.getUri().getQuery()));
            return null;
        }

        return task;
    }
}
