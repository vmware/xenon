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

package com.vmware.xenon.services.common;

import java.net.URI;

import com.vmware.xenon.common.ODataQueryVisitor;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.UriUtils.ODataOrder;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.SortOrder;
import com.vmware.xenon.services.common.QueryTask.QueryTerm;

/**
 * Parses the OData parameters of the URI and issues a direct Query.
 */
public class ODataQueryService extends StatelessService {
    public static String SELF_LINK = ServiceUriPaths.ODATA_QUERIES;

    @Override
    public void handleGet(Operation op) {

        try {
            QueryTask.Query q = getFilterQuery(op.getUri());

            Integer top = UriUtils.getODataTopParamValue(op.getUri());
            Integer skip = UriUtils.getODataSkipParamValue(op.getUri());
            UriUtils.ODataOrderByTuple orderBy = UriUtils.getODataOrderByParamValue(op.getUri());

            QueryTask task = new QueryTask();
            task.setDirect(true);
            task.querySpec = new QueryTask.QuerySpecification();
            task.querySpec.query = q;

            // always expand
            task.querySpec.options.add(QueryOption.EXPAND_CONTENT);

            if (orderBy != null) {
                task.querySpec.options.add(QueryOption.SORT);
                task.querySpec.sortOrder = orderBy.order == ODataOrder.ASC ? SortOrder.ASC
                        : SortOrder.DESC;
                task.querySpec.sortTerm = new QueryTerm();
                task.querySpec.sortTerm.propertyName = orderBy.propertyName;
            }

            if (top != null) {
                task.querySpec.resultLimit = top;
            }

            if (skip != null) {
                op.fail(new IllegalArgumentException(
                        UriUtils.URI_PARAM_ODATA_SKIP + " is not supported"));
                return;
            }

            sendRequest(Operation.createPost(this, ServiceUriPaths.CORE_QUERY_TASKS).setBody(task)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            op.fail(e);
                            return;
                        }

                        QueryTask result = o.getBody(QueryTask.class);
                        op.setBodyNoCloning(result);
                        op.complete();
                    }));
        } catch (Exception e) {
            op.fail(e);
        }
    }

    private QueryTask.Query getFilterQuery(URI uri) throws Exception {
        String oDataFilterParam = UriUtils.getODataFilterParamValue(uri);
        if (oDataFilterParam == null) {
            throw new IllegalArgumentException("$filter query not found");
        }

        return new ODataQueryVisitor().toQuery(oDataFilterParam);
    }

}
