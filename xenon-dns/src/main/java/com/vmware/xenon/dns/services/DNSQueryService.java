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

package com.vmware.xenon.dns.services;

import java.net.URI;
import java.util.EnumSet;

import com.vmware.xenon.common.ODataQueryVisitor;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.dns.services.DNSService.DNSServiceState;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * A stateless service to retrieve dns records. The service uses ODATA format.
 * Refer DNSServiceState for valid parameters.
 */
public class DNSQueryService extends StatelessService {

    public static String SELF_LINK = ServiceUriPaths.DNS + "/query";

    @Override
    public void handleGet(Operation op) {
        try {
            QueryTask.Query userQuery = getQuery(op.getUri());
            QueryTask.Query query;

            QueryTask.Query kindClause = new QueryTask.Query()
                    .setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                    .setTermMatchValue(Utils.buildKind(DNSServiceState.class));
            if (userQuery == null) {
                query  = kindClause;
            } else {
                query = new QueryTask.Query();
                query.addBooleanClause(userQuery);
                query.addBooleanClause(kindClause);
                query.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
            }

            // create a query spec to query based on serviceName and tags
            QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
            q.options = EnumSet
                    .of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);
            q.query = query;
            QueryTask task = QueryTask.create(q).setDirect(true);
            Operation.CompletionHandler c = (o, ex) -> {
                if (ex != null) {
                    op.fail(ex);
                    return;
                }
                QueryTask rsp = o.getBody(QueryTask.class);
                if (rsp.results != null) {
                    op.setBodyNoCloning(rsp.results);
                }
                op.complete();
            };
            // post to the query service
            Operation postQuery = Operation
                    .createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                    .setBody(task)
                    .setCompletion(c);
            sendRequest(postQuery);
        } catch (Exception e) {
            op.fail(e);
        }
    }

    private QueryTask.Query getQuery(URI uri) {
        String oDataFilterParam = UriUtils.getODataFilterParamValue(uri);
        if (oDataFilterParam == null) {
            return null;
        }

        return new ODataQueryVisitor().toQuery(oDataFilterParam);
    }
}
