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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.dns.services.DNSService.DNSServiceState;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * A stateless service to retrieve dns records The user can input the serviceName and one or more
 * tags for the service as a comma separated list Either parameter is optional. Tags are ANDed. Both
 * serviceName and tags can be wildcards
 */
public class DNSQueryService extends StatelessService {

    public static String SELF_LINK = DNSUriPaths.DNS + "/query";

    @Override
    public void handleGet(Operation op) {
        Map<String, String> params = UriUtils.parseUriQueryParams(op.getUri());
        String serviceName = params.get(DNSServiceState.FIELD_NAME_SERVICE_NAME);
        // extract the list of tags if specified
        String tagString = params.get(DNSServiceState.FIELD_NAME_SERVICE_TAGS);
        List<String> tagList = null;
        if (tagString != null) {
            tagList = Arrays.asList(tagString.split(","));
        }

        boolean filterAvailable = params.containsKey(DNSServiceState.FIELD_NAME_SERVICE_AVAILABLE);

        // create a query spec to query based on serviceName and tags
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();

        q.options = EnumSet
                .of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);
        QueryTask.Query serviceNameClause = null;
        boolean filtered = false;
        if (serviceName != null) {
            serviceNameClause = new QueryTask.Query()
                    .setTermPropertyName(DNSServiceState.FIELD_NAME_SERVICE_NAME)
                    .setTermMatchValue(serviceName);
            serviceNameClause.term.matchType = QueryTask.QueryTerm.MatchType.WILDCARD;
            serviceNameClause.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
            q.query.addBooleanClause(serviceNameClause);
            filtered = true;
        }
        QueryTask.Query tagClause = null;
        if (tagList != null) {
            for (String tag : tagList) {
                tagClause = new QueryTask.Query()
                        .setTermPropertyName(
                                QuerySpecification
                                        .buildCollectionItemName(DNSServiceState.FIELD_NAME_SERVICE_TAGS))
                        .setTermMatchValue(tag);
                tagClause.term.matchType = QueryTask.QueryTerm.MatchType.WILDCARD;
                tagClause.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
                q.query.addBooleanClause(tagClause);
            }
            filtered = true;
        }
        QueryTask.Query availableClause = null;
        if (filterAvailable) {
            availableClause = new QueryTask.Query()
                    .setTermPropertyName(DNSServiceState.FIELD_NAME_SERVICE_AVAILABLE)
                    .setTermMatchValue("true");
            availableClause.term.matchType = QueryTask.QueryTerm.MatchType.TERM;
            availableClause.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
            q.query.addBooleanClause(availableClause);
            filtered = true;
        }

        // add a document kind clause to restrict the scope of the search
        if (filtered) {
            QueryTask.Query kindClause = new QueryTask.Query()
                    .setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                    .setTermMatchValue(
                            Utils.buildKind(DNSServiceState.class));
            q.query.addBooleanClause(kindClause);
        } else {
            q.query.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND);
            q.query.setTermMatchValue(Utils.buildKind(DNSServiceState.class));
        }

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
    }
}
