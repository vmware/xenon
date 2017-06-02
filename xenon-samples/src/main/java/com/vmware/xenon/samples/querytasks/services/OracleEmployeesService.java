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

package com.vmware.xenon.samples.querytasks.services;

import java.util.EnumSet;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class OracleEmployeesService extends StatelessService {
    public static final String SELF_LINK = "/oracle-employees";

    @Override
    public void handleGet(Operation get) {

        QueryTask.Query q = new QueryTask.Query();
        q.setTermPropertyName(PersonService.PersonState.FIELD_NAME_EMAIL);
        q.setTermMatchType(QueryTask.QueryTerm.MatchType.WILDCARD);
        q.setTermMatchValue("*@oracle*");

        QueryTask.QuerySpecification qs = new QueryTask.QuerySpecification();
        qs.query = q;
        qs.options = EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);

        QueryTask tsk = QueryTask.create(qs);
        tsk.setDirect(true);

        Operation.CompletionHandler c = (o, ex) -> {
            if (ex != null) {
                get.fail(ex);
                return;
            }
            QueryTask rsp = o.getBody(QueryTask.class);
            if (rsp.results != null) {
                get.setBodyNoCloning(rsp.results);
            }
            get.complete();
        };
        // post to the query service
        Operation postQuery = Operation
                .createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                .setBody(tsk)
                .setCompletion(c);

        sendRequest(postQuery);
    }
}
