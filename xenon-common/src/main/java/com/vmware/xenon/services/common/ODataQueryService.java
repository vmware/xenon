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

import com.vmware.xenon.common.ODataUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;

/**
 * Parses the OData parameters of the URI and issues a direct Query.
 */
public class ODataQueryService extends StatelessService {
    public static String SELF_LINK = ServiceUriPaths.ODATA_QUERIES;

    @Override
    public void handleGet(Operation op) {

        try {
            QueryTask task = ODataUtils.toQuery(op);
            if (task == null) {
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
}
