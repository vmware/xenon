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

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.Utils;

public class QueryTaskFactoryService extends FactoryService {
    public static final String SELF_LINK = ServiceUriPaths.CORE_QUERY_TASKS;

    public QueryTaskFactoryService() {
        super(QueryTask.class);
    }

    @Override
    public void handlePost(Operation post) {
        if (!post.hasBody()) {
            post.fail(new IllegalArgumentException("body is required"));
            return;
        }

        QueryTask initState = post.getBody(QueryTask.class);

        if (initState.taskInfo.isDirect) {
            // do not replicate direct queries, they auto expire immediately
            post.setReplicationDisabled(true);
        }

        // check if this is a clone request
        if (initState.documentSourceLink == null) {
            // let the super class handle the POST
            post.complete();
            return;
        }

        // the client has request we clone an existing query task, and use its state to create a new
        // task service
        if (!getHost().checkServiceAvailable(initState.documentSourceLink)) {
            post.fail(new IllegalStateException("Source query task is not available:"
                    + initState.documentSourceLink));
            return;
        }

        // retrieve the source document
        sendRequest(Operation
                .createGet(this, initState.documentSourceLink)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                logWarning("Failure retrieving state from %s: %s",
                                        initState.documentSourceLink, Utils.toString(e));
                                post.fail(e);
                                return;
                            }

                            QueryTask mergedInitState = o.getBody(QueryTask.class);
                            mergedInitState.taskInfo.failure = null;
                            mergedInitState.taskInfo.stage = TaskStage.CREATED;
                            mergedInitState.results = null;
                            mergedInitState.documentSelfLink = initState.documentSelfLink;
                            mergedInitState.documentOwner = initState.documentOwner;
                            mergedInitState.documentSourceLink = initState.documentSourceLink;
                            mergedInitState.documentDescription = null;

                            if (initState.taskInfo != null
                                    && initState.taskInfo.isDirect != mergedInitState.taskInfo.isDirect) {
                                mergedInitState.taskInfo.isDirect = initState.taskInfo.isDirect;
                            }

                            // we don't currently support merging the query specification between
                            // the source and the new state

                            // super class will now create a new service using the merged body as
                            // initial state
                            post.setBody(mergedInitState).complete();
                        }));
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new QueryTaskService();
    }

}