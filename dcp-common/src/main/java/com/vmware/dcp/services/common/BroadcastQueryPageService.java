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

package com.vmware.dcp.services.common;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.ServiceDocumentQueryResult;
import com.vmware.dcp.common.StatelessService;
import com.vmware.dcp.common.TaskState;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;

public class BroadcastQueryPageService extends StatelessService {
    public static final String SELF_LINK_PREFIX = "broadcast-query-page";
    public static final String KIND = Utils.buildKind(QueryTask.class);

    private QueryTask.QuerySpecification spec;
    private List<String> pageLinks;

    public BroadcastQueryPageService(QueryTask.QuerySpecification spec, List<String> pageLinks) {
        super(QueryTask.class);
        this.spec = spec;
        this.pageLinks = pageLinks;
    }

    @Override
    public void handleStart(Operation post) {
        ServiceDocument initState = post.getBody(ServiceDocument.class);

        long interval = initState.documentExpirationTimeMicros - Utils.getNowMicrosUtc();
        if (interval < 0) {
            logWarning("Task expiration is in the past, extending it");
            interval = TimeUnit.SECONDS.toMicros(getHost().getMaintenanceIntervalMicros() * 2);
        }

        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.setMaintenanceIntervalMicros(interval);

        post.complete();
    }

    @Override
    public void handleGet(Operation get) {
        List<QueryTask> responses = new ArrayList<>();
        AtomicInteger remainingQueries = new AtomicInteger(this.pageLinks.size());

        for (String indexLink : this.pageLinks) {
            Operation op = Operation
                    .createGet(UriUtils.buildUri(this.getHost(), indexLink))
                    .setReferer(get.getReferer())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            QueryTask t = new QueryTask();
                            t.taskInfo.stage = TaskState.TaskStage.FAILED;
                            t.taskInfo.failure = Utils.toServiceErrorResponse(e);
                            get.setBody(t).fail(e);

                            return;
                        }

                        QueryTask rsp = o.getBody(QueryTask.class);
                        responses.add(rsp);

                        if (remainingQueries.decrementAndGet() == 0) {
                            rsp.results = collectPagesAndStartNewServices(responses, get);
                            get.setBodyNoCloning(rsp).complete();
                        }
                    });
            this.getHost().sendRequest(op);
        }
    }

    private ServiceDocumentQueryResult collectPagesAndStartNewServices(List<QueryTask> responses, Operation origOperation) {
        List<ServiceDocumentQueryResult> queryResults = new ArrayList<>();
        List<String> nextPageLinks = new ArrayList<>();
        List<String> prevPageLinks = new ArrayList<>();
        for (QueryTask rsp : responses) {
            queryResults.add(rsp.results);

            if (rsp.results.nextPageLink != null) {
                nextPageLinks.add(rsp.results.nextPageLink);
            }

            if (rsp.results.prevPageLink != null) {
                prevPageLinks.add(rsp.results.prevPageLink);
            }
        }

        boolean isAscOrder = this.spec.sortOrder == null
                || this.spec.sortOrder == QueryTask.QuerySpecification.SortOrder.ASC;
        ServiceDocumentQueryResult mergeResults = Utils.mergeQueryResults(queryResults, isAscOrder);

        if (!nextPageLinks.isEmpty()) {
            mergeResults.nextPageLink = startNewService(nextPageLinks);
        }

        if (!prevPageLinks.isEmpty()) {
            mergeResults.prevPageLink = startNewService(prevPageLinks);
        }

        return mergeResults;
    }

    @Override
    public void handleMaintenance(Operation op) {
        op.complete();

        getHost().stopService(this);
    }

    private String startNewService(List<String> pageLinks) {
        URI broadcastPageServiceUri = UriUtils.buildUri(this.getHost(), UriUtils.buildUriPath(ServiceUriPaths.CORE,
                BroadcastQueryPageService.SELF_LINK_PREFIX, String.valueOf(Utils.getNowMicrosUtc())));

        URI forwarderUri = UriUtils.buildForwardToPeerUri(broadcastPageServiceUri, getHost().getId(),
                ServiceUriPaths.DEFAULT_NODE_SELECTOR, EnumSet.noneOf(ServiceOption.class));

        String broadcastQueryPageLink = forwarderUri.getPath() + UriUtils.URI_QUERY_CHAR + forwarderUri.getQuery();

        ServiceDocument postBody = new ServiceDocument();
        postBody.documentSelfLink = broadcastPageServiceUri.getPath();

        Operation startPost = Operation
                .createPost(broadcastPageServiceUri)
                .setBody(postBody)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        failTask(e, o, null);
                        return;
                    }
                });
        this.getHost().startService(startPost,
                new BroadcastQueryPageService(this.spec, pageLinks));

        return broadcastQueryPageLink;
    }

    private void failTask(Throwable e, Operation directOp, Operation.CompletionHandler c) {
        QueryTask t = new QueryTask();
        // self patch to failure
        t.taskInfo.stage = TaskState.TaskStage.FAILED;
        t.taskInfo.failure = Utils.toServiceErrorResponse(e);
        if (directOp != null) {
            directOp.setBody(t).fail(e);
            return;
        }

        sendRequest(Operation.createPatch(getUri()).setBody(t).setCompletion(c));
    }
}
