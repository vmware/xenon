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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

public class BroadcastQueryPageService extends StatelessService {
    public static final String SELF_LINK_PREFIX = "broadcast-query-page";
    public static final String KIND = Utils.buildKind(QueryTask.class);

    private final QueryTask.QuerySpecification spec;
    private final List<String> pageLinks;
    private final long expirationMicros;
    private final NodeGroupBroadcastResponse nodeGroupResponse;
    private String currentPageLink;
    private String prevPageLink;
    private String nextPageLink;

    public BroadcastQueryPageService(QueryTask.QuerySpecification spec, List<String> pageLinks,
            long expMicros, NodeGroupBroadcastResponse nodeGroupResponse, String currentPageLink,
            String prevPageLink, String nextPageLink) {
        super(QueryTask.class);
        this.spec = spec;
        this.pageLinks = pageLinks;
        this.expirationMicros = expMicros;
        this.nodeGroupResponse = nodeGroupResponse;
        this.currentPageLink = currentPageLink;
        this.prevPageLink = prevPageLink;
        this.nextPageLink = nextPageLink;
    }

    @Override
    public void handleStart(Operation post) {
        ServiceDocument initState = post.getBody(ServiceDocument.class);

        long interval = initState.documentExpirationTimeMicros - Utils.getSystemNowMicrosUtc();
        if (interval < 0) {
            logWarning("Task expiration is in the past, extending it");
            interval = getHost().getMaintenanceIntervalMicros() * 2;
        }

        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.setMaintenanceIntervalMicros(interval);

        post.complete();
    }

    @Override
    public void handleGet(Operation get) {
        List<QueryTask> responses = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger remainingQueries = new AtomicInteger(this.pageLinks.size());

        if (remainingQueries.get() == 0) {
            get.complete();
            return;
        }
        for (String indexLink : this.pageLinks) {
            Operation op = Operation
                    .createGet(UriUtils.buildUri(this.getHost(), indexLink))
                    .transferRefererFrom(get)
                    .setExpiration(
                            Utils.fromNowMicrosUtc(
                                    getHost().getOperationTimeoutMicros()))
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            get.fail(e);
                            return;
                        }
                        QueryTask rsp = o.getBody(QueryTask.class);
                        if (rsp != null) {
                            responses.add(rsp);
                        }
                        int r = remainingQueries.decrementAndGet();
                        if (r == 0) {
                            collectPagesAndStartNewServices(responses, (response, error) -> {
                                if (error != null) {
                                    get.fail(error);
                                    return;
                                }
                                rsp.results = response;
                                get.setBodyNoCloning(rsp).complete();
                            });
                        }
                    });
            this.getHost().sendRequest(op);
        }
    }

    private void collectPagesAndStartNewServices(List<QueryTask> responses,
            BiConsumer<ServiceDocumentQueryResult, Throwable> onCompletion) {
        List<ServiceDocumentQueryResult> queryResults = new ArrayList<>();
        List<String> nextPageLinks = new ArrayList<>();
        List<String> prevPageLinks = new ArrayList<>();
        for (QueryTask rsp : responses) {
            if (rsp.results == null) {
                continue;
            }

            queryResults.add(rsp.results);

            if (rsp.results.nextPageLink != null) {
                nextPageLinks.add(rsp.results.nextPageLink);
            }

            if (rsp.results.prevPageLink != null) {
                prevPageLinks.add(rsp.results.prevPageLink);
            }
        }

        ServiceDocumentQueryResult mergeResults = new ServiceDocumentQueryResult();

        // start new BroadcastQueryPageService only when link exists and previously not created

        if (!nextPageLinks.isEmpty()) {
            if (this.nextPageLink == null) {
                this.nextPageLink = startNewService(nextPageLinks, this.currentPageLink, null);
            }
            mergeResults.nextPageLink = this.nextPageLink;
        }

        if (!prevPageLinks.isEmpty()) {
            if (this.prevPageLink == null) {
                this.prevPageLink = startNewService(prevPageLinks, null, this.currentPageLink);
            }
            mergeResults.prevPageLink = this.prevPageLink;
        }

        boolean isAscOrder = this.spec.sortOrder == null
                || this.spec.sortOrder == QueryTask.QuerySpecification.SortOrder.ASC;
        QueryTaskUtils.processQueryResults(getHost(), queryResults,
                isAscOrder, this.spec.options, this.nodeGroupResponse, mergeResults, onCompletion);

    }

    @Override
    public void handleMaintenance(Operation op) {
        op.complete();

        getHost().stopService(this);
    }

    private String startNewService(List<String> pageLinks, String prevPageLink, String nextPageLink) {
        URI broadcastPageServiceUri = UriUtils.buildUri(this.getHost(), UriUtils.buildUriPath(
                ServiceUriPaths.CORE_QUERY_BROADCAST_PAGE, String.valueOf(Utils.getNowMicrosUtc())));

        URI forwarderUri = UriUtils.buildForwardToPeerUri(broadcastPageServiceUri, getHost().getId(),
                ServiceUriPaths.DEFAULT_NODE_SELECTOR, EnumSet.noneOf(ServiceOption.class));

        String broadcastQueryPageLink = forwarderUri.getPath() + UriUtils.URI_QUERY_CHAR + forwarderUri.getQuery();

        ServiceDocument postBody = new ServiceDocument();
        postBody.documentSelfLink = broadcastPageServiceUri.getPath();
        postBody.documentExpirationTimeMicros = this.expirationMicros;

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
                new BroadcastQueryPageService(this.spec, pageLinks, this.expirationMicros, this.nodeGroupResponse,
                        broadcastQueryPageLink, prevPageLink, nextPageLink));

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

    @Override
    public void handleDelete(Operation delete) {

        if (this.pageLinks.isEmpty()) {
            super.handleDelete(delete);
            return;
        }

        // delete associated pages
        OperationJoin.create(
                this.pageLinks.stream().map(link ->
                        Operation.createDelete(this, link).setReferer(getUri())
                )
        ).setCompletion((ops, exs) -> {
            if (exs != null) {
                logWarning("Failed to delete query result pages for broadcast query result %s: %s", getUri(), Utils.toString(exs));
            }
            // delete itself and complete
            super.handleDelete(delete);
        }).sendWith(this);
    }

}
