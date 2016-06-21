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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;

public class QueryTaskService extends StatefulService {
    private static final long DEFAULT_EXPIRATION_SECONDS = 600;
    private ServiceDocumentQueryResult results;

    public QueryTaskService() {
        super(QueryTask.class);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        if (!startPost.hasBody()) {
            startPost.fail(new IllegalArgumentException("Body is required"));
            return;
        }

        QueryTask initState = startPost.getBody(QueryTask.class);
        if (initState.taskInfo == null) {
            initState.taskInfo = new TaskState();
        } else if (TaskState.isFinished(initState.taskInfo)) {
            startPost.complete();
            return;
        }

        if (!validateState(initState, startPost)) {
            return;
        }

        if (initState.documentExpirationTimeMicros == 0) {
            // always set expiration so we do not accumulate tasks
            initState.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + TimeUnit.SECONDS.toMicros(DEFAULT_EXPIRATION_SECONDS);
        }
        initState.taskInfo.stage = TaskStage.CREATED;

        if (!initState.taskInfo.isDirect) {
            // complete POST immediately
            startPost.setStatusCode(Operation.STATUS_CODE_ACCEPTED).complete();
            // kick off query processing by patching self to STARTED
            QueryTask patchBody = new QueryTask();
            patchBody.taskInfo = new TaskState();
            patchBody.taskInfo.stage = TaskStage.STARTED;
            patchBody.querySpec = initState.querySpec;
            sendRequest(Operation.createPatch(getUri()).setBody(patchBody));
        } else {
            if (initState.querySpec.options.contains(QueryOption.BROADCAST)) {
                createAndSendBroadcastQuery(initState, startPost);
            } else {
                forwardQueryToDocumentIndexService(initState, startPost);
            }
        }
    }

    private boolean validateState(QueryTask initState, Operation startPost) {
        if (initState.querySpec == null) {
            startPost.fail(new IllegalArgumentException("querySpec is required"));
            return false;
        }

        if (initState.querySpec.query == null) {
            startPost.fail(new IllegalArgumentException("querySpec.query is required"));
            return false;
        }

        if (initState.querySpec.options == null || initState.querySpec.options.isEmpty()) {
            return true;
        }

        if (initState.querySpec.options.contains(QueryOption.EXPAND_LINKS)) {
            if (!initState.querySpec.options.contains(QueryOption.SELECT_LINKS)) {
                startPost.fail(new IllegalArgumentException(
                        "Must be combined with " + QueryOption.SELECT_LINKS));
                return false;
            }
            // additional option combination validation will be done in the SELECT_LINKS
            // block, since that option must be combined with this one
        }

        if (initState.querySpec.options.contains(QueryOption.SELECT_LINKS)) {
            final String errFmt = QueryOption.SELECT_LINKS + " is not compatible with %s";
            if (initState.querySpec.options.contains(QueryOption.COUNT)) {
                startPost.fail(new IllegalArgumentException(
                        String.format(errFmt, QueryOption.COUNT)));
                return false;
            }
            if (initState.querySpec.options.contains(QueryOption.CONTINUOUS)) {
                startPost.fail(new IllegalArgumentException(
                        String.format(errFmt, QueryOption.CONTINUOUS)));
                return false;
            }
            if (initState.querySpec.linkTerms == null || initState.querySpec.linkTerms.isEmpty()) {
                startPost.fail(new IllegalArgumentException(
                        "querySpec.linkTerms must have at least one entry"));
                return false;
            }
        }

        if (initState.taskInfo.isDirect
                && initState.querySpec.options.contains(QueryOption.CONTINUOUS)) {
            startPost.fail(new IllegalArgumentException("direct query task is not compatible with "
                    + QueryOption.CONTINUOUS));
            return false;
        }

        if (initState.querySpec.options.contains(QueryOption.BROADCAST)
                && initState.querySpec.options.contains(QueryOption.SORT)
                && initState.querySpec.sortTerm != null
                && !Objects.equals(initState.querySpec.sortTerm.propertyName, ServiceDocument.FIELD_NAME_SELF_LINK)) {
            startPost.fail(new IllegalArgumentException(QueryOption.BROADCAST
                    + " only supports sorting on ["
                    + ServiceDocument.FIELD_NAME_SELF_LINK + "]"));
            return false;
        }

        return true;
    }

    private void createAndSendBroadcastQuery(QueryTask origQueryTask, Operation startPost) {
        QueryTask queryTask = Utils.clone(origQueryTask);
        queryTask.setDirect(true);

        queryTask.querySpec.options.remove(QueryOption.BROADCAST);

        if (!queryTask.querySpec.options.contains(QueryOption.SORT)) {
            queryTask.querySpec.options.add(QueryOption.SORT);
            queryTask.querySpec.sortOrder = QuerySpecification.SortOrder.ASC;
            queryTask.querySpec.sortTerm = new QueryTask.QueryTerm();
            queryTask.querySpec.sortTerm.propertyType = ServiceDocumentDescription.TypeName.STRING;
            queryTask.querySpec.sortTerm.propertyName = ServiceDocument.FIELD_NAME_SELF_LINK;
        }

        URI localQueryTaskFactoryUri = UriUtils.buildUri(this.getHost(),
                ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        URI forwardingService = UriUtils.buildBroadcastRequestUri(localQueryTaskFactoryUri,
                queryTask.nodeSelectorLink);

        Operation op = Operation
                .createPost(forwardingService)
                .setBody(queryTask)
                .setReferer(this.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        failTask(e, startPost, null);
                        return;
                    }

                    NodeGroupBroadcastResponse rsp = o.getBody((NodeGroupBroadcastResponse.class));
                    if (!rsp.failures.isEmpty()) {
                        if (rsp.jsonResponses.size() < rsp.membershipQuorum) {
                            failTask(new IllegalStateException(
                                    "Failures received: " + Utils.toJsonHtml(rsp)),
                                    startPost, null);
                            return;
                        } else {
                            logWarning(
                                    "task will proceed, received %d responses (for quorum size %d)"
                                            + "even though %d errors were received: %s",
                                    rsp.jsonResponses.size(), rsp.membershipQuorum,
                                    rsp.failures.size(), rsp.failures.keySet());
                        }
                    }

                    collectBroadcastQueryResults(rsp.jsonResponses, queryTask);

                    queryTask.taskInfo.stage = TaskStage.FINISHED;
                    if (startPost != null) {
                        // direct query, complete original POST
                        startPost.setBodyNoCloning(queryTask).complete();
                    } else {
                        // self patch with results
                        sendRequest(Operation.createPatch(getUri()).setBodyNoCloning(queryTask));
                    }
                });
        this.getHost().sendRequest(op);
    }

    private void collectBroadcastQueryResults(Map<URI, String> jsonResponses, QueryTask queryTask) {
        long startTime = Utils.getNowMicrosUtc();

        List<ServiceDocumentQueryResult> queryResults = new ArrayList<>();
        for (Map.Entry<URI, String> entry : jsonResponses.entrySet()) {
            QueryTask rsp = Utils.fromJson(entry.getValue(), QueryTask.class);
            queryResults.add(rsp.results);
        }

        boolean isPaginatedQuery = queryTask.querySpec.resultLimit != null
                && queryTask.querySpec.resultLimit < Integer.MAX_VALUE
                && !queryTask.querySpec.options.contains(QueryOption.TOP_RESULTS);

        if (!isPaginatedQuery) {
            boolean isAscOrder = queryTask.querySpec.sortOrder == null
                    || queryTask.querySpec.sortOrder == QuerySpecification.SortOrder.ASC;

            queryTask.results = QueryTaskUtils.mergeQueryResults(queryResults, isAscOrder,
                    queryTask.querySpec.options);
        } else {
            URI broadcastPageServiceUri = UriUtils.buildUri(this.getHost(), UriUtils.buildUriPath(ServiceUriPaths.CORE,
                    BroadcastQueryPageService.SELF_LINK_PREFIX, String.valueOf(Utils.getNowMicrosUtc())));

            URI forwarderUri = UriUtils.buildForwardToPeerUri(broadcastPageServiceUri, getHost().getId(),
                    ServiceUriPaths.DEFAULT_NODE_SELECTOR, EnumSet.noneOf(ServiceOption.class));

            ServiceDocument postBody = new ServiceDocument();
            postBody.documentSelfLink = broadcastPageServiceUri.getPath();
            postBody.documentExpirationTimeMicros = queryTask.documentExpirationTimeMicros;

            Operation startPost = Operation
                    .createPost(broadcastPageServiceUri)
                    .setBody(postBody)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            failTask(e, o, null);
                        }
                    });

            List<String> nextPageLinks = queryResults.stream()
                    .filter(r -> r.nextPageLink != null)
                    .map(r -> r.nextPageLink)
                    .collect(Collectors.toList());

            queryTask.results = new ServiceDocumentQueryResult();
            queryTask.results.documentCount = 0L;

            if (!nextPageLinks.isEmpty()) {
                queryTask.results.nextPageLink = forwarderUri.getPath() + UriUtils.URI_QUERY_CHAR +
                        forwarderUri.getQuery();
                this.getHost().startService(startPost,
                        new BroadcastQueryPageService(queryTask.querySpec, nextPageLinks,
                                queryTask.documentExpirationTimeMicros));
            } else {
                queryTask.results.nextPageLink = null;
            }
        }

        long timeElapsed = Utils.getNowMicrosUtc() - startTime;
        queryTask.taskInfo.durationMicros = timeElapsed + Collections.max(queryResults.stream().map(r -> r
                .queryTimeMicros).collect(Collectors.toList()));
    }

    @Override
    public void handleGet(Operation get) {
        QueryTask currentState = Utils.clone(getState(get));
        ServiceDocumentQueryResult r = this.results;
        if (r == null || currentState == null) {
            get.setBodyNoCloning(currentState).complete();
            return;
        }

        // Infrastructure special case, do not cut and paste in services:
        // the results might contain non clonable JSON serialization artifacts so we go through
        // all these steps to use cached results, avoid cloning, etc This is NOT what services
        // should be doing but due to a unfortunate combination of KRYO and GSON, we cant
        // use results as the body, since it will not clone properly
        currentState.results = new ServiceDocumentQueryResult();
        r.copyTo(this.results);

        currentState.results.documentCount = r.documentCount;
        currentState.results.nextPageLink = r.nextPageLink;
        currentState.results.prevPageLink = r.prevPageLink;

        if (r.documentLinks != null) {
            currentState.results.documentLinks = new ArrayList<>(r.documentLinks);
        }
        if (r.documents != null) {
            currentState.results.documents = new HashMap<>(r.documents);
        }
        if (r.selectedLinksPerDocument != null) {
            currentState.results.selectedLinksPerDocument = new HashMap<>(r.selectedLinksPerDocument);
        }
        if (r.selectedLinks != null) {
            currentState.results.selectedLinks = new HashSet<>(r.selectedLinks);
        }
        if (r.selectedDocuments != null) {
            currentState.results.selectedDocuments = new HashMap<>(r.selectedDocuments);
        }

        get.setBodyNoCloning(currentState).complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        if (patch.isFromReplication()) {
            patch.complete();
            return;
        }

        QueryTask state = getState(patch);

        if (state == null) {
            // service has likely expired
            patch.fail(new IllegalStateException("service state missing"));
            return;
        }

        QueryTask patchBody = patch.getBody(QueryTask.class);
        TaskState newTaskState = patchBody.taskInfo;

        this.results = patchBody.results;

        if (newTaskState == null) {
            patch.fail(new IllegalArgumentException("taskInfo is required"));
            return;
        }

        if (newTaskState.stage == null) {
            patch.fail(new IllegalArgumentException("stage is required"));
            return;
        }

        if (state.querySpec.options.contains(QueryOption.CONTINUOUS)) {
            if (handlePatchForContinuousQuery(state, patchBody, patch)) {
                return;
            }
        }

        if (newTaskState.stage.ordinal() <= state.taskInfo.stage.ordinal()) {
            patch.fail(new IllegalArgumentException(
                    "new stage must be greater than current"));
            return;
        }

        state.taskInfo = newTaskState;
        if (newTaskState.stage == TaskStage.STARTED) {
            patch.setStatusCode(Operation.STATUS_CODE_ACCEPTED);
        } else if (newTaskState.stage == TaskStage.FAILED
                || newTaskState.stage == TaskStage.CANCELLED) {
            if (newTaskState.failure == null) {
                patch.fail(new IllegalArgumentException(
                        "failure must be specified"));
                return;
            }
            logWarning("query failed: %s", newTaskState.failure.message);
        }

        patch.complete();

        if (newTaskState.stage == TaskStage.STARTED) {
            if (patchBody.querySpec.options.contains(QueryOption.BROADCAST)) {
                createAndSendBroadcastQuery(patchBody, null);
            } else {
                forwardQueryToDocumentIndexService(state, null);
            }
        }
    }

    private boolean handlePatchForContinuousQuery(QueryTask state, QueryTask patchBody,
            Operation patch) {
        switch (state.taskInfo.stage) {
        case STARTED:
            // handled below
            break;
        default:
            return false;
        }

        // handle transitions from the STARTED stage
        switch (patchBody.taskInfo.stage) {
        case CREATED:
            return false;
        case STARTED:
            // if the new state is STARTED, and we are in STARTED, this is just a update notification
            // from the index that either the initial query completed, or a new update passed the
            // query filter. Subscribers can subscribe to this task and see what changed.
            break;

        case CANCELLED:
        case FAILED:
        case FINISHED:
            cancelContinuousQueryOnIndex(state);
            break;
        default:
            break;
        }

        patch.complete();
        return true;
    }

    private void forwardQueryToDocumentIndexService(QueryTask task, Operation directOp) {
        try {
            if (task.querySpec.resultLimit == null) {
                task.querySpec.resultLimit = Integer.MAX_VALUE;
            }

            Operation localPatch = Operation
                    .createPatch(this, task.indexLink)
                    .setBodyNoCloning(task)
                    .setCompletion((o, e) -> {
                        if (e == null) {
                            task.results = (ServiceDocumentQueryResult) o.getBodyRaw();
                        }

                        handleQueryCompletion(task, e, directOp);
                    });

            sendRequest(localPatch);
        } catch (Throwable e) {
            handleQueryCompletion(task, e, directOp);
        }
    }

    private void scheduleTaskExpiration(QueryTask task) {
        if (task.taskInfo.isDirect) {
            getHost().stopService(this);
            return;
        }

        if (getHost().isStopping()) {
            return;
        }

        Operation delete = Operation.createDelete(getUri()).setBody(new ServiceDocument());
        long delta = task.documentExpirationTimeMicros - Utils.getNowMicrosUtc();
        delta = Math.max(1, delta);
        getHost().schedule(() -> {
            if (task.querySpec.options.contains(QueryOption.CONTINUOUS)) {
                cancelContinuousQueryOnIndex(task);
            }
            sendRequest(delete);
        }, delta, TimeUnit.MICROSECONDS);
    }

    private void cancelContinuousQueryOnIndex(QueryTask task) {
        QueryTask body = new QueryTask();
        body.documentSelfLink = task.documentSelfLink;
        body.taskInfo.stage = TaskStage.CANCELLED;
        body.querySpec = task.querySpec;
        body.documentKind = task.documentKind;
        Operation cancelActiveQueryPatch = Operation
                .createPatch(this, task.indexLink)
                .setBodyNoCloning(body);
        sendRequest(cancelActiveQueryPatch);
    }

    private void failTask(Throwable e, Operation directOp, CompletionHandler c) {
        QueryTask t = new QueryTask();
        // self patch to failure
        t.taskInfo.stage = TaskStage.FAILED;
        t.taskInfo.failure = Utils.toServiceErrorResponse(e);
        if (directOp != null) {
            directOp.setBody(t).fail(e);
            return;
        }

        sendRequest(Operation.createPatch(getUri()).setBody(t).setCompletion(c));
    }

    private boolean handleQueryRetry(QueryTask task, Operation directOp) {
        if (task.querySpec.expectedResultCount == null) {
            return false;
        }

        if (task.results.documentCount >= task.querySpec.expectedResultCount) {
            return false;
        }

        // Fail the task now if we would expire within the next maint interval.
        // Otherwise self patch can fail if the document has expired and clients
        // need a chance to GET the FAILED state.
        long exp = task.documentExpirationTimeMicros - getHost().getMaintenanceIntervalMicros();
        if (exp < Utils.getNowMicrosUtc()) {
            failTask(new TimeoutException(), directOp, (o, e) -> {
                scheduleTaskExpiration(task);
            });
            return true;
        }

        getHost().schedule(() -> {
            forwardQueryToDocumentIndexService(task, directOp);
        }, getMaintenanceIntervalMicros(), TimeUnit.MICROSECONDS);

        return true;
    }

    private void handleQueryCompletion(QueryTask task, Throwable e, Operation directOp) {
        boolean scheduleExpiration = true;

        try {
            task.querySpec.context.nativeQuery = null;

            if (e != null) {
                failTask(e, directOp, null);
                return;
            }

            if (handleQueryRetry(task, directOp)) {
                scheduleExpiration = false;
                return;
            }

            if (task.querySpec.options.contains(QueryOption.CONTINUOUS)) {
                // A continuous query does not cache results: since it receive updates
                // at any time, a GET on the query will cause the query to be re-computed. This is
                // costly, so it should be avoided.
                task.taskInfo.stage = TaskStage.STARTED;
            } else {
                this.results = task.results;
                task.taskInfo.stage = TaskStage.FINISHED;
                task.taskInfo.durationMicros = task.results.queryTimeMicros;
            }

            if (task.documentOwner == null) {
                task.documentOwner = getHost().getId();
            }

            scheduleExpiration = !task.querySpec.options.contains(QueryOption.EXPAND_LINKS);
            if (directOp != null) {
                if (!task.querySpec.options.contains(QueryOption.EXPAND_LINKS)) {
                    directOp.setBodyNoCloning(task).complete();
                    return;
                }
                directOp.nestCompletion((o, ex) -> {
                    directOp.setStatusCode(o.getStatusCode())
                            .setBodyNoCloning(o.getBodyRaw()).complete();
                    scheduleTaskExpiration(task);
                });
                QueryTaskUtils.expandLinks(getHost(), task, directOp);
            } else {
                if (!task.querySpec.options.contains(QueryOption.EXPAND_LINKS)) {
                    sendRequest(Operation.createPatch(getUri()).setBodyNoCloning(task));
                    return;
                }

                CompletionHandler c = (o, ex) -> {
                    scheduleTaskExpiration(task);
                    if (ex != null) {
                        failTask(ex, null, null);
                        return;
                    }
                    sendRequest(Operation.createPatch(getUri()).setBodyNoCloning(task));
                };
                Operation dummyOp = Operation.createGet(getHost().getUri()).setCompletion(c)
                        .setReferer(getUri());
                QueryTaskUtils.expandLinks(getHost(), task, dummyOp);
            }
        } finally {
            if (scheduleExpiration) {
                scheduleTaskExpiration(task);
            }
        }
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument td = super.getDocumentTemplate();
        QueryTask template = (QueryTask) td;
        QuerySpecification q = new QueryTask.QuerySpecification();

        QueryTask.Query kindClause = new QueryTask.Query().setTermPropertyName(
                ServiceDocument.FIELD_NAME_KIND).setTermMatchValue(
                Utils.buildKind(ExampleServiceState.class));

        QueryTask.Query nameClause = new QueryTask.Query();
        nameClause.setTermPropertyName("name")
                .setTermMatchValue("query-target")
                .setTermMatchType(MatchType.PHRASE);

        q.query.addBooleanClause(kindClause).addBooleanClause(nameClause);
        template.querySpec = q;

        QueryTask exampleTask = new QueryTask();
        template.indexLink = exampleTask.indexLink;
        return template;
    }
}
