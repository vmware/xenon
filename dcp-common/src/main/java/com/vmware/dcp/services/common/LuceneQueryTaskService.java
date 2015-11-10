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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.ServiceDocumentQueryResult;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.TaskState;
import com.vmware.dcp.common.TaskState.TaskStage;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.services.common.ExampleService.ExampleServiceState;
import com.vmware.dcp.services.common.QueryTask.QuerySpecification;
import com.vmware.dcp.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.dcp.services.common.QueryTask.QueryTerm.MatchType;

public class LuceneQueryTaskService extends StatefulService {
    private static final long DEFAULT_EXPIRATION_SECONDS = 600;
    private ServiceDocumentQueryResult results;

    public LuceneQueryTaskService() {
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
            sendRequest(Operation.createPatch(getUri()).setBody(patchBody));
        } else {
            // Complete POST when we have results
            this.convertAndForwardToLucene(initState, startPost);
        }
    }

    private boolean validateState(QueryTask initState, Operation startPost) {
        if (initState.querySpec == null) {
            startPost.fail(new IllegalArgumentException("specification is required"));
            return false;
        }

        if (initState.querySpec.query == null) {
            startPost.fail(new IllegalArgumentException("specification.query is required"));
            return false;
        }

        if (initState.taskInfo.isDirect
                && initState.querySpec.options != null
                && initState.querySpec.options.contains(QueryOption.CONTINUOUS)) {
            startPost.fail(new IllegalArgumentException("direct query task is not compatible with "
                    + QueryOption.CONTINUOUS));
            return false;
        }

        return true;
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
            convertAndForwardToLucene(state, null);
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

    private void convertAndForwardToLucene(QueryTask task, Operation directOp) {
        try {
            org.apache.lucene.search.Query q =
                    LuceneQueryConverter.convertToLuceneQuery(task.querySpec.query);

            task.querySpec.context.nativeQuery = q;

            org.apache.lucene.search.Sort sort = null;
            if (task.querySpec.options != null
                    && task.querySpec.options.contains(QuerySpecification.QueryOption.SORT)) {
                sort = LuceneQueryConverter.convertToLuceneSort(task.querySpec);
            }

            task.querySpec.context.nativeSort = sort;

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
            convertAndForwardToLucene(task, directOp);
        }, getMaintenanceIntervalMicros(), TimeUnit.MICROSECONDS);

        return true;
    }

    private void handleQueryCompletion(QueryTask task, Throwable e, Operation directOp) {
        boolean scheduleExpiration = true;

        try {
            task.querySpec.context.nativeQuery = null;
            if (task.postProcessingSpec != null) {
                e = new IllegalArgumentException(
                        "Post processing is not currently supported");
            }

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

            if (directOp != null) {
                directOp.setBodyNoCloning(task).complete();
            } else {
                // PATCH self to finished
                // we do not clone our state since we already cloned before the query
                // started
                sendRequest(Operation.createPatch(getUri()).setBodyNoCloning(task));
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
