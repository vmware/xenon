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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceMaintenanceRequest;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Test service used to validate document queries
 */
public class ReplicationTestService extends StatefulService {

    public static final String ERROR_MESSAGE_STRING_FIELD_IS_REQUIRED = "stringField is required";
    public static final String REFERER_TOKEN = UUID.randomUUID().toString();
    public static final String STRING_MARKER_FAIL_WITH_CONFLICT_CODE = "fail request withconflict error code, verify no retry";

    public static class ReplicationTestServiceState extends ServiceDocument {
        public static final String CLIENT_PATCH_HINT = "client-";
        public String stringField;
        public String queryTaskLink;
        public String failureNodeId;
    }

    public static class ReplicationTestServiceErrorResponse extends ServiceErrorResponse {

        public static final String KIND = Utils
                .buildKind(ReplicationTestServiceErrorResponse.class);

        public static ReplicationTestServiceErrorResponse create(String message) {
            ReplicationTestServiceErrorResponse er = new ReplicationTestServiceErrorResponse();
            er.message = message;
            er.documentKind = KIND;
            er.customErrorField = Math.PI;
            return er;
        }

        public double customErrorField;
    }

    public ReplicationTestService() {
        super(ReplicationTestServiceState.class);
    }

    @Override
    public void handleRequest(Operation op) {

        logInfo("Handling update of type " + op.getAction() + " (ID " + op.getId() + ")"
                + " on host " + getHost().getId());

        if (op.isUpdate() && op.isFromReplication()) {
            ReplicationTestServiceState body = op.getBody(ReplicationTestServiceState.class);
            String failureNodeId = body.failureNodeId;
            logInfo("failureNodeId is " + failureNodeId);
            if (getHost().getId().equals(body.failureNodeId)) {
                logWarning("Mysteriously failing op " + op.getId() + " on " + getHost().getId()
                        + ": " + Utils.toJsonHtml(body));
                failRequest(op, new IllegalStateException("Operation failed mysteriously"));
                return;
            }
        }

        super.handleRequest(op);
    }

    @Override
    public void handleStart(Operation startPost) {
        if (!startPost.hasBody()) {
            startPost.fail(new IllegalArgumentException("body is required"));
            return;
        }

        QueryTask queryTask = QueryTask.Builder.create()
                .setQuery(QueryTask.Query.Builder.create()
                        .addKindFieldClause(ReplicationTestServiceState.class)
                        .build())
                .build();

        queryTask.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                TimeUnit.SECONDS.toMicros(getHost().getOperationTimeoutMicros()));

        String queryTaskId = UUID.randomUUID().toString();
        queryTask.documentSelfLink = queryTaskId;

        sendRequest(Operation
                .createPost(this, QueryTaskFactoryService.SELF_LINK)
                .setBody(queryTask));

        ReplicationTestServiceState initState = startPost.getBody(
                ReplicationTestServiceState.class);
        initState.queryTaskLink = UriUtils.buildUriPath(ServiceUriPaths.CORE_QUERY_TASKS,
                queryTaskId);
        startPost.complete();

        initState.stringField = UUID.randomUUID().toString();
        initState.failureNodeId = null;

        if (hasOption(ServiceOption.STRICT_UPDATE_CHECKING)) {
            // we enforce strict update checking which means we need to get our OWN state, fill
            // signature and version,
            // then issue the patch. Otherwise DCP JavaService will bounce it in the inbound handler
            // processing.
            sendRequest(Operation.createGet(getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            logSevere(e);
                            return;
                        }

                        ReplicationTestServiceState currentState =
                                o.getBody(ReplicationTestServiceState.class);
                        initState.documentVersion = currentState.documentVersion;
                        sendRequest(Operation.createPatch(this, getSelfLink())
                                .setBody(initState));
                    }));

        } else if (!startPost.isFromReplication() && initState.documentVersion == 0) {
            sendRequest(Operation.createPatch(this, getSelfLink())
                    .setBody(initState));
        }
    }

    private String retryRequestContextId;
    public AtomicInteger retryCount = new AtomicInteger();

    @Override
    public void handleGet(Operation get) {
        if (get.getUri().getQuery() != null && !get.getRefererAsString().contains(REFERER_TOKEN)) {
            get.fail(new IllegalArgumentException("invalid referer"));
            return;
        }
        get.setBody(getState(get)).complete();
    }

    @Override
    public void handlePut(Operation put) {
        ReplicationTestServiceState body = getBody(put);
        if (body.stringField.equals(STRING_MARKER_FAIL_WITH_CONFLICT_CODE)) {
            if (this.retryRequestContextId != null
                    && this.retryRequestContextId.equals(put.getContextId())) {
                // the runtime retried the request, same context id, that is not expected
                this.retryCount.incrementAndGet();
            }
            this.retryRequestContextId = put.getContextId();

            // fail request with a status code that should induce a retry
            put.setStatusCode(Operation.STATUS_CODE_CONFLICT)
                    .fail(new IllegalStateException("failing intentionally with conflict error"));
        } else {
            super.handlePut(put);
        }
    }

    @Override
    public void handlePatch(Operation patch) {
        ReplicationTestServiceState body = getBody(patch);
        ReplicationTestServiceState state = getState(patch);

        if (body.stringField == null) {
            logWarning("invalid body in op: %s", patch.toString());
            patch.fail(new IllegalArgumentException(ERROR_MESSAGE_STRING_FIELD_IS_REQUIRED),
                    ReplicationTestServiceErrorResponse
                            .create(ERROR_MESSAGE_STRING_FIELD_IS_REQUIRED));
            return;
        }
        if (body.stringField.startsWith(ReplicationTestServiceState.CLIENT_PATCH_HINT)) {
            // direct client patch, used for replication tests, after service has converged
            state.stringField = body.stringField;
            patch.complete();
            return;
        }

        if (!body.documentSelfLink.equals(getSelfLink())) {
            patch.fail(new IllegalStateException("Selflink mismatch:" + body.documentSelfLink));
            return;
        }

        if (!body.stringField.equals(getSelfLink()) && state.stringField != null
                && state.stringField.equals(getSelfLink())) {
            patch.fail(new IllegalStateException("Out of order"));
            return;
        }

        boolean isDifferent = false;
        if (body.queryTaskLink != null) {
            state.queryTaskLink = body.queryTaskLink;
            isDifferent = true;
        }

        if (state.stringField == null || !state.stringField.equals(body.stringField)) {
            state.stringField = body.stringField;
            isDifferent = true;
        }

        if (state.failureNodeId == null || !state.failureNodeId.equals(body.failureNodeId)) {
            state.failureNodeId = body.failureNodeId;
            isDifferent = true;
        }

        if (!isDifferent) {
            patch.setStatusCode(Operation.STATUS_CODE_NOT_MODIFIED).complete();
            return;
        }

        logInfo("Saving state: " + Utils.toJsonHtml(state));

        patch.complete();

        if (state.stringField.equals(getSelfLink()) && body.queryTaskLink != null) {
            // stop sending self patches
            return;
        }

        if (hasOption(ServiceOption.STRICT_UPDATE_CHECKING)) {
            return;
        }

        if (!hasOption(ServiceOption.OWNER_SELECTION) || patch.isFromReplication()) {
            return;
        }

        // send another self patch to self
        state.stringField = getSelfLink();
        state.failureNodeId = null;
        sendRequest(Operation.createPatch(getUri()).setBody(state));
    }

    @Override
    public void handleNodeGroupMaintenance(Operation post) {
        ServiceMaintenanceRequest request = post.getBody(ServiceMaintenanceRequest.class);
        if (!request.reasons.contains(MaintenanceReason.NODE_GROUP_CHANGE)) {
            post.fail(new IllegalArgumentException("expected NODE_GROUP_CHANGE reason"));
            return;
        }
        post.complete();
    }

}