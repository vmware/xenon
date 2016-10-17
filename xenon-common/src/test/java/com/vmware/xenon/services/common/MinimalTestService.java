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

import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceMaintenanceRequest;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.MinimalTestServiceState;

public class MinimalTestService extends StatefulService {

    public static final AtomicInteger HANDLE_START_COUNT = new AtomicInteger();

    public static final String CUSTOM_CONTENT_TYPE = "application/vnd.vmware.horizon.manager.error+json;charset=UTF-8";

    public static final String STRING_MARKER_FAIL_REQUEST = "fail request";
    public static final String STRING_MARKER_RETRY_REQUEST = "fail request with error that causes retry";
    public static final String STRING_MARKER_TIMEOUT_REQUEST = "do not complete this request";
    public static final String STRING_MARKER_HAS_CONTEXT_ID = "check context id";
    public static final String STRING_MARKER_USE_DIFFERENT_CONTENT_TYPE = "change content type on response";
    public static final String STRING_MARKER_DELAY_COMPLETION = "do a tight loop";
    public static final String STRING_MARKER_FAIL_WITH_PLAIN_TEXT_RESPONSE = "fail with plain text content type";
    public static final String STRING_MARKER_FAIL_WITH_CUSTOM_CONTENT_TYPE_RESPONSE = "fail with "
            + CUSTOM_CONTENT_TYPE;

    public static final String TEST_HEADER_NAME = "TestServiceHeader";
    public static final String QUERY_HEADERS = "headers";
    public static final String QUERY_DELAY_COMPLETION = "delay";

    public static final String PLAIN_TEXT_RESPONSE = createPlainTextResponse();
    public static final String ERROR_MESSAGE_ID_IS_REQUIRED = "id is required";

    public static final String STAT_NAME_MAINTENANCE_SUCCESS_COUNT = "maintSuccessCount";
    public static final String STAT_NAME_MAINTENANCE_FAILURE_COUNT = "maintFailureCount";

    public static class MinimalTestServiceErrorResponse extends ServiceErrorResponse {

        public static final String KIND = Utils.buildKind(MinimalTestServiceErrorResponse.class);

        public static MinimalTestServiceErrorResponse create(String message) {
            MinimalTestServiceErrorResponse er = new MinimalTestServiceErrorResponse();
            er.message = message;
            er.documentKind = KIND;
            er.customErrorField = Math.PI;
            return er;
        }

        public double customErrorField;
    }

    public boolean delayMaintenance;

    public boolean isStateModifiedPostCompletion;

    public MinimalTestService() {
        super(MinimalTestServiceState.class);
    }

    private static String createPlainTextResponse() {
        Random r = new Random();
        byte[] b = new byte[1500];
        r.nextBytes(b);
        return java.util.Base64.getMimeEncoder().encodeToString(b);
    }

    @Override
    public void handleCreate(Operation post) {
        if (!ServiceHost.isServiceCreate(post)) {
            post.fail(new IllegalStateException("not marked as create"));
        } else {
            post.complete();
        }
    }

    @Override
    public void handleStart(Operation post) {
        HANDLE_START_COUNT.incrementAndGet();

        MinimalTestServiceState s = null;
        if (post.hasBody()) {
            s = post.getBody(MinimalTestServiceState.class);
            if (s.id == null) {
                post.fail(new IllegalArgumentException(ERROR_MESSAGE_ID_IS_REQUIRED),
                        MinimalTestServiceErrorResponse.create(ERROR_MESSAGE_ID_IS_REQUIRED));
                return;
            }

            if (s.id.equals(STRING_MARKER_TIMEOUT_REQUEST)) {
                // we want to induce a timeout, so do NOT complete
                return;
            }
            // these fields are set by the parent factory and the runtime before indexing. We set them here
            // eagerly so the state modification test below passes
            s.documentSelfLink = getSelfLink();
            s.documentKind = Utils.buildKind(s.getClass());
        }
        post.complete();

        if (!post.hasBody()) {
            return;
        }
        MinimalTestServiceState initialState = (MinimalTestServiceState) post.getBodyRaw();
        // verify state has not been modified post completion
        if (initialState.documentSelfLink == null || initialState.id == null) {
            this.isStateModifiedPostCompletion = true;
        }
    }

    public boolean gotDeleted;

    public boolean gotStopped;

    @Override
    public void handleStop(Operation delete) {
        this.gotStopped = true;
        delete.complete();
    }

    @Override
    public void handleDelete(Operation delete) {
        if (delete.hasBody()) {
            MinimalTestServiceState s = delete.getBody(MinimalTestServiceState.class);
            if (STRING_MARKER_FAIL_REQUEST.equals(s.id)) {
                delete.fail(new IllegalStateException("failing intentionally"));
                return;
            }
        }
        this.gotDeleted = true;
        delete.complete();
    }

    private String retryRequestContextId;

    @Override
    public void handlePatch(Operation patch) {
        MinimalTestServiceState patchBody = patch
                .getBody(MinimalTestServiceState.class);
        MinimalTestServiceState currentState = getState(patch);

        if (patchBody.id == null) {
            patch.fail(new IllegalArgumentException(ERROR_MESSAGE_ID_IS_REQUIRED),
                    MinimalTestServiceErrorResponse.create(ERROR_MESSAGE_ID_IS_REQUIRED));
            return;
        }

        if (patchBody.id.equals(STRING_MARKER_FAIL_WITH_PLAIN_TEXT_RESPONSE)) {
            patch.setBody("test induced failure in custom text")
                    .setContentType(Operation.MEDIA_TYPE_TEXT_PLAIN)
                    .fail(Operation.STATUS_CODE_BAD_REQUEST);
            return;
        }

        if (patchBody.id.equals(STRING_MARKER_FAIL_WITH_CUSTOM_CONTENT_TYPE_RESPONSE)) {
            patch.setBody("test induced failure in custom text")
                    .setContentType(CUSTOM_CONTENT_TYPE)
                    .fail(Operation.STATUS_CODE_BAD_REQUEST);
            return;
        }

        if (patchBody.id.equals(STRING_MARKER_RETRY_REQUEST)) {
            if (this.retryRequestContextId != null) {
                if (this.retryRequestContextId.equals(patch.getContextId())) {
                    // the request was retried, and the body is correct since the ID is still set to
                    // retry marker
                    this.retryRequestContextId = null;
                    patch.complete();
                    return;
                } else {
                    patch.fail(new IllegalStateException("Expected retry context id to match"));
                    return;
                }
            }

            this.retryRequestContextId = patch.getContextId();
            // fail request with a status code that should induce a retry
            patch.setStatusCode(Operation.STATUS_CODE_BAD_REQUEST)
                    .fail(new IllegalStateException("failing intentionally"));
            return;
        }

        if (patchBody.id.equals(STRING_MARKER_DELAY_COMPLETION)) {
            try {
                if (patchBody.responseDelay != null) {
                    Thread.sleep(patchBody.responseDelay);
                } else {
                    Thread.sleep(25);
                }
            } catch (InterruptedException e) {
            }
            patch.complete();
            return;
        }

        if (patchBody.id.equals(STRING_MARKER_USE_DIFFERENT_CONTENT_TYPE)) {
            patch.setContentType(Operation.MEDIA_TYPE_APPLICATION_X_WWW_FORM_ENCODED).complete();
            return;
        }

        if (patchBody.id.equals(STRING_MARKER_TIMEOUT_REQUEST)) {
            // we want to induce a timeout, so do NOT complete the patch
            return;
        }

        if (patchBody.id.equals(STRING_MARKER_HAS_CONTEXT_ID)
                && (patch.getContextId() == null
                || !Objects.equals(patch.getContextId(), patchBody.stringValue))) {
            patch.fail(new IllegalArgumentException(
                    String.format(
                            "Context Id check failed. It was either 'null' or did not match expected value. expected[%s], got[%s]",
                            patchBody.stringValue,
                            patch.getContextId())));
            return;
        }

        int statusCode = Operation.STATUS_CODE_OK;

        if (currentState != null) {
            if (patchBody.id.equals(currentState.id)) {
                statusCode = Operation.STATUS_CODE_NOT_MODIFIED;
            } else {
                currentState.id = patchBody.id;
            }
            if (patchBody.documentExpirationTimeMicros > 0) {
                currentState.documentExpirationTimeMicros = patchBody.documentExpirationTimeMicros;
            }
        } else {
            setState(patch, patchBody);
        }
        addResponseHeader(patch);
        patch.setStatusCode(statusCode).complete();
    }

    @Override
    public void handleGet(Operation get) {
        // for test purposes only check if the client wants us to return JSON
        // or plain text
        String acceptHeader = get.getRequestHeader("Accept");
        if (acceptHeader != null
                && acceptHeader.equals(Operation.MEDIA_TYPE_TEXT_PLAIN)) {
            get.setBodyNoCloning(PLAIN_TEXT_RESPONSE).complete();
            return;
        }

        Map<String, String> params = UriUtils.parseUriQueryParams(get.getUri());
        if (params.containsKey(QUERY_HEADERS)) {
            respondWithHeaders(get);
            return;
        }


        final MinimalTestServiceState state = getState(get);
        if (params.containsKey(MinimalTestService.QUERY_DELAY_COMPLETION)) {
            long delay = Integer.parseInt(params.get(MinimalTestService.QUERY_DELAY_COMPLETION));
            getHost().schedule(
                    () -> {
                        get.setBody(state).complete();
                    } ,
                    delay, TimeUnit.SECONDS);
            return;
        }

        get.setBody(state).complete();
    }

    /**
    * If we receive a get with the "headers" query, we serialize the incoming
    * headers and return this. This allows us to test that the right headers
    * were sent.
    */
    private void respondWithHeaders(Operation get) {
        StringBuilder sb = new StringBuilder();
        Map<String, String> headers = get.getRequestHeaders();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            sb.append(header.getKey());
            sb.append(":");
            sb.append(header.getValue());
            sb.append("\n");
        }
        MinimalTestServiceState state = new MinimalTestServiceState();
        state.stringValue = sb.toString();
        get.setBody(state).complete();
    }

    @Override
    public void handlePut(Operation put) {
        if (Operation.MEDIA_TYPE_APPLICATION_OCTET_STREAM.equals(put.getContentType())) {
            // just echo back binary bodies, do not update state
            put.complete();
            return;
        }

        if (put.getRequestHeader(TEST_HEADER_NAME) == null) {
            if (put.getUri().getQuery() != null) {
                put.fail(new IllegalArgumentException("Query not expected"));
                return;
            }
        }

        MinimalTestServiceState replacementState = put.getBody(MinimalTestServiceState.class);

        if (replacementState.id == null) {
            put.fail(new IllegalArgumentException(ERROR_MESSAGE_ID_IS_REQUIRED),
                    MinimalTestServiceErrorResponse.create(ERROR_MESSAGE_ID_IS_REQUIRED));
            return;
        }

        replacementState.documentOwner = getHost().getId();
        addResponseHeader(put);
        // PUT replaces entire state, so update the linked state
        setState(put, replacementState);
        put.setBody(replacementState).complete();
    }

    private void addResponseHeader(Operation op) {
        // if the test client has added a request header, add a response header before completion
        if (op.getRequestHeader(TEST_HEADER_NAME) != null) {
            op.addResponseHeader(TEST_HEADER_NAME,
                    op.getRequestHeader(TEST_HEADER_NAME).replace("request", "response"));
        }
    }

    @Override
    public void handleMaintenance(Operation op) {
        if (this.delayMaintenance) {
            try {
                logInfo("delaying maintenance on purpose");
                Thread.sleep((2 * getHost().getMaintenanceIntervalMicros()) / 1000);
            } catch (Exception e1) {
            }
        }

        if (!op.hasBody()) {
            adjustStat(STAT_NAME_MAINTENANCE_FAILURE_COUNT, 1);
            op.fail(new IllegalStateException("missing body in maintenance op"));
            return;
        }

        ServiceMaintenanceRequest r = op.getBody(ServiceMaintenanceRequest.class);

        if (!ServiceMaintenanceRequest.KIND.equals(r.kind)) {
            adjustStat(STAT_NAME_MAINTENANCE_FAILURE_COUNT, 1);
            op.fail(new IllegalStateException("invalid body in maintenance op"));
            return;
        }

        if (!r.reasons.contains(MaintenanceReason.PERIODIC_SCHEDULE)) {
            adjustStat(STAT_NAME_MAINTENANCE_FAILURE_COUNT, 1);
            op.fail(new IllegalStateException("invalid reason"));
            return;
        }
        adjustStat(STAT_NAME_MAINTENANCE_SUCCESS_COUNT, 1);
        op.complete();
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument template = super.getDocumentTemplate();
        // this service is a target of throughput tests so we set the limit high to avoid grooming
        // during the tests. Tests can use the example service to verify throughput while grooming
        // is active
        template.documentDescription.versionRetentionLimit = 1000 * 1000;
        return template;
    }
}
