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

import java.util.Objects;
import java.util.Random;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceMaintenanceRequest;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.MinimalTestServiceState;

public class MinimalTestService extends StatefulService {

    public static final String STRING_MARKER_TIMEOUT_REQUEST = "do not complete this request";
    public static final String STRING_MARKER_HAS_CONTEXT_ID = "check context id";
    public static final String STRING_MARKER_USE_DIFFERENT_CONTENT_TYPE = "change content type on response";
    public static final String STRING_MARKER_DELAY_COMPLETION = "do a tight loop";
    public static final String TEST_HEADER_NAME = "TestServiceHeader";

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

    public MinimalTestService() {
        super(MinimalTestServiceState.class);
    }

    private static String createPlainTextResponse() {
        Random r = new Random();
        byte[] b = new byte[1500];
        r.nextBytes(b);
        return java.util.Base64.getMimeEncoder().encodeToString(b).intern();
    }

    @Override
    public void handleStart(Operation post) {
        if (post.hasBody()) {
            MinimalTestServiceState s = post.getBody(MinimalTestServiceState.class);
            if (s.id == null) {
                post.fail(new IllegalArgumentException(ERROR_MESSAGE_ID_IS_REQUIRED),
                        MinimalTestServiceErrorResponse.create(ERROR_MESSAGE_ID_IS_REQUIRED));
                return;
            }

            if (s.id.equals(STRING_MARKER_TIMEOUT_REQUEST)) {
                // we want to induce a timeout, so do NOT complete
                return;
            }
        }
        post.complete();
    }

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

        if (patchBody.id.equals(STRING_MARKER_DELAY_COMPLETION)) {
            try {
                Thread.sleep(25);
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
        MinimalTestServiceState state = getState(get);
        get.setBody(state).complete();
    }

    @Override
    public void handlePut(Operation put) {
        if (Operation.MEDIA_TYPE_APPLICATION_OCTET_STREAM.equals(put.getContentType())) {
            // just echo back binary bodies, do not update state
            put.complete();
            return;
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

    @Override
    public void handleOptions(Operation options) {
        options.complete();
    }

    private void addResponseHeader(Operation op) {
        // if the test client has added a request header, add a response header before completion
        if (op.getRequestHeader(TEST_HEADER_NAME) != null) {
            op.addResponseHeader(TEST_HEADER_NAME,
                    op.getRequestHeader(TEST_HEADER_NAME).replace("request", "response"));
        }
    }

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
