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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

/**
 * Service for persistent local checkpoint per factory
 */
public class CheckpointService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.CHECKPOINTS;

    public static final long VERSION_RETENTION_LIMIT = 5;
    public static final long VERSION_RETENTION_FLOOR = 3;

    public static class CheckpointState extends ServiceDocument {
        /**
         * checkpoint timestamp
         */
        public Long timestamp;

        /**
         * link to factory
         */
        public String factoryLink;
    }

    public CheckpointService() {
        super(CheckpointState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        if (validateState(startPost) == null) {
            return;
        }
        startPost.complete();
    }

    @Override
    public void handlePut(Operation put) {
        // Fail the request if this was not a POST converted to PUT.
        if (!put.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT)) {
            Operation.failActionNotSupported(put);
            return;
        }
        CheckpointState newState = validateState(put);
        if (newState == null) {
            return;
        }
        CheckpointState currentState = getState(put);
        boolean update = updateState(currentState, newState);
        if (!update) {
            put.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_STATE_NOT_MODIFIED);
            put.complete();
            return;
        }
        put.complete();
    }

    private CheckpointState validateState(Operation op) {
        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("initial state is required"));
            return null;
        }
        CheckpointState body = op.getBody(CheckpointState.class);
        if (body.timestamp == null) {
            op.fail(new IllegalArgumentException("timestamp is required"));
            return null;
        }
        return body;
    }

    private boolean updateState(CheckpointState currentState, CheckpointState newState) {
        if (!(newState.timestamp > currentState.timestamp)) {
            return false;
        }
        currentState.timestamp = newState.timestamp;
        return true;
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument td = super.getDocumentTemplate();
        td.documentDescription.versionRetentionFloor = VERSION_RETENTION_FLOOR;
        td.documentDescription.versionRetentionLimit = VERSION_RETENTION_LIMIT;
        return td;
    }
}