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

    public static class CheckpointState extends ServiceDocument {

        public static final long VERSION_RETENTION_LIMIT = 10;
        public static final long VERSION_RETENTION_FLOOR = 3;

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
        if (!startPost.hasBody()) {
            startPost.fail(new IllegalArgumentException("initial state is required"));
            return;
        }
        CheckpointState body = startPost.getBody(CheckpointState.class);
        if (body.timestamp == null) {
            startPost.fail(new IllegalArgumentException("checkpoint is required"));
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

        CheckpointState currentState = getState(put);
        CheckpointState newState = getBody(put);

        if (newState.timestamp == null) {
            put.fail(new IllegalArgumentException("checkpoint is required"));
            return;
        }

        if (newState.timestamp <= currentState.timestamp) {
            put.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_STATE_NOT_MODIFIED);
        } else {
            setState(put, newState);
        }
        put.complete();
    }
}
