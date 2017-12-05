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
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;

/**
 * Service for persistent local checkpoint per factory
 */
public class CheckpointService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.CHECKPOINT;

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

    public static FactoryService createFactory() {
        FactoryService fs = new FactoryService(CheckpointState.class) {

            @Override
            public Service createServiceInstance() throws Throwable {
                return new CheckpointService();
            }

            // do not start synchronization task for checkpoint service
            @Override
            public void handleStart(Operation startPost) {
                toggleOption(ServiceOption.PERSISTENCE, true);
                setUseBodyForSelfLink(true);
                setAvailable(true);
                startPost.complete();
            }

            @Override
            protected String buildDefaultChildSelfLink(ServiceDocument s)
                    throws IllegalArgumentException {
                CheckpointState childState = (CheckpointState) s;
                if (childState == null || childState.factoryLink.isEmpty()) {
                    throw new IllegalArgumentException("factoryLink is required");
                }
                String childSelfLink = UriUtils.buildUriPath(
                        CheckpointService.FACTORY_LINK, UriUtils.convertPathCharsFromLink(childState.factoryLink));
                return childSelfLink;
            }

            /**
             * check point is locally persistent
             * @param maintOp
             */
            @Override
            public void handleNodeGroupMaintenance(Operation maintOp) {
                maintOp.complete();
            }
        };
        return fs;
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
        if (!put.hasBody()) {
            put.fail(new IllegalArgumentException("initial state is required"));
            return;
        }
        CheckpointState newState = put.getBody(CheckpointState.class);
        if (newState.timestamp == null) {
            put.fail(new IllegalArgumentException("checkpoint is required"));
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

    private boolean updateState(CheckpointState currentState, CheckpointState newState) {
        if (!(newState.timestamp > currentState.timestamp)) {
            return false;
        }
        currentState.timestamp = newState.timestamp;
        return true;
    }
}