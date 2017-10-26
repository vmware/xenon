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
 * Service for persistent local checkpoint
 */
public class CheckPointService extends StatefulService {

    public static class CheckPointState extends ServiceDocument {

        public static final long VERSION_RETENTION_LIMIT = 10;
        public static final long VERSION_RETENTION_FLOOR = 3;
        /**
         * check point for synchronization task
         */
        Long checkPoint;
    }

    public CheckPointService() {
        super(CheckPointState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        CheckPointState initState = getState(startPost);
        // restart
        if (initState != null) {
            startPost.setBody(initState)
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                    .complete();
            return;
        }
        // initial start
        if (!startPost.hasBody()) {
            startPost.fail(new IllegalArgumentException("initial state is required"));
            return;
        }
        CheckPointState body = startPost.getBody(CheckPointState.class);
        if (body.checkPoint == null) {
            startPost.fail(new IllegalArgumentException("checkpoint is required"));
            return;
        }
        startPost.complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        if (!patch.hasBody()) {
            patch.fail(new IllegalArgumentException("initial state is required"));
            return;
        }
        CheckPointState newState = patch.getBody(CheckPointState.class);
        if (newState.checkPoint == null) {
            patch.fail(new IllegalArgumentException("checkpoint is required"));
            return;
        }
        CheckPointState currentState = getState(patch);
        boolean update = updateState(currentState, newState);
        if (!update) {
            patch.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_STATE_NOT_MODIFIED);
            patch.complete();
            return;
        }
        patch.complete();
    }

    private boolean updateState(CheckPointState currentState, CheckPointState newState) {
        if (!(newState.checkPoint > currentState.checkPoint)) {
            return false;
        }
        currentState.checkPoint = newState.checkPoint;
        return true;
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument template = super.getDocumentTemplate();
        template.documentDescription.versionRetentionLimit = CheckPointState.VERSION_RETENTION_LIMIT;
        template.documentDescription.versionRetentionFloor = CheckPointState.VERSION_RETENTION_FLOOR;
        return template;
    }
}