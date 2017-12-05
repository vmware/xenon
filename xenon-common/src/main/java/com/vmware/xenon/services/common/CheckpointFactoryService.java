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
import com.vmware.xenon.common.UriUtils;

public class CheckpointFactoryService extends FactoryService {
    public static final String SELF_LINK = ServiceUriPaths.CHECKPOINTS;

    public CheckpointFactoryService() {
        super(CheckpointService.CheckpointState.class);
    }

    // do not start synchronization task for checkpoint service
    @Override
    public void handleStart(Operation startPost) {
        toggleOption(ServiceOption.PERSISTENCE, true);
        setAvailable(true);
        startPost.complete();
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new CheckpointService();
    }

    /**
     * build checkpoint selflink based on associated factory link
     * @param document
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected String buildDefaultChildSelfLink(ServiceDocument document)
            throws IllegalArgumentException {
        CheckpointService.CheckpointState state = (CheckpointService.CheckpointState) document;
        if (state == null || state.factoryLink.isEmpty()) {
            throw new IllegalArgumentException("factoryLink is required");
        }
        return createSelfLinkFromState(state);
    }

    private String createSelfLinkFromState(CheckpointService.CheckpointState state) {
        return UriUtils.buildUriPath(SELF_LINK, UriUtils.convertPathCharsFromLink(state.factoryLink));
    }
}