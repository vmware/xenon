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

package com.vmware.dcp.services.samples;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.StatefulService;

/**
 * Sample service
 */
public class SampleServiceWithCustomUi extends StatefulService {

    public static class SampleServiceWithCustomUiState extends ServiceDocument {
        public String name;
    }

    public SampleServiceWithCustomUi() {
        super(SampleServiceWithCustomUiState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
        super.toggleOption(ServiceOption.HTML_USER_INTERFACE, true);
    }

    @Override
    public void handlePut(Operation put) {
        SampleServiceWithCustomUiState newState = put.getBody(SampleServiceWithCustomUiState.class);
        super.setState(put, newState);
        put.complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        SampleServiceWithCustomUiState currentState = getState(patch);
        SampleServiceWithCustomUiState body = patch.getBody(SampleServiceWithCustomUiState.class);
        currentState.name = body.name;
        patch.complete();
    }
}
