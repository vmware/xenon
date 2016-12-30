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

package com.vmware.xenon.gateway;

import java.net.URI;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;

/**
 * Used to store configuration state for a {@link GatewayService}
 */
public class GatewayConfigService extends StatefulService {

    public static final String FACTORY_LINK = GatewayUriPaths.CONFIGS;

    public static FactoryService createFactory() {
        return FactoryService.create(GatewayConfigService.class);
    }

    public static class State extends ServiceDocument {
        public static final String KIND = Utils.buildKind(State.class);

        /**
         * Status of the Gateway.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public GatewayStatus status;

        /**
         * URI reference of the endpoint where all requests
         * will get forwarded to by the Gateway.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public URI forwardingUri;

        /**
         * A flag used to turn on/off request filtering through URI paths.
         * When turned-off the Gateway service just forwards message to
         * the configured forwardingURI.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Boolean filterRequests = true;
    }

    public GatewayConfigService() {
        super(State.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
    }

    @Override
    public void handleStart(Operation start) {
        State state = validateStartState(start);
        if (state == null) {
            return;
        }

        start.setBody(state);
        start.complete();
    }

    private State validateStartState(Operation start) {
        if (!start.hasBody()) {
            start.fail(new IllegalStateException("Body is required"));
            return null;
        }

        State state = getBody(start);
        if (state.status == null) {
            start.fail(new IllegalStateException("status is missing"));
            return null;
        }

        return state;
    }

    @Override
    public void handlePatch(Operation patch) {
        State body = validateUpdateState(patch);
        if (body == null) {
            return;
        }
        State currentState = getState(patch);
        updateState(currentState, body);

        patch.setBody(currentState);
        patch.complete();
    }

    @Override
    public void handlePut(Operation put) {
        State body = validateUpdateState(put);
        if (body == null) {
            return;
        }

        State currentState = getState(put);
        updateState(currentState, body);

        setState(put, currentState);
        put.complete();
    }

    private State validateUpdateState(Operation update) {
        if (!update.hasBody()) {
            update.fail(new IllegalStateException("Body is required"));
            return null;
        }
        return getBody(update);
    }

    private void updateState(State currentState, State updatedState) {
        Utils.mergeWithState(getStateDescription(), currentState, updatedState);
    }
}
