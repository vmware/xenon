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

import java.util.EnumSet;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Used to store white-listed URI Paths and allowed
 * actions for a {@link GatewayService}
 */
public class GatewayPathService extends StatefulService {

    public static final String FACTORY_LINK = GatewayUriPaths.PATHS;

    public static FactoryService createFactory() {
        return FactoryService.create(GatewayPathService.class);
    }

    public static class State extends ServiceDocument {
        public static final String KIND = Utils.buildKind(State.class);

        /**
         * URI path of the downstream service. Immutable.
         */
        public String path;

        /**
         * Set of Http Actions allowed on the
         * specified service path. Optional.
         * If not specified, all actions will be enabled.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public EnumSet<Action> actions;
    }

    public GatewayPathService() {
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
        if (state.path == null) {
            start.fail(new IllegalStateException("path is missing"));
            return null;
        }

        // A specific format for the self-link is used to guarantee
        // uniqueness of uri paths for a specific gateway.
        String selfLink = createSelfLinkFromState(state);
        if (!getSelfLink().equals(selfLink)) {
            Exception ex = new IllegalStateException(
                    "gatewayId and/or path do not match documentSelfLink");
            start.fail(ex);
            return null;
        }

        return state;
    }

    public static final String createSelfLinkFromState(State state) {
        String path = UriUtils.convertPathCharsFromLink(state.path);
        return UriUtils.buildUriPath(FACTORY_LINK, path);
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

        State body = getBody(update);
        State state = getState(update);
        if (body.path != null && !state.path.equals(body.path)) {
            update.fail(new IllegalStateException("path cannot be changed"));
            return null;
        }
        return body;
    }

    private void updateState(State currentState, State updatedState) {
        Utils.mergeWithState(getStateDescription(), currentState, updatedState);
        if (updatedState.actions.isEmpty()) {
            currentState.actions = updatedState.actions;
        }
    }
}
