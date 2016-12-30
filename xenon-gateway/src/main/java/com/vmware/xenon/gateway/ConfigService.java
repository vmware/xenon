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

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Used to store configuration state for a {@link GatewayService}
 */
public class ConfigService extends StatefulService {

    public static final String FACTORY_LINK = GatewayUriPaths.CONFIGS;
    public static final String GATEWAY_ID_FIELD_NAME = "gatewayId";

    public static FactoryService createFactory() {
        return FactoryService.create(ConfigService.class);
    }

    public static class State extends ServiceDocument {
        public static final String KIND = Utils.buildKind(State.class);

        /**
         * Id of the Gateway. Immutable.
         */
        public String gatewayId;

        /**
         * Status of the Gateway.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public GatewayStatus status;
    }

    public ConfigService() {
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
        if (state.gatewayId == null) {
            state.gatewayId = UriUtils.getLastPathSegment(getSelfLink());
        } else {
            String selfLink = createSelfLinkFromState(state);
            if (!getSelfLink().equals(selfLink)) {
                Exception ex = new IllegalStateException(
                        "Invalid gatewayId. Id must match documentSelfLink");
                start.fail(ex);
                return null;
            }
        }

        if (state.status == null) {
            start.fail(new IllegalStateException("status is missing"));
            return null;
        }

        return state;
    }

    private String createSelfLinkFromState(State state) {
        return FACTORY_LINK + UriUtils.URI_PATH_CHAR + state.gatewayId;
    }

    @Override
    public void handlePatch(Operation patch) {
        State body = validateUpdateState(patch);
        if (body == null) {
            return;
        }
        updateState(getState(patch), body);
        patch.complete();
    }

    @Override
    public void handlePut(Operation put) {
        State body = validateUpdateState(put);
        if (body == null) {
            return;
        }
        updateState(getState(put), body);
        put.complete();
    }

    private State validateUpdateState(Operation update) {
        if (!update.hasBody()) {
            update.fail(new IllegalStateException("Body is required"));
            return null;
        }

        State body = getBody(update);
        State state = getState(update);
        if (body.gatewayId != null && !state.gatewayId.equals(body.gatewayId)) {
            update.fail(new IllegalStateException("gatewayId cannot be changed"));
            return null;
        }

        return body;
    }

    private void updateState(State currentState, State updatedState) {
        Utils.mergeWithState(getStateDescription(), currentState, updatedState);
    }
}
