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
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;

/**
 * Used to store backend nodes for a {@GatewayService}

 * A Node refers to a backend server where requests
 * get routed to from the gateway service.
 */
public class NodeService extends StatefulService {

    public static final String FACTORY_LINK = GatewayUriPaths.NODES;
    private static long HEALTH_CHECK_INTERVAL_MICROS = TimeUnit.SECONDS.toMicros(3);

    public static FactoryService createFactory() {
        return FactoryService.create(NodeService.class);
    }

    public static class State extends ServiceDocument {
        public static final String KIND = Utils.buildKind(State.class);

        /**
         * Id of the Gateway. Immutable.
         */
        public String gatewayId;

        /**
         * URI reference of the node registered
         * with the gateway. Immutable.
         */
        public URI reference;

        /**
         * Status of this node. Optional.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public NodeStatus status = NodeStatus.UNAVAILABLE;

        /**
         * URI path of the health check endpoint
         * available on this node. Optional.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String healthCheckPath;

        /**
         * Health Check interval in microseconds.
         * Optional, Immutable.
         */
        public Long healthCheckIntervalMicros = HEALTH_CHECK_INTERVAL_MICROS;
    }

    public NodeService() {
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

        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.setMaintenanceIntervalMicros(state.healthCheckIntervalMicros);

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
            start.fail(new IllegalStateException("gatewayId is missing"));
            return null;
        }

        if (state.reference == null) {
            start.fail(new IllegalStateException("reference is missing"));
            return null;
        }

        // A specific format for the self-link is used to guarantee
        // uniqueness of node-services for a specific gateway.
        String selfLink = createSelfLinkFromState(state);
        if (!getSelfLink().equals(selfLink)) {
            Exception ex = new IllegalStateException(
                    "gatewayId and/or reference do not match documentSelfLink");
            start.fail(ex);
            return null;
        }

        return state;
    }

    public static final String createSelfLinkFromState(State state) {
        String host = state.reference.getHost();
        int port = state.reference.getPort();
        if (port == -1) {
            // no port was defined.
            return String.format("%s/%s-%s",
                    FACTORY_LINK, state.gatewayId, host);
        }
        return String.format("%s/%s-%s-%d",
                FACTORY_LINK, state.gatewayId, host, port);
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

        if (body.reference != null && !state.reference.equals(body.reference)) {
            update.fail(new IllegalStateException("reference cannot be changed"));
            return null;
        }

        if (body.healthCheckIntervalMicros != null &&
                !state.healthCheckIntervalMicros.equals(body.healthCheckIntervalMicros)) {
            update.fail(new IllegalStateException("healthCheckIntervalMicros cannot be changed"));
            return null;
        }

        return body;
    }

    private void updateState(State currentState, State updatedState) {
        Utils.mergeWithState(getStateDescription(), currentState, updatedState);
    }

    @Override
    public void handlePeriodicMaintenance(Operation post) {
        post.complete();
        // TODO implement health-checks.
    }
}
