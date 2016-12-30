/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;

/**
 * Helper class used to add, update, delete configuration for a
 * Gateway service and verify the gateway service state.
 */
public class TestGatewayManager {

    private TestRequestSender sender;
    private VerificationHost gatewayHost;

    private GatewayConfigService.State configState;
    private Map<String, GatewayPathService.State> paths = new ConcurrentSkipListMap<>();

    public TestGatewayManager(VerificationHost gatewayHost) {
        this.gatewayHost = gatewayHost;
        this.sender = new TestRequestSender(this.gatewayHost);
    }

    public void addConfig(GatewayConfigService.State state) {
        Operation op = Operation
                .createPost(this.gatewayHost, GatewayConfigService.FACTORY_LINK)
                .setBody(state);
        this.configState = this.sender.sendAndWait(op, GatewayConfigService.State.class);
    }

    public Set<String> addPaths(String pathTemplate, int count, EnumSet<Action> actions) {
        Set<String> returnVal = new HashSet<>(count);
        List<Operation> ops = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            GatewayPathService.State state = new GatewayPathService.State();
            state.path = String.format(pathTemplate, i);
            state.actions = actions;
            state.documentSelfLink = GatewayPathService.createSelfLinkFromState(state);
            ops.add(Operation
                    .createPost(this.gatewayHost, GatewayPathService.FACTORY_LINK)
                    .setBody(state));
        }
        List<GatewayPathService.State> responses = this.sender
                .sendAndWait(ops, GatewayPathService.State.class);

        responses.forEach(r -> {
            returnVal.add(r.documentSelfLink);
            this.paths.put(r.documentSelfLink, r);
        });

        return returnVal;
    }

    public void changeConfigStatus(GatewayStatus status) {
        GatewayConfigService.State state = new GatewayConfigService.State();
        state.status = status;
        patchConfig(state);
    }

    public void changeForwardingUri(URI forwardingUri) {
        GatewayConfigService.State state = new GatewayConfigService.State();
        state.forwardingUri = forwardingUri;
        patchConfig(state);
    }

    public void changeRequestFilteringStatus(boolean status) {
        GatewayConfigService.State state = new GatewayConfigService.State();
        state.filterRequests = status;
        patchConfig(state);
    }

    public void patchConfig(GatewayConfigService.State patchState) {
        Operation op = Operation
                .createPatch(this.gatewayHost, this.configState.documentSelfLink)
                .setBody(patchState);
        this.configState = this.sender
                .sendAndWait(op, GatewayConfigService.State.class);
    }

    public void updatePaths(Set<String> pathLinks, EnumSet<Action> actions) {
        List<Operation> ops = new ArrayList<>(pathLinks.size());
        for (String pathLink : pathLinks) {
            GatewayPathService.State state = this.paths.get(pathLink);
            if (state == null) {
                throw new IllegalArgumentException("Unknown path");
            }
            state.actions = actions;
            ops.add(Operation
                    .createPut(this.gatewayHost, pathLink)
                    .setBody(state));
        }
        List<GatewayPathService.State> responses = this.sender
                .sendAndWait(ops, GatewayPathService.State.class);
        responses.forEach(r -> this.paths.put(r.documentSelfLink, r));
    }

    public void deleteConfig() {
        Operation op = Operation
                .createDelete(this.gatewayHost, this.configState.documentSelfLink);
        this.sender.sendAndWait(op);
        this.configState = null;
    }

    public void deletePaths(Set<String> pathLinks) {
        if (pathLinks == null) {
            pathLinks = this.paths.keySet();
        }
        List<Operation> ops = new ArrayList<>(pathLinks.size());
        for (String pathLink : pathLinks) {
            ops.add(Operation.createDelete(this.gatewayHost, pathLink));
        }

        this.sender.sendAndWait(ops);
        pathLinks.forEach(link -> this.paths.remove(link));
    }

    public void verifyGatewayState() {
        verifyGatewayState(this.gatewayHost);
    }

    public void verifyGatewayStateAcrossPeers() {
        verifyGatewayState();
        this.gatewayHost.getInProcessHostMap().values().forEach(peer -> verifyGatewayState(peer));
    }

    private void verifyGatewayState(VerificationHost host) {
        this.gatewayHost.waitFor("Gateway cache was not updated", () -> {
            Operation rspOp = this.sender.sendAndWait(Operation.createGet(host.getUri()));
            if (rspOp.getStatusCode() != Operation.STATUS_CODE_OK) {
                return false;
            }

            GatewayCache.CachedState cache = rspOp.getBody(GatewayCache.CachedState.class);
            if ((this.configState == null && cache.status != GatewayStatus.UNAVAILABLE) ||
                    (this.configState != null && cache.status != this.configState.status)) {
                host.log(Level.INFO, "Unexpected gateway Status. Cached Status:%s, Expected:%s",
                        cache.status, this.configState != null ? this.configState.status : "Unknown");
                return false;
            }
            if (this.configState != null) {
                if ((this.configState.forwardingUri == null && this.configState.forwardingUri != cache.forwardingUri) ||
                        (this.configState.forwardingUri != null && !this.configState.forwardingUri.equals(cache.forwardingUri)) ||
                        (this.configState.filterRequests == null && !cache.filterRequests) ||
                        (this.configState.filterRequests != null && cache.filterRequests != this.configState.filterRequests)) {
                    host.log(Level.INFO, "Unexpected gateway Config");
                    return false;
                }
            }
            if (cache.paths.size() != this.paths.size()) {
                host.log(Level.INFO, "Unexpected number of paths. Cached count:%d, Expected:%d",
                        cache.paths.size(), this.paths.size());
                return false;
            }
            for (GatewayPathService.State pathState : this.paths.values()) {
                Set<Action> actions = cache.paths.get(pathState.path);
                if (actions == null) {
                    host.log(Level.INFO,
                            "Cached path state contain zero registered actions. Path %s", pathState.path);
                    return false;
                }
                EnumSet<Action> srcActions = pathState.actions;
                if (srcActions == null || srcActions.isEmpty()) {
                    srcActions = EnumSet.allOf(Action.class);
                }
                if (actions.size() != srcActions.size()) {
                    host.log(Level.INFO,
                            "Unexpected action count for path %s. Cached:%d, Expected:%d",
                            pathState.path, actions.size(), srcActions.size());
                    return false;
                }
                for (Action action : actions) {
                    if (!srcActions.contains(action)) {
                        host.log(Level.INFO,
                                "Unexpected action %s for path %s",
                                pathState.path, action);
                        return false;
                    }
                }
            }
            return true;
        });
    }
}
