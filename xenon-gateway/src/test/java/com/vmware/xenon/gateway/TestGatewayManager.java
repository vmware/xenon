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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;

/**
 * Helper class used to add, update, delete configuration for a
 * Gateway service and verify the gateway service state.
 */
public class TestGatewayManager {

    private VerificationHost gatewayHost;

    private GatewayConfigService.State configState;
    private Map<String, GatewayPathService.State> paths = new ConcurrentSkipListMap<>();

    public TestGatewayManager(VerificationHost gatewayHost) {
        this.gatewayHost = gatewayHost;
    }

    public void addConfig(GatewayConfigService.State state) {
        TestContext ctx = this.gatewayHost.testCreate(1);
        Operation.createPost(this.gatewayHost, GatewayConfigService.FACTORY_LINK)
                .setBody(state)
                .setReferer(this.gatewayHost.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    this.configState = o.getBody(GatewayConfigService.State.class);
                    ctx.completeIteration();
                })
                .sendWith(this.gatewayHost);
        ctx.await();
    }

    public Set<String> addPaths(String pathTemplate, int count, EnumSet<Action> actions) {
        Set<String> returnVal = new HashSet<>(count);
        TestContext ctx = this.gatewayHost.testCreate(count);
        for (int i = 0; i < count; i++) {
            GatewayPathService.State state = new GatewayPathService.State();
            state.path = String.format(pathTemplate, i);
            state.actions = actions;
            state.documentSelfLink = GatewayPathService.createSelfLinkFromState(state);
            Operation.createPost(this.gatewayHost, GatewayPathService.FACTORY_LINK)
                    .setBody(state)
                    .setReferer(this.gatewayHost.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        GatewayPathService.State rsp = o.getBody(GatewayPathService.State.class);
                        this.paths.put(rsp.documentSelfLink, rsp);
                        ctx.completeIteration();
                    })
                    .sendWith(this.gatewayHost);
            returnVal.add(state.documentSelfLink);
        }
        ctx.await();
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
        TestContext ctx = this.gatewayHost.testCreate(1);
        Operation.createPatch(this.gatewayHost, this.configState.documentSelfLink)
                .setBody(patchState)
                .setReferer(this.gatewayHost.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    this.configState = o.getBody(GatewayConfigService.State.class);
                    ctx.completeIteration();
                })
                .sendWith(this.gatewayHost);
        ctx.await();
    }

    public void updatePaths(Set<String> pathLinks, EnumSet<Action> actions) {
        TestContext ctx = this.gatewayHost.testCreate(pathLinks.size());
        for (String pathLink : pathLinks) {
            GatewayPathService.State state = this.paths.get(pathLink);
            if (state == null) {
                throw new IllegalArgumentException("Unknown path");
            }
            state.actions = actions;
            Operation.createPut(this.gatewayHost, pathLink)
                    .setBody(state)
                    .setReferer(this.gatewayHost.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        GatewayPathService.State rsp = o.getBody(GatewayPathService.State.class);
                        this.paths.put(pathLink, rsp);
                        ctx.completeIteration();
                    })
                    .sendWith(this.gatewayHost);
        }
        ctx.await();
    }

    public void deleteConfig() {
        TestContext ctx = this.gatewayHost.testCreate(1);
        Operation.createDelete(this.gatewayHost, this.configState.documentSelfLink)
                .setReferer(this.gatewayHost.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    this.configState = null;
                    ctx.completeIteration();
                })
                .sendWith(this.gatewayHost);
        ctx.await();
    }

    public void deletePaths(Set<String> pathLinks) {
        if (pathLinks == null) {
            pathLinks = this.paths.keySet();
        }
        TestContext ctx = this.gatewayHost.testCreate(pathLinks.size());
        for (String pathLink : pathLinks) {
            Operation.createDelete(this.gatewayHost, pathLink)
                    .setReferer(this.gatewayHost.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        this.paths.remove(pathLink);
                        ctx.completeIteration();
                    })
                    .sendWith(this.gatewayHost);
        }
        ctx.await();
    }

    public void verifyGatewayState() {
        verifyGatewayState(this.gatewayHost);
    }

    public void verifyGatewayStateAcrossPeers() {
        verifyGatewayState();
        for (VerificationHost peerHost : this.gatewayHost.getInProcessHostMap().values()) {
            verifyGatewayState(peerHost);
        }
    }

    private void verifyGatewayState(VerificationHost host) {
        this.gatewayHost.waitFor("Gateway cache was not updated", () -> {
            boolean[] passed = new boolean[]{false};
            TestContext ctx = this.gatewayHost.testCreate(1);
            Operation.createGet(host.getUri())
                    .setReferer(this.gatewayHost.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        GatewayCache.CachedState cache = o.getBody(GatewayCache.CachedState.class);
                        if ((this.configState == null && cache.status != GatewayStatus.UNAVAILABLE) ||
                                (this.configState != null && cache.status != this.configState.status)) {
                            ctx.completeIteration();
                            return;
                        }
                        if (this.configState != null) {
                            if ((this.configState.forwardingUri == null && this.configState.forwardingUri != cache.forwardingUri) ||
                                    (this.configState.forwardingUri != null && !this.configState.forwardingUri.equals(cache.forwardingUri)) ||
                                    (this.configState.filterRequests == null && !cache.filterRequests) ||
                                    (this.configState.filterRequests != null && cache.filterRequests != this.configState.filterRequests)) {
                                ctx.completeIteration();
                                return;
                            }
                        }
                        if (cache.paths.size() != this.paths.size()) {
                            ctx.completeIteration();
                            return;
                        }
                        for (GatewayPathService.State pathState : this.paths.values()) {
                            Set<Action> actions = cache.paths.get(pathState.path);
                            if (actions == null) {
                                ctx.completeIteration();
                                return;
                            }
                            EnumSet<Action> srcActions = pathState.actions;
                            if (srcActions == null || srcActions.isEmpty()) {
                                srcActions = EnumSet.allOf(Action.class);
                            }
                            if (actions.size() != srcActions.size()) {
                                ctx.completeIteration();
                                return;
                            }
                            for (Action verb : actions) {
                                if (!srcActions.contains(verb)) {
                                    ctx.completeIteration();
                                    return;
                                }
                            }
                        }
                        passed[0] = true;
                        ctx.completeIteration();
                    })
                    .sendWith(host);
            ctx.await();
            return passed[0];
        });
    }
}
