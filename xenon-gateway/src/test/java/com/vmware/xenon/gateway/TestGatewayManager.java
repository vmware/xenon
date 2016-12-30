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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;

/**
 * Helper class used to add, update, delete configuration for a
 * Gateway service and verify the gateway service state.
 */
public class TestGatewayManager {

    private String gatewayId;
    private String gatewayPrefix;
    private VerificationHost gatewayHost;

    private ConfigService.State configState;
    private Map<String, PathService.State> paths = new ConcurrentSkipListMap<>();
    private Map<String, NodeService.State> nodes = new ConcurrentSkipListMap<>();

    public TestGatewayManager(VerificationHost gatewayHost, String gatewayId, String gatewayPrefix) {
        this.gatewayHost = gatewayHost;
        this.gatewayId = gatewayId;
        this.gatewayPrefix = gatewayPrefix;
    }

    public void addConfig(GatewayStatus status) {
        ConfigService.State state = new ConfigService.State();
        state.gatewayId = this.gatewayId;
        state.status = status;
        state.documentSelfLink = this.gatewayId;

        TestContext ctx = this.gatewayHost.testCreate(1);
        Operation.createPost(this.gatewayHost, ConfigService.FACTORY_LINK)
                .setBody(state)
                .setReferer(this.gatewayHost.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    this.configState = o.getBody(ConfigService.State.class);
                    ctx.completeIteration();
                })
                .sendWith(this.gatewayHost);
        ctx.await();
    }

    public Set<String> addPaths(int count, String pathTemplate,
                                  boolean allowAll, Set<Action> verbs) {
        Set<String> returnVal = new HashSet<>(count);
        TestContext ctx = this.gatewayHost.testCreate(count);
        for (int i = 0; i < count; i++) {
            PathService.State state = new PathService.State();
            state.gatewayId = this.gatewayId;
            state.path = String.format(pathTemplate, i);
            state.allowAllVerbs = allowAll;
            state.verbs = verbs;
            state.documentSelfLink = PathService.createSelfLinkFromState(state);
            Operation.createPost(this.gatewayHost, PathService.FACTORY_LINK)
                    .setBody(state)
                    .setReferer(this.gatewayHost.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        PathService.State rsp = o.getBody(PathService.State.class);
                        this.paths.put(rsp.documentSelfLink, rsp);
                        ctx.completeIteration();
                    })
                    .sendWith(this.gatewayHost);
            returnVal.add(state.documentSelfLink);
        }
        ctx.await();
        return returnVal;
    }

    public Set<String> addNodes(int count, String host, int portStart,
                                  NodeStatus status, String healthCheckPath) {
        Set<String> returnVal = new HashSet<>(count);
        TestContext ctx = this.gatewayHost.testCreate(count);
        for (int i = 0; i < count; i++) {
            NodeService.State state = new NodeService.State();
            state.gatewayId = this.gatewayId;
            state.status = status;
            state.healthCheckPath = healthCheckPath;
            state.reference = UriUtils.buildUri(host, portStart + i, null, null);
            state.documentSelfLink = NodeService.createSelfLinkFromState(state);
            Operation.createPost(this.gatewayHost, NodeService.FACTORY_LINK)
                    .setBody(state)
                    .setReferer(this.gatewayHost.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        NodeService.State rsp = o.getBody(NodeService.State.class);
                        this.nodes.put(rsp.documentSelfLink, rsp);
                        ctx.completeIteration();
                    })
                    .sendWith(this.gatewayHost);
            returnVal.add(state.documentSelfLink);
        }
        ctx.await();
        return returnVal;
    }

    public void updateConfig(GatewayStatus status) {
        ConfigService.State state = new ConfigService.State();
        state.gatewayId = this.gatewayId;
        state.status = status;
        TestContext ctx = this.gatewayHost.testCreate(1);
        Operation.createPatch(this.gatewayHost, this.configState.documentSelfLink)
                .setBody(state)
                .setReferer(this.gatewayHost.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    this.configState = o.getBody(ConfigService.State.class);
                    ctx.completeIteration();
                })
                .sendWith(this.gatewayHost);
        ctx.await();
    }

    public void updatePaths(Set<String> pathLinks, boolean allowAll, Set<Action> allowedVerbs) {
        TestContext ctx = this.gatewayHost.testCreate(pathLinks.size());
        for (String pathLink : pathLinks) {
            PathService.State state = this.paths.get(pathLink);
            if (state == null) {
                throw new IllegalArgumentException("Unknown path");
            }
            state.allowAllVerbs = allowAll;
            state.verbs = allowedVerbs;
            Operation.createPut(this.gatewayHost, pathLink)
                    .setBody(state)
                    .setReferer(this.gatewayHost.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        PathService.State rsp = o.getBody(PathService.State.class);
                        this.paths.put(pathLink, rsp);
                        ctx.completeIteration();
                    })
                    .sendWith(this.gatewayHost);
        }
        ctx.await();
    }

    public void updateNodes(Set<String> nodeLinks, NodeStatus status, String healthCheckPath) {
        TestContext ctx = this.gatewayHost.testCreate(nodeLinks.size());
        for (String nodeLink : nodeLinks) {
            NodeService.State state = this.nodes.get(nodeLink);
            if (state == null) {
                throw new IllegalArgumentException("Unknown path");
            }
            state.status = status;
            state.healthCheckPath = healthCheckPath;
            Operation.createPut(this.gatewayHost, nodeLink)
                    .setBody(state)
                    .setReferer(this.gatewayHost.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        NodeService.State rsp = o.getBody(NodeService.State.class);
                        this.nodes.put(nodeLink, rsp);
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

    public void deleteNodes(Set<String> nodeLinks) {
        if (nodeLinks == null) {
            nodeLinks = this.nodes.keySet();
        }
        TestContext ctx = this.gatewayHost.testCreate(nodeLinks.size());
        for (String nodeLink : nodeLinks) {
            Operation.createDelete(this.gatewayHost, nodeLink)
                    .setReferer(this.gatewayHost.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        this.nodes.remove(nodeLink);
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
            boolean[] passed = new boolean[]{ false };
            TestContext ctx = this.gatewayHost.testCreate(1);
            Operation.createGet(host, this.gatewayPrefix)
                    .setReferer(this.gatewayHost.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        GatewayCache.State cache = o.getBody(GatewayCache.State.class);
                        if ((this.configState == null && cache.status != GatewayStatus.UNAVAILABLE) ||
                                (this.configState != null && cache.status != this.configState.status)) {
                            ctx.completeIteration();
                            return;
                        }
                        if (cache.nodes.size() != this.nodes.size() || cache.paths.size() != this.paths.size()) {
                            ctx.completeIteration();
                            return;
                        }
                        for (PathService.State pathState : this.paths.values()) {
                            Set<Action> verbs = cache.paths.get(pathState.path);
                            if (verbs == null) {
                                ctx.completeIteration();
                                return;
                            }
                            Set<Action> verbsSource = pathState.verbs;
                            if (pathState.allowAllVerbs) {
                                verbsSource = new HashSet<>(Action.values().length);
                                for (Action a : Action.values()) {
                                    verbsSource.add(a);
                                }
                            }
                            if (verbs.size() != verbsSource.size()) {
                                ctx.completeIteration();
                                return;
                            }
                            for (Action verb : verbs) {
                                if (!verbsSource.contains(verb)) {
                                    ctx.completeIteration();
                                    return;
                                }
                            }
                        }
                        for (NodeService.State nodeState : this.nodes.values()) {
                            NodeStatus status = cache.nodes.get(nodeState.reference);
                            if (status == null || status != nodeState.status) {
                                ctx.completeIteration();
                                return;
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
