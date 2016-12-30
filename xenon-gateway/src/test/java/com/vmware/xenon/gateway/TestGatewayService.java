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

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Array;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.RootNamespaceService;

public class TestGatewayService {

    private static final String GATEWAY_ID = "default";
    private static final String GATEWAY_LINK = "/";
    private static final String MINIMAL_SERVICE_LINK = "/minimal-service";

    private TestGatewayManager gatewayMgr;
    private VerificationHost gatewayHost;
    private VerificationHost backendHost;

    public int serviceCount = 10;
    public int updateCount = 10;
    public int peerCount = 3;

    @Before
    public void setUp() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        if (this.gatewayHost == null) {
            this.gatewayHost = VerificationHost.create(0);
            this.gatewayHost.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(
                    VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
            this.gatewayHost.start();
            setupGatewayHost(this.gatewayHost, GATEWAY_ID);
        }
        if (this.gatewayMgr == null) {
            this.gatewayMgr = new TestGatewayManager(
                    this.gatewayHost, GATEWAY_ID, GATEWAY_LINK);
        }
    }

    private void setupGatewayHost(VerificationHost host, String gatewayId) throws Throwable {
        // Stop ExampleService. Otherwise any requests to example-service
        // will be served by the Gateway.
        FactoryService exampleFactory = ExampleService.createFactory();
        exampleFactory.setSelfLink(ExampleService.FACTORY_LINK);
        host.stopService(exampleFactory);

        // Stop RootNamespaceService, since gateway will listen on "/"
        RootNamespaceService service2 = new RootNamespaceService();
        service2.setSelfLink(RootNamespaceService.SELF_LINK);
        host.stopService(service2);

        // Start Stateful services
        host.startFactory(PathService.class, PathService::createFactory);
        host.startFactory(NodeService.class, NodeService::createFactory);
        host.startFactory(ConfigService.class, ConfigService::createFactory);

        // Wait for the factories to become available.
        waitForReplicatedFactoryServiceAvailable(host, PathService.FACTORY_LINK);
        waitForReplicatedFactoryServiceAvailable(host, NodeService.FACTORY_LINK);
        waitForReplicatedFactoryServiceAvailable(host, ConfigService.FACTORY_LINK);

        // Start the gateway service.
        TestContext ctx = host.testCreate(1);
        Operation postOp = Operation
                .createPost(host, GATEWAY_LINK)
                .setCompletion(ctx.getCompletion());
        host.startService(postOp, new GatewayService(gatewayId));
        ctx.await();
    }

    private void setupBackendHost() throws Throwable {
        this.backendHost = VerificationHost.create(0);
        this.backendHost.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(
                VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        this.backendHost.start();

        waitForReplicatedFactoryServiceAvailable(this.backendHost, ExampleService.FACTORY_LINK);
        startMinimalTestService(this.backendHost);
    }

    private void waitForReplicatedFactoryServiceAvailable(VerificationHost host, String factoryLink) {
        URI factoryUri = UriUtils.buildUri(host, factoryLink);
        host.waitForReplicatedFactoryServiceAvailable(factoryUri);
    }

    private void startMinimalTestService(VerificationHost host) throws Throwable {
        MinimalTestService minimalService = new MinimalTestService();
        MinimalTestServiceState state = new MinimalTestServiceState();
        state.id = UUID.randomUUID().toString();
        host.startServiceAndWait(minimalService, MINIMAL_SERVICE_LINK, state);
    }

    @After
    public void tearDown() {
        this.gatewayMgr = null;
        if (this.gatewayHost != null) {
            this.gatewayHost.tearDownInProcessPeers();
            this.gatewayHost.tearDown();
            this.gatewayHost = null;
        }
        if (this.backendHost != null) {
            this.backendHost.tearDownInProcessPeers();
            this.backendHost.tearDown();
            this.backendHost = null;
        }
    }

    /**
     * This test verifies gateway routing for different
     * type of http requests to a backend node.
     */
    @Test
    public void testGatewayRouting() throws Throwable {
        setupBackendHost();

        this.gatewayMgr.addConfig(GatewayStatus.AVAILABLE);
        this.gatewayMgr.addPaths(1, ExampleService.FACTORY_LINK, true, null);
        this.gatewayMgr.addPaths(1, MINIMAL_SERVICE_LINK, true, null);
        this.gatewayMgr.addNodes(1, this.backendHost.getUri().getHost(),
                this.backendHost.getUri().getPort(), NodeStatus.AVAILABLE, null);
        this.gatewayMgr.verifyGatewayState();

        ServiceDocumentDescription desc = ServiceDocumentDescription
                .Builder.create().buildDescription(ExampleServiceState.class);

        // Verify POSTs
        List<URI> exampleUris = new ArrayList<>();
        this.gatewayHost.createExampleServices(
                this.gatewayHost, this.serviceCount, exampleUris, null, true);

        // Verify GETs
        Map<URI, ExampleServiceState> examples = this.gatewayHost.getServiceState(
                null, ExampleServiceState.class, exampleUris);

        // Verify GET with Query Params
        URI factoryUri = UriUtils.buildUri(this.gatewayHost, ExampleService.FACTORY_LINK);
        URI expandedUri = UriUtils.buildExpandLinksQueryUri(factoryUri);
        ServiceDocumentQueryResult result = this.gatewayHost.getFactoryState(expandedUri);
        assertTrue(result.documents.size() == examples.size());
        for (ExampleServiceState example : examples.values()) {
            assertTrue(result.documentLinks.contains(example.documentSelfLink));
            String jsonDocument = (String)result.documents.get(example.documentSelfLink);
            ExampleServiceState state = Utils.fromJson(jsonDocument, ExampleServiceState.class);
            assertTrue(ServiceDocument.equals(desc, example, state));
        }

        // Verify PUTs
        for (int i = 0; i < this.updateCount; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = UUID.randomUUID().toString();
            state.counter = 100L + i;
            this.gatewayHost.doServiceUpdates(examples.keySet(), Action.PUT, state);
        }

        // Verify PATCHes
        for (int i = 0; i < this.updateCount; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.counter = 100L + i;
            this.gatewayHost.doServiceUpdates(examples.keySet(), Action.PATCH, state);
        }

        // Verify DELETEs
        this.gatewayHost.deleteAllChildServices(
                UriUtils.buildUri(this.gatewayHost, ExampleService.FACTORY_LINK));

        // Verify errors.
        // 400 - BAD REQUEST
        ExampleServiceState state = new ExampleServiceState();
        state.name = null;
        ServiceErrorResponse rsp = makeRequest(Action.POST,
                ExampleService.FACTORY_LINK, state,
                ServiceErrorResponse.class, Operation.STATUS_CODE_BAD_REQUEST);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_BAD_REQUEST);

        // 404 - NOT-FOUND
        rsp = makeRequest(Action.GET,
                ExampleService.FACTORY_LINK + "/does-not-exist", null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_NOT_FOUND);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_NOT_FOUND);

        // 409 - CONFLICT
        String documentSelfLink = examples.values().iterator().next().documentSelfLink;
        state.name = "contoso";
        state.counter = 1000L;
        state.documentSelfLink = documentSelfLink;
        rsp = makeRequest(Action.POST, ExampleService.FACTORY_LINK, state,
                ServiceErrorResponse.class, Operation.STATUS_CODE_CONFLICT);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_CONFLICT);

        // Verify requests with Custom Request/Response headers
        TestContext ctx = this.gatewayHost.testCreate(1);
        MinimalTestServiceState minimalState = new MinimalTestServiceState();
        minimalState.id = UUID.randomUUID().toString();
        Operation putOp = Operation
                .createPut(this.gatewayHost, MINIMAL_SERVICE_LINK)
                .setBody(minimalState)
                .setReferer(this.gatewayHost.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    String value = o.getResponseHeader(MinimalTestService.TEST_HEADER_NAME);
                    if (value != null && value.equals("response-" + minimalState.id)) {
                        ctx.completeIteration();
                        return;
                    }
                    ctx.failIteration(new IllegalStateException("response did not contain expected header"));
                });
        putOp.addRequestHeader(MinimalTestService.TEST_HEADER_NAME, "request-" + minimalState.id);
        this.gatewayHost.send(putOp);
        ctx.await();
    }

    /**
     * This test verifies various error code paths in the
     * GatewayService, when the gateway service is expected
     * to fail incoming requests.
     */
    @Test
    public void testGatewayErrors() throws Throwable {
        // Gateway is currently UNAVAILABLE. All http requests
        // should fail with http 503.
        ServiceErrorResponse rsp = makeRequest(
                Action.GET, ExampleService.FACTORY_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_UNAVAILABLE);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_UNAVAILABLE);

        // Set the gateway state to PAUSED and retry the same request. It
        // should now fail with http 404, since path is not registered yet.
        this.gatewayMgr.addConfig(GatewayStatus.PAUSED);
        this.gatewayMgr.verifyGatewayState();
        rsp = makeRequest(
                Action.GET, ExampleService.FACTORY_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_NOT_FOUND);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_NOT_FOUND);

        // Add the path with POST verb. It should now fail with
        // http 405, since GET ver is not yet registered.
        Set<String> paths = this.gatewayMgr.addPaths(1, ExampleService.FACTORY_LINK,
                false, EnumSet.of(Action.POST));
        this.gatewayMgr.verifyGatewayState();
        rsp = makeRequest(
                Action.GET, ExampleService.FACTORY_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_BAD_METHOD);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_BAD_METHOD);

        // Add the GET verb now. The request should still fail
        // with UNAVAILABLE error code since gateway is PAUSED.
        this.gatewayMgr.updatePaths(paths, true, null);
        this.gatewayMgr.verifyGatewayState();
        rsp = makeRequest(
                Action.GET, ExampleService.FACTORY_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_UNAVAILABLE);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_UNAVAILABLE);

        // Change gateway state to AVAILABLE. The request should still
        // fail since there are no AVAILABLE nodes
        this.gatewayMgr.updateConfig(GatewayStatus.AVAILABLE);
        this.gatewayMgr.verifyGatewayState();
        rsp = makeRequest(
                Action.GET, ExampleService.FACTORY_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_UNAVAILABLE);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_UNAVAILABLE);

        // Register a backend host but mark it as UN-AVAILABLE. The
        // request should still fail.
        setupBackendHost();
        Set<String> hosts = this.gatewayMgr.addNodes(1, this.backendHost.getUri().getHost(),
                this.backendHost.getUri().getPort(), NodeStatus.UNAVAILABLE, null);
        this.gatewayMgr.verifyGatewayState();
        rsp = makeRequest(
                Action.GET, ExampleService.FACTORY_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_UNAVAILABLE);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_UNAVAILABLE);

        // Now update the node's status to AVAILABLE and retry the
        // request. This time it should succeed!
        this.gatewayMgr.updateNodes(hosts, NodeStatus.AVAILABLE, null);
        this.gatewayMgr.verifyGatewayState();
        ExampleServiceState state = new ExampleServiceState();
        state.name = "testing";
        ExampleServiceState result = makeRequest(
                Action.POST, ExampleService.FACTORY_LINK, state,
                ExampleServiceState.class, Operation.STATUS_CODE_OK);
        assertTrue(result.name.equals(state.name));
    }

    /**
     * This test verifies that the gateway cached state reflects any
     * configuration changes made.
     */
    @Test
    public void testGatewayConfigChanges() throws Throwable {
        // Add config.
        this.gatewayMgr.addConfig(GatewayStatus.AVAILABLE);
        Set<String> pathsA = this.gatewayMgr.addPaths(
                10, "/core/factoryA-%s", false, EnumSet.of(Action.POST));
        Set<String> pathsB = this.gatewayMgr.addPaths(
                10, "/core/factoryB-%s", false, EnumSet.of(Action.PUT));
        Set<String> pathsC = this.gatewayMgr.addPaths(
                10, "/core/factoryC-%s", true, null);
        Set<String> nodesA = this.gatewayMgr.addNodes(
                10, "127.0.0.1", 10000, NodeStatus.AVAILABLE, null);
        Set<String> nodesB = this.gatewayMgr.addNodes(
                10, "127.0.0.1", 20000, NodeStatus.UNAVAILABLE, null);
        Set<String> nodesC = this.gatewayMgr.addNodes(
                10, "127.0.0.1", 30000, NodeStatus.UNHEALTHY, null);
        this.gatewayMgr.verifyGatewayState();

        // Update config.
        this.gatewayMgr.updateConfig(GatewayStatus.UNAVAILABLE);
        this.gatewayMgr.updatePaths(pathsA, true, null);
        this.gatewayMgr.updateNodes(nodesC, NodeStatus.UNAVAILABLE, null);
        this.gatewayMgr.verifyGatewayState();

        // Delete config
        this.gatewayMgr.deletePaths(pathsB);
        this.gatewayMgr.deleteNodes(nodesA);
        this.gatewayMgr.verifyGatewayState();

        // Restart the gateway host. And make sure that the
        // config gets populated correctly during bootstrap.
        this.gatewayHost.stop();
        this.gatewayHost.setPort(0);
        if (!VerificationHost.restartStatefulHost(this.gatewayHost)) {
            this.gatewayHost.log("Restart of gatewayHost failed, aborting");
            return;
        }
        setupGatewayHost(this.gatewayHost, GATEWAY_ID);
        this.gatewayMgr.verifyGatewayState();

        // Make some config changes again.
        this.gatewayMgr.deleteConfig();
        this.gatewayMgr.deletePaths(pathsC);
        this.gatewayMgr.deleteNodes(nodesB);
        this.gatewayMgr.verifyGatewayState();
    }

    /**
     * This test verifies the gateway service in a multi-node setup.
     * It adds/ removes hosts dynamically while making changes to the
     * configuration to make sure that the gateway cached state on each host
     * is updated as expected.
     */
    @Test
    public void testMultiNodeGatewaySetup() throws Throwable {
        // Add some configuration to the first node.
        this.gatewayMgr.addConfig(GatewayStatus.AVAILABLE);
        this.gatewayMgr.addPaths(10, "/core/factoryA-%s", false, EnumSet.of(Action.POST));
        this.gatewayMgr.addPaths(10, "/core/factoryB-%s", false, EnumSet.of(Action.PUT));
        this.gatewayMgr.addPaths(10, "/core/factoryC-%s", true, null);
        this.gatewayMgr.addNodes(10, "127.0.0.1", 10000, NodeStatus.AVAILABLE, null);
        this.gatewayMgr.addNodes(10, "127.0.0.1", 20000, NodeStatus.UNAVAILABLE, null);
        this.gatewayMgr.addNodes(10, "127.0.0.1", 30000, NodeStatus.UNHEALTHY, null);
        this.gatewayMgr.verifyGatewayState();
        this.gatewayHost.addPeerNode(this.gatewayHost);

        // Start adding nodes and make sure after every addition
        // that each node reflects the latest and up-to-date cache
        // state.
        for (int i = 0; i < this.peerCount; i++) {
            VerificationHost peerHost = VerificationHost.create(0);
            peerHost.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(
                    VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
            peerHost.start();
            setupGatewayHost(peerHost, GATEWAY_ID);

            this.gatewayHost.addPeerNode(peerHost);
            this.gatewayHost.joinNodesAndVerifyConvergence(i + 1);

            // As we are adding nodes, also keep updating configuration.
            this.gatewayMgr.addPaths(
                    1, "/core/factoryA" + i, true, null);
            this.gatewayMgr.addNodes(
                    1, "127.0.0.1", 40000 + i, NodeStatus.AVAILABLE, null);

            // Now verify the state across all peers.
            this.gatewayMgr.verifyGatewayStateAcrossPeers();
        }
    }

    @SuppressWarnings("unchecked")
    private <T, S> T makeRequest(Action action, String path,
                                 S body, Class<T> clazz,
                                 int statusCode) {
        T[] response = (T[]) Array.newInstance(clazz, 1);
        TestContext ctx = this.gatewayHost.testCreate(1);
        Operation op = Operation.createPost(this.gatewayHost, path)
                .setBody(body)
                .setReferer(this.gatewayHost.getUri())
                .setCompletion((o, e) -> {
                    if (o.getStatusCode() != statusCode) {
                        Exception ex = new IllegalStateException(
                                "Expected statusCode: " + statusCode +
                                        ", returned statusCode: " + o.getStatusCode());
                        ctx.failIteration(ex);
                        return;
                    }
                    response[0] = o.getBody(clazz);
                    ctx.completeIteration();
                });
        op.setAction(action);
        this.gatewayHost.sendRequest(op);
        ctx.await();

        return response[0];
    }
}
