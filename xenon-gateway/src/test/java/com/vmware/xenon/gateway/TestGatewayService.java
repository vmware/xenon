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
            setupGatewayHost(this.gatewayHost);
        }
        if (this.gatewayMgr == null) {
            this.gatewayMgr = new TestGatewayManager(this.gatewayHost);
        }
    }

    private void setupGatewayHost(VerificationHost host) throws Throwable {
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
        host.startFactory(GatewayPathService.class, GatewayPathService::createFactory);
        host.startFactory(GatewayConfigService.class, GatewayConfigService::createFactory);

        // Wait for the factories to become available.
        waitForReplicatedFactoryServiceAvailable(host, GatewayPathService.FACTORY_LINK);
        waitForReplicatedFactoryServiceAvailable(host, GatewayConfigService.FACTORY_LINK);

        // Start the gateway service.
        TestContext ctx = host.testCreate(1);
        Operation postOp = Operation
                .createPost(host.getUri())
                .setCompletion(ctx.getCompletion());
        host.startService(postOp, new GatewayService(host.getUri()));
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

        GatewayConfigService.State configState = createConfigState(GatewayStatus.AVAILABLE);

        this.gatewayMgr.addConfig(configState);
        this.gatewayMgr.addPaths(ExampleService.FACTORY_LINK, 1, null);
        this.gatewayMgr.addPaths(MINIMAL_SERVICE_LINK, 1, null);
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
        GatewayConfigService.State configState = createConfigState(GatewayStatus.PAUSED);
        this.gatewayMgr.addConfig(configState);
        this.gatewayMgr.verifyGatewayState();
        rsp = makeRequest(
                Action.GET, ExampleService.FACTORY_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_NOT_FOUND);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_NOT_FOUND);

        // Add the path with POST verb. It should now fail with
        // http 405, since GET ver is not yet registered.
        Set<String> paths = this.gatewayMgr.addPaths(
                ExampleService.FACTORY_LINK, 1, EnumSet.of(Action.POST));
        this.gatewayMgr.verifyGatewayState();
        rsp = makeRequest(
                Action.GET, ExampleService.FACTORY_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_BAD_METHOD);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_BAD_METHOD);

        // Add the GET verb now. The request should still fail
        // with UNAVAILABLE error code since gateway is PAUSED.
        this.gatewayMgr.updatePaths(paths, EnumSet.noneOf(Action.class));
        this.gatewayMgr.verifyGatewayState();
        rsp = makeRequest(
                Action.GET, ExampleService.FACTORY_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_UNAVAILABLE);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_UNAVAILABLE);

        // Change gateway state to AVAILABLE. The request should still
        // fail since there are no AVAILABLE nodes
        this.gatewayMgr.changeConfigStatus(GatewayStatus.AVAILABLE);
        this.gatewayMgr.verifyGatewayState();
        rsp = makeRequest(
                Action.GET, ExampleService.FACTORY_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_UNAVAILABLE);
        assertTrue(rsp.statusCode == Operation.STATUS_CODE_UNAVAILABLE);

        // Register a forwardingURI. This time it should succeed!
        setupBackendHost();
        this.gatewayMgr.changeForwardingUri(this.backendHost.getUri());
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
        GatewayConfigService.State configState = createConfigState(GatewayStatus.AVAILABLE);
        configState.forwardingUri = new URI("http://127.0.0.1:2132");
        this.gatewayMgr.addConfig(configState);
        Set<String> pathsA = this.gatewayMgr.addPaths("/core/factoryA-%s", 10, EnumSet.of(Action.POST));
        Set<String> pathsB = this.gatewayMgr.addPaths("/core/factoryB-%s", 10, EnumSet.of(Action.PUT));
        Set<String> pathsC = this.gatewayMgr.addPaths("/core/factoryC-%s", 10, null);
        this.gatewayMgr.verifyGatewayState();

        // Update config.
        configState = new GatewayConfigService.State();
        configState.status = GatewayStatus.UNAVAILABLE;
        configState.filterRequests = false;
        this.gatewayMgr.patchConfig(configState);
        this.gatewayMgr.updatePaths(pathsA, EnumSet.noneOf(Action.class));
        this.gatewayMgr.verifyGatewayState();

        // Delete config
        this.gatewayMgr.deletePaths(pathsB);
        this.gatewayMgr.verifyGatewayState();

        // Restart the gateway host. And make sure that the
        // config gets populated correctly during bootstrap.
        this.gatewayHost.stop();
        this.gatewayHost.setPort(0);
        if (!VerificationHost.restartStatefulHost(this.gatewayHost)) {
            this.gatewayHost.log("Restart of gatewayHost failed, aborting");
            return;
        }
        setupGatewayHost(this.gatewayHost);
        this.gatewayMgr.verifyGatewayState();

        // Make some config changes again.
        this.gatewayMgr.deleteConfig();
        this.gatewayMgr.deletePaths(pathsC);
        this.gatewayMgr.verifyGatewayState();
    }

    /**
     * This test verifies the gateway service in a multi-node setup.
     * It adds hosts dynamically while making changes to the
     * configuration to make sure that the gateway cached state exists
     * on each host as expected.
     */
    @Test
    public void testMultiNodeGatewaySetup() throws Throwable {
        // Add some configuration to the first node.
        GatewayConfigService.State configState = createConfigState(GatewayStatus.AVAILABLE);
        this.gatewayMgr.addConfig(configState);
        this.gatewayMgr.addPaths("/core/factoryA-%s", 10, EnumSet.of(Action.POST));
        this.gatewayMgr.verifyGatewayState();

        // Start adding nodes and make sure after every addition
        // that each node reflects the latest and up-to-date cache
        // state.
        this.gatewayHost.addPeerNode(this.gatewayHost);
        for (int i = 0; i < this.peerCount; i++) {
            VerificationHost peerHost = VerificationHost.create(0);
            peerHost.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(
                    VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
            peerHost.start();
            setupGatewayHost(peerHost);
            this.gatewayHost.addPeerNode(peerHost);
            this.gatewayHost.joinNodesAndVerifyConvergence(i + 1);

            // Wait for each gateway host to reflect the same config.
            this.gatewayMgr.verifyGatewayStateAcrossPeers();

            // Now make some additional config changes and verify that
            // also reflects on all hosts.
            this.gatewayMgr.changeRequestFilteringStatus(i % 2 == 0);
            this.gatewayMgr.addPaths("/core/factoryA" + i, 1, null);
            this.gatewayMgr.verifyGatewayStateAcrossPeers();
        }
    }

    /**
     * This test verifies that the gateway service will reject a
     * DELETE request on the gateway service self-link.
     */
    @Test
    public void testGatewayServiceStop() throws Throwable {
        makeRequest(Action.DELETE,
                GatewayService.SELF_LINK, null,
                ServiceErrorResponse.class, Operation.STATUS_CODE_BAD_METHOD);

        // Host stops should still work.
        TestContext ctx = this.gatewayHost.testCreate(1);
        Operation
                .createDelete(this.gatewayHost, GatewayService.SELF_LINK)
                .setReferer(this.gatewayHost.getUri())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                .setCompletion(ctx.getCompletion())
                .sendWith(this.gatewayHost);
        ctx.await();
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

    private GatewayConfigService.State createConfigState(GatewayStatus status) {
        GatewayConfigService.State configState = new GatewayConfigService.State();
        configState.filterRequests = true;
        configState.forwardingUri = this.backendHost != null ? this.backendHost.getUri() : null;
        configState.status = GatewayStatus.AVAILABLE;
        configState.documentSelfLink = GatewayUriPaths.DEFAULT_CONFIG_PATH;
        return configState;
    }
}
