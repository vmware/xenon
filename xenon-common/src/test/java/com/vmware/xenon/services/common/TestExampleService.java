/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.services.common;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.junit.After;
import org.junit.Test;

import com.vmware.xenon.common.AuthorizationSetupHelper;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceConfiguration;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.AuthTestUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleODLService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;

public class TestExampleService {

    private static final Long COUNTER_VALUE = Long.MAX_VALUE;
    private static final String PREFIX = "example-";

    public int serviceCount = 100;
    private List<VerificationHost> hostsToCleanup = new ArrayList<>();

    private VerificationHost createAndStartHost(boolean enableAuth) throws Throwable {
        VerificationHost host = VerificationHost.create(0);
        host.setAuthorizationEnabled(enableAuth);

        // to speed up tests, set short maintenance interval.
        // it needs to "explicitly" set for VerificationHost instance
        host.setMaintenanceIntervalMicros(
                TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        host.start();

        // add to the list for cleanup after each test run
        this.hostsToCleanup.add(host);
        return host;
    }

    @After
    public void tearDown() {
        this.hostsToCleanup.forEach(VerificationHost::tearDown);
        this.hostsToCleanup.clear();
    }

    @Test
    public void strictUpdateVersionCheck() throws Throwable {
        VerificationHost host = createAndStartHost(false);
        // Make sure example factory is started. the host does not wait for it
        // to start since its not a core service. Note that in production code
        // this is all asynchronous, you should not block and wait, just pass a
        // completion.
        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        TestRequestSender sender = new TestRequestSender(host);

        ExampleServiceState initialState = new ExampleServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = Long.MAX_VALUE;

        // Create an example service
        Operation createPost = Operation.createPost(host, ExampleService.FACTORY_LINK).setBody(initialState);
        ExampleServiceState rsp = sender.sendAndWait(createPost, ExampleServiceState.class);

        // Make some regular updates
        initialState.name = rsp.name + "update-1";
        ExampleServiceState state = sender.sendAndWait(Operation.createPatch(host, rsp.documentSelfLink)
                .setBody(initialState), ExampleServiceState.class);
        assertTrue(state.name.endsWith("update-1"));

        initialState.name = rsp.name + "update-2";
        initialState.counter = 1L;
        state = sender.sendAndWait(Operation.createPatch(host, rsp.documentSelfLink)
                .setBody(initialState), ExampleServiceState.class);
        assertTrue(state.name.endsWith("update-2"));

        // Verify that strict update succeeds with correct version
        ExampleService.StrictUpdateRequest strictUpdateRequest = new ExampleService.StrictUpdateRequest();
        strictUpdateRequest.documentVersion = state.documentVersion;
        strictUpdateRequest.name = rsp.name + "update-3";
        strictUpdateRequest.kind = Utils.buildKind(ExampleService.StrictUpdateRequest.class);
        state = sender.sendAndWait(Operation.createPatch(host, rsp.documentSelfLink)
                .setBody(strictUpdateRequest), ExampleServiceState.class);
        assertTrue(state.name.endsWith("update-3"));

        // Verify that strict update fails with wrong version
        strictUpdateRequest.documentVersion = state.documentVersion - 1;
        sender.sendAndWaitFailure(Operation.createPatch(host, rsp.documentSelfLink)
                .setBody(strictUpdateRequest));
    }

    @Test
    public void singleNodeFactoryPost() throws Throwable {
        VerificationHost host = createAndStartHost(false);
        // make sure example factory is started. the host does not wait for it
        // to start since its not a core service. Note that in production code
        // this is all asynchronous, you should not block and wait, just pass a
        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        TestRequestSender sender = new TestRequestSender(host);

        List<Operation> postOps = getOpsToCreateExampleServices(host, "factory-post");
        List<ExampleServiceState> childStates = sender.sendAndWait(postOps, ExampleServiceState.class);

        // do GET on all child URIs
        for (ExampleServiceState s : childStates) {
            assertEquals(COUNTER_VALUE, s.counter);
            assertTrue(s.name.startsWith(PREFIX));
            assertEquals(host.getId(), s.documentOwner);
            assertEquals(3, s.keyValues.size());
            assertEquals(Long.valueOf(0), s.documentEpoch);
        }

        // verify template GET works on factory
        URI factoryUri = UriUtils.buildUri(host, ExampleService.FACTORY_LINK);
        URI uri = UriUtils.extendUri(factoryUri, ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE);
        ServiceDocumentQueryResult templateResult = sender.sendGetAndWait(uri,
                ServiceDocumentQueryResult.class);

        assertTrue(templateResult.documentLinks.size() == templateResult.documents.size());
        ExampleServiceState childTemplate = Utils.fromJson(
                templateResult.documents.get(templateResult.documentLinks.iterator().next()),
                ExampleServiceState.class);
        assertNotNull(childTemplate.keyValues);
        assertNotNull(childTemplate.counter);
        assertNotNull(childTemplate.name);
        assertNotNull(childTemplate.documentDescription);
        assertNotNull(childTemplate.documentDescription.propertyDescriptions);
        assertTrue(childTemplate.documentDescription.propertyDescriptions.size() > 0);
        assertTrue(childTemplate.documentDescription.propertyDescriptions.containsKey("name"));
        assertTrue(childTemplate.documentDescription.propertyDescriptions.containsKey("counter"));

        PropertyDescription pdMap = childTemplate.documentDescription.propertyDescriptions
                .get(ExampleServiceState.FIELD_NAME_KEY_VALUES);
        assertTrue(pdMap.usageOptions.contains(PropertyUsageOption.OPTIONAL));
        assertTrue(pdMap.indexingOptions.contains(PropertyIndexingOption.EXPAND));
    }

    @Test
    public void singleNodeFactoryPatchMap() throws Throwable {

        VerificationHost host = createAndStartHost(false);
        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        TestRequestSender sender = new TestRequestSender(host);

        //create example services
        List<Operation> postOps = getOpsToCreateExampleServices(host, "patch-map");
        List<ExampleServiceState> childStates = sender.sendAndWait(postOps, ExampleServiceState.class);
        List<String> childPaths = childStates.stream().map(state -> state.documentSelfLink).collect(toList());


        //test that example services are created correctly
        for (ExampleServiceState s : childStates) {
            assertEquals(COUNTER_VALUE, s.counter);
            assertTrue(s.name.startsWith(PREFIX));
            assertEquals(host.getId(), s.documentOwner);
            assertEquals(3, s.keyValues.size());
            assertEquals("test-value-1", s.keyValues.get("test-key-1"));
            assertEquals("test-value-2", s.keyValues.get("test-key-2"));
            assertEquals("test-value-3", s.keyValues.get("test-key-3"));
            assertEquals(Long.valueOf(0), s.documentEpoch);
        }

        //patch example services
        List<Operation> patches = new ArrayList<>();
        for (ExampleServiceState s : childStates) {
            s.keyValues.put("test-key-1", "test-value-1-patch-1");
            s.keyValues.put("test-key-2", "test-value-2-patch-1");
            s.keyValues.put("test-key-3", "test-value-3-patch-1");
            Operation createPatch = Operation
                    .createPatch(UriUtils.buildUri(host, s.documentSelfLink))
                    .setBody(s);
            patches.add(createPatch);
        }
        sender.sendAndWait(patches);

        //test that example services are patched correctly
        List<ExampleServiceState> patchedStates = getExampleServiceStates(host, childPaths);
        for (ExampleServiceState s : patchedStates) {
            assertEquals(COUNTER_VALUE, s.counter);
            assertTrue(s.name.startsWith(PREFIX));
            assertEquals(host.getId(), s.documentOwner);
            assertEquals(3, s.keyValues.size());
            assertEquals("test-value-1-patch-1", s.keyValues.get("test-key-1"));
            assertEquals("test-value-2-patch-1", s.keyValues.get("test-key-2"));
            assertEquals("test-value-3-patch-1", s.keyValues.get("test-key-3"));
            assertEquals(Long.valueOf(0), s.documentEpoch);
        }

        //patch example services when deleting some values in the keyValues map
        List<Operation> patchesToSetNull = new ArrayList<>();
        for (ExampleServiceState s : patchedStates) {
            s.keyValues.put("test-key-1", "test-value-1-patch-1");
            s.keyValues.put("test-key-2", null);
            s.keyValues.put("test-key-3", null);
            Operation createPatch = Operation
                    .createPatch(UriUtils.buildUri(host, s.documentSelfLink))
                    .setBody(s);
            patchesToSetNull.add(createPatch);
        }
        sender.sendAndWait(patchesToSetNull);

        //test that deleted values in the keyValues map are gone
        List<ExampleServiceState> patchesToSetNullStates = getExampleServiceStates(host, childPaths);
        for (ExampleServiceState s : patchesToSetNullStates) {
            assertEquals(COUNTER_VALUE, s.counter);
            assertTrue(s.name.startsWith(PREFIX));
            assertEquals(host.getId(), s.documentOwner);
            assertEquals(1, s.keyValues.size());
            assertEquals("test-value-1-patch-1", s.keyValues.get("test-key-1"));
            assertEquals(Long.valueOf(0), s.documentEpoch);
        }
    }

    @Test
    public void singleNodePutExpectFailure() throws Throwable {

        VerificationHost host = createAndStartHost(false);

        // make sure example factory is started. the host does not wait for it
        // to start since its not a core service. Note that in production code
        // this is all asynchronous, you should not block and wait, just pass a
        // completion
        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        ExampleServiceState initialState = new ExampleServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = Long.MAX_VALUE;

        // create an example service
        Operation createPost = Operation.createPost(host, ExampleService.FACTORY_LINK).setBody(initialState);
        TestRequestSender sender = new TestRequestSender(host);
        ServiceDocument rsp = sender.sendAndWait(createPost, ServiceDocument.class);
        URI childURI = UriUtils.buildUri(host, rsp.documentSelfLink);


        // issue a PUT that we expect it to fail.
        ExampleServiceState emptyBody = new ExampleServiceState();
        Operation put = Operation.createPut(childURI).setBody(emptyBody);

        FailureResponse failureResponse = sender.sendAndWaitFailure(put);
        assertEquals("name must be set", failureResponse.failure.getMessage());
    }

    private List<Operation> getOpsToCreateExampleServices(ServiceHost host, String suffix) {
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState initialState = new ExampleServiceState();
            initialState.name = initialState.documentSelfLink = PREFIX + i + suffix;
            initialState.counter = COUNTER_VALUE;
            initialState.keyValues.put("test-key-1", "test-value-1");
            initialState.keyValues.put("test-key-2", "test-value-2");
            initialState.keyValues.put("test-key-3", "test-value-3");
            // create an example service
            Operation createPost = Operation.createPost(host, ExampleService.FACTORY_LINK).setBody(initialState);
            ops.add(createPost);
        }
        return ops;
    }

    private List<ExampleServiceState> getExampleServiceStates(ServiceHost host, List<String> servicePaths) {
        List<Operation> ops = servicePaths.stream()
                .map(path -> Operation.createGet(host, path))
                .collect(toList());
        TestRequestSender sender = new TestRequestSender(host);
        return sender.sendAndWait(ops, ExampleServiceState.class);
    }

    @Test
    public void multiNodeBasic() throws Throwable {
        // scenario:
        //   create 2 nodes and join to default group
        //   post & get example service

        // prepare multiple nodes (for simplicity, using VerificationHost)
        VerificationHost host1 = createAndStartHost(false);
        VerificationHost host2 = createAndStartHost(false);

        TestNodeGroupManager nodeGroup = new TestNodeGroupManager();
        nodeGroup.addHost(host1);
        nodeGroup.addHost(host2);

        // make node group join to the "default" node group, then wait cluster to be stabilized
        nodeGroup.joinNodeGroupAndWaitForConvergence();

        // wait the service to be available in cluster
        nodeGroup.waitForFactoryServiceAvailable(ExampleService.FACTORY_LINK);

        // prepare operation sender(client)
        ServiceHost peer = nodeGroup.getHost();
        TestRequestSender sender = new TestRequestSender(peer);

        // POST request. create a doc with static selflink: /core/examples/foo
        String suffix = "foo";
        ExampleServiceState body = new ExampleServiceState();
        body.name = "FOO";
        body.documentSelfLink = suffix;
        Operation post = Operation.createPost(peer, ExampleService.FACTORY_LINK).setBody(body);

        // verify post response
        ExampleServiceState result = sender.sendAndWait(post, ExampleServiceState.class);
        assertEquals("FOO", result.name);

        // make get and validate result
        String servicePath = UriUtils.buildUriPath(ExampleService.FACTORY_LINK, suffix);
        Operation get = Operation.createGet(peer, servicePath);
        ExampleServiceState getResult = sender.sendAndWait(get, ExampleServiceState.class);

        // validate get result...
        assertEquals("FOO", getResult.name);
    }

    @Test
    public void multiNodeCompression() throws Throwable {
        // scenario:
        //   create 2 nodes and join to default group
        //   post & get example service with compression enabled

        // prepare multiple nodes (for simplicity, using VerificationHost)
        VerificationHost host1 = createAndStartHost(false);
        VerificationHost host2 = createAndStartHost(false);

        TestNodeGroupManager nodeGroup = new TestNodeGroupManager();
        nodeGroup.addHost(host1);
        nodeGroup.addHost(host2);

        // make node group join to the "default" node group, then wait cluster to be stabilized
        nodeGroup.joinNodeGroupAndWaitForConvergence();

        // wait the service to be available in cluster
        nodeGroup.waitForFactoryServiceAvailable(ExampleService.FACTORY_LINK);

        // prepare operation sender(client)
        ServiceHost peer = nodeGroup.getHost();
        TestRequestSender sender = new TestRequestSender(peer);

        // POST request. create a doc with static selflink: /core/examples/foo
        String suffix = "foo";
        ExampleServiceState body = new ExampleServiceState();
        body.name = "FOO";
        body.documentSelfLink = suffix;
        Operation post = Operation.createPost(peer, ExampleService.FACTORY_LINK).setBody(body);

        // enable compression
        post.addRequestHeader(Operation.CONTENT_ENCODING_HEADER, Operation.CONTENT_ENCODING_GZIP);
        post.addRequestHeader(Operation.ACCEPT_ENCODING_HEADER, Operation.CONTENT_ENCODING_GZIP);

        // verify post response
        ExampleServiceState result = sender.sendAndWait(post, ExampleServiceState.class);
        assertEquals("FOO", result.name);

        // make get and validate result
        String servicePath = UriUtils.buildUriPath(ExampleService.FACTORY_LINK, suffix);
        Operation get = Operation.createGet(peer, servicePath);
        get.addRequestHeader(Operation.CONTENT_ENCODING_HEADER, Operation.CONTENT_ENCODING_GZIP);
        get.addRequestHeader(Operation.ACCEPT_ENCODING_HEADER, Operation.CONTENT_ENCODING_GZIP);

        ExampleServiceState getResult = sender.sendAndWait(get, ExampleServiceState.class);

        // validate get result...
        assertEquals("FOO", getResult.name);

        // now do a manual get with manual gunzip to check compresion was actually used
        URL url = new URL(peer.getUri() + getResult.documentSelfLink);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty(Operation.ACCEPT_ENCODING_HEADER, Operation.CONTENT_ENCODING_GZIP);

        InputStream inputStream = conn.getInputStream();
        ByteArrayOutputStream baos;
        try (GZIPInputStream zis = new GZIPInputStream(inputStream)) {
            baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int read = 0;
            while ((read = zis.read(buffer, 0, buffer.length)) != -1) {
                baos.write(buffer, 0, read);
            }   baos.flush();
        }

        String json = new String(baos.toByteArray(), Utils.CHARSET);
        ExampleServiceState manualResult = Utils.fromJson(json, ExampleServiceState.class);
        assertEquals("FOO", manualResult.name);
    }

    @Test
    public void multiNodeOnDemandLoad() throws Throwable {
        // scenario:
        //   create 2 nodes and join to default group
        //   post & get example service

        // prepare multiple nodes (for simplicity, using VerificationHost)
        VerificationHost host1 = createAndStartHost(false);
        host1.startFactory(ExampleODLService::createFactory, ExampleODLService.FACTORY_LINK);
        VerificationHost host2 = createAndStartHost(false);
        host2.startFactory(ExampleODLService::createFactory, ExampleODLService.FACTORY_LINK);

        TestNodeGroupManager nodeGroup = new TestNodeGroupManager();
        int maintMillis = VerificationHost.FAST_MAINT_INTERVAL_MILLIS / 2;
        nodeGroup.setMaintenanceInterval(Duration.ofMillis(maintMillis));
        host1.setServiceCacheClearDelayMicros(TimeUnit.MILLISECONDS.toMicros(maintMillis));
        nodeGroup.addHost(host1);
        nodeGroup.addHost(host2);

        // make node group join to the "default" node group, then wait cluster to be stabilized
        nodeGroup.joinNodeGroupAndWaitForConvergence();

        // wait the service to be available in cluster
        nodeGroup.waitForFactoryServiceAvailable(ExampleODLService.FACTORY_LINK);

        // create a request sender which can be any host in this scenario
        TestRequestSender sender = new TestRequestSender(host1);

        String suffix = "foo";
        ExampleServiceState postBody = new ExampleServiceState();
        postBody.name = suffix;
        postBody.counter = 0L;
        postBody.documentSelfLink = suffix; // static self link: /core/examples/foo

        Operation post = Operation.createPost(host1, ExampleODLService.FACTORY_LINK)
                .setBody(postBody);

        // send POST and wait for response
        sender.sendAndWait(post, ExampleServiceState.class);

        String servicePath = UriUtils.buildUriPath(ExampleODLService.FACTORY_LINK, suffix);

        // verify ODL is enabled
        URI configUri = UriUtils.buildConfigUri(UriUtils.buildUri(host1, servicePath));
        ServiceConfiguration configState = sender.sendGetAndWait(configUri,
                ServiceConfiguration.class);
        assertTrue(configState.options.contains(ServiceOption.ON_DEMAND_LOAD));

        for (int i = 0; i < 5; i++) {
            ExampleServiceState patchBody = new ExampleServiceState();
            patchBody.name = "foo-" + i;
            patchBody.counter = (long) (i + 1);
            Operation patch = Operation.createPatch(host1, servicePath).setBody(patchBody);
            sender.sendAndWait(patch);
            host1.log("Sent patch, now sleeping to induce on demand stop / load");
            Thread.sleep(maintMillis * 5);
        }
    }

    @Test
    public void multipleNodeGroups() throws Throwable {
        // scenario:
        //   create custom node groups - groupA and groupB
        //   post to groupA
        //   change quorum in groupA

        // prepare multiple nodes (for simplicity, using VerificationHost
        VerificationHost host1 = createAndStartHost(false);  // will join groupA & groupB
        VerificationHost host2 = createAndStartHost(false);  // will join groupA only
        VerificationHost host3 = createAndStartHost(false);  // will join groupA only
        VerificationHost host4 = createAndStartHost(false);  // will join groupB only

        // create a request sender which can be any host in this scenario
        TestRequestSender sender = new TestRequestSender(host1);

        TestNodeGroupManager groupA = new TestNodeGroupManager("groupA")
                .addHost(host1)
                .addHost(host2)
                .addHost(host3)
                .createNodeGroup() // create groupA
                .joinNodeGroupAndWaitForConvergence();  // make them join groupA

        TestNodeGroupManager groupB = new TestNodeGroupManager("groupB")
                .addHost(host1)
                .addHost(host4)
                .createNodeGroup()  // create groupB
                .joinNodeGroupAndWaitForConvergence();  // make them join groupB

        // wait the service to be available in cluster
        groupA.waitForFactoryServiceAvailable(ExampleService.FACTORY_LINK);
        groupB.waitForFactoryServiceAvailable(ExampleService.FACTORY_LINK);

        // perform operation and verify...

        // POST request to host2 which is in groupA
        String suffix = "foo";
        ExampleServiceState body = new ExampleServiceState();
        body.name = "FOO";
        body.documentSelfLink = suffix;
        Operation post = Operation.createPost(host2, ExampleService.FACTORY_LINK).setBody(body);

        ExampleServiceState postResult = sender.sendAndWait(post, ExampleServiceState.class);

        // verify owner is one of the host in groupA
        Set<String> expectedOwnerIds = groupA.getAllHosts().stream()
                .map(ServiceHost::getId)
                .collect(toSet());
        String msg = String.format("DocumentOwner %s is not in %s", postResult.documentOwner, expectedOwnerIds);
        assertTrue(msg, expectedOwnerIds.contains(postResult.documentOwner));


        // send a GET to host4 which is in groupB. It should fail.
        String servicePath = UriUtils.buildUriPath(ExampleService.FACTORY_LINK, suffix);
        Operation get = Operation.createGet(host4, servicePath);
        FailureResponse failure = sender.sendAndWaitFailure(get);
        assertEquals(failure.op.getStatusCode(), 404);


        // another use case. relaxing quorum

        // check current quorum is 3
        verifyCurrentQuorum(groupA, 3);

        // relax quorum to 2, location quorum stays at 1
        groupA.updateQuorum(2, 1);

        // internally updateQuorum() checks the new quorum value, so this check is not required but
        // just for demo purpose.
        verifyCurrentQuorum(groupA, 2);
    }

    private void verifyCurrentQuorum(TestNodeGroupManager manager, int expectedQuorum) {
        ServiceHost node = manager.getHost();
        TestRequestSender sender = new TestRequestSender(node);


        String nodeGroupPath = UriUtils.buildUriPath(ServiceUriPaths.NODE_GROUP_FACTORY, manager.getNodeGroupName());
        Operation op = Operation.createGet(node, nodeGroupPath);
        NodeGroupState nodeGroupState = sender.sendAndWait(op, NodeGroupState.class);

        nodeGroupState.nodes.values().forEach(
                nodeState -> assertEquals("quorum on host=" + nodeState.id, expectedQuorum, nodeState.membershipQuorum));
    }


    @Test
    public void authorization() throws Throwable {
        VerificationHost host = createAndStartHost(true);

        TestNodeGroupManager nodeGroup = new TestNodeGroupManager();
        nodeGroup.addHost(host);

        // perform lambda under system auth context
        // see user creation below to perform logic under system auth context in descriptive way
        AuthTestUtils.executeWithSystemAuthContext(nodeGroup, () -> {
            nodeGroup.joinNodeGroupAndWaitForConvergence();
            nodeGroup.waitForFactoryServiceAvailable(ExampleService.FACTORY_LINK);
        });


        // create user, user-group, resource-group, role for foo@vmware.com
        //   user: /core/authz/users/foo@vmware.com
        TestContext waitContext = new TestContext(1, Duration.ofSeconds(30));
        String username = "foo@vmware.com";
        AuthorizationSetupHelper userBuilder = AuthorizationSetupHelper.create()
                .setHost(host)
                .setUserSelfLink(username)
                .setUserEmail(username)
                .setUserPassword("password")
                .setDocumentKind(Utils.buildKind(ExampleServiceState.class))
                .setCompletion(waitContext.getCompletion());

        // descriptively execute code under system auth context
        AuthTestUtils.setSystemAuthorizationContext(host);
        userBuilder.start();
        AuthTestUtils.resetAuthorizationContext(host);

        waitContext.await();


        // check login failure
        AuthTestUtils.loginExpectFailure(nodeGroup, username, "wrong password");


        // login and subsequent operations will be performed as foo@vmware.com
        AuthTestUtils.loginAndSetToken(nodeGroup, username, "password");

        // POST request (static selflink: /core/examples/foo)
        String suffix = "foo";
        ExampleServiceState body = new ExampleServiceState();
        body.name = "FOO";
        body.documentSelfLink = suffix;
        Operation post = Operation.createPost(host, ExampleService.FACTORY_LINK).setBody(body);

        // verify post response
        TestRequestSender sender = new TestRequestSender(host);
        ExampleServiceState postResult = sender.sendAndWait(post, ExampleServiceState.class);
        assertEquals("FOO", postResult.name);
        String expectedAuthPrincipalLink = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, username);
        assertEquals(expectedAuthPrincipalLink, postResult.documentAuthPrincipalLink);

        String servicePath = UriUtils.buildUriPath(ExampleService.FACTORY_LINK, suffix);
        Operation get = Operation.createGet(host, servicePath);
        Operation getResponse = sender.sendAndWait(get);
        assertEquals(200, getResponse.getStatusCode());


        AuthTestUtils.logout(nodeGroup);

        // after logout, request should fail
        Operation getAfterLogout = Operation.createGet(host, servicePath);
        FailureResponse failureResponse = sender.sendAndWaitFailure(getAfterLogout);
        assertEquals(403, failureResponse.op.getStatusCode());
    }
}
