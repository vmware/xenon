/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.common.test.VerificationHost.WaitHandler;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MigrationTaskService.MigrationOption;
import com.vmware.xenon.services.common.MigrationTaskService.State;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeState.NodeOption;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.TestNodeGroupService.ExampleFactoryServiceWithCustomSelector;

public class TestMigrationTaskService extends BasicReusableHostTestCase {
    private static final String CUSTOM_NODE_GROUP_NAME = "custom";
    private static final String CUSTOM_NODE_GROUP = UriUtils.buildUriPath(
            ServiceUriPaths.NODE_GROUP_FACTORY,
            CUSTOM_NODE_GROUP_NAME);
    private static final String CUSTOM_GROUP_NODE_SELECTOR = UriUtils.buildUriPath(
            ServiceUriPaths.NODE_SELECTOR_PREFIX,
            CUSTOM_NODE_GROUP_NAME);
    private static final String CUSTOM_EXAMPLE_FACTORY_LINK = "custom-group-examples";

    private static final int UNACCESSABLE_PORT = 123;
    private static final URI FAKE_URI = UriUtils.buildUri("127.0.0.1", UNACCESSABLE_PORT, null, null);
    private static final String TRANSFORMATION = "transformation";
    private static final String TRANSFORMATION_V2 = "transformation-v2";
    private static final String TRANSFORMATION_V3 = "transformation-v3";
    private URI sourceFactoryUri;
    private URI destinationFactoryUri;
    private URI exampleSourceFactory;
    private URI exampleDestinationFactory;

    private static VerificationHost destinationHost;

    public long serviceCount = 10;
    public int nodeCount = 3;
    public int iterationCount = 1;
    private URI exampleWithCustomSelectorDestinationFactory;
    private URI exampleWithCustomSelectorDestinationFactoryOnObserver;
    private URI destinationCustomNodeGroupOnObserver;
    private URI exampleWithCustomSelectorSourceFactory;

    @Before
    public void setUp() throws Throwable {
        if (this.host.getInProcessHostMap().isEmpty()) {
            this.host.setStressTest(this.host.isStressTest);
            this.host.setPeerSynchronizationEnabled(true);
            this.host.setUpPeerHosts(this.nodeCount);
            getSourceHost().setNodeGroupQuorum(this.nodeCount);
            this.host.joinNodesAndVerifyConvergence(this.nodeCount, true);
            this.host.setNodeGroupQuorum(this.nodeCount);;

            for (VerificationHost host : this.host.getInProcessHostMap().values()) {
                startMigrationService(host);
                host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
                host.waitForServiceAvailable(MigrationTaskService.FACTORY_LINK);
            }

            setupCustomNodeGroup(this.host);
        }
        for (VerificationHost host : this.host.getInProcessHostMap().values()) {
            host.toggleServiceOptions(UriUtils.buildUri(host, ExampleService.FACTORY_LINK),
                    EnumSet.of(ServiceOption.IDEMPOTENT_POST), null);
        }

        if (destinationHost == null) {
            destinationHost = VerificationHost.create(0);
            destinationHost.start();
            destinationHost.setStressTest(destinationHost.isStressTest);
            destinationHost.setPeerSynchronizationEnabled(true);
            destinationHost.setUpPeerHosts(this.nodeCount);
            destinationHost.joinNodesAndVerifyConvergence(this.nodeCount);
            destinationHost.setNodeGroupQuorum(this.nodeCount);
            for (VerificationHost host : destinationHost.getInProcessHostMap().values()) {
                startMigrationService(host);
                host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
                host.waitForServiceAvailable(MigrationTaskService.FACTORY_LINK);
            }

            setupCustomNodeGroup(destinationHost);
        }

        for (VerificationHost host : destinationHost.getInProcessHostMap().values()) {
            host.toggleServiceOptions(UriUtils.buildUri(host, ExampleService.FACTORY_LINK),
                    EnumSet.of(ServiceOption.IDEMPOTENT_POST), null);
        }

        this.sourceFactoryUri = UriUtils.buildUri(getSourceHost(), MigrationTaskService.FACTORY_LINK);
        this.destinationFactoryUri = UriUtils.buildUri(getDestinationHost(),
                MigrationTaskService.FACTORY_LINK);

        this.exampleSourceFactory = UriUtils.buildUri(getSourceHost(), ExampleService.FACTORY_LINK);
        this.exampleDestinationFactory = UriUtils.buildUri(getDestinationHost(),
                ExampleService.FACTORY_LINK);

        setUpCustomGroupUris(this.host, false);
        setUpCustomGroupUris(destinationHost, true);

        this.host.waitForReplicatedFactoryServiceAvailable(this.destinationFactoryUri);
        this.host.waitForReplicatedFactoryServiceAvailable(this.sourceFactoryUri);
        this.host.waitForReplicatedFactoryServiceAvailable(this.exampleSourceFactory);
        this.host.waitForReplicatedFactoryServiceAvailable(this.exampleDestinationFactory);
    }

    private void setUpCustomGroupUris(VerificationHost testHost, boolean isDestination) {
        for (URI hostUri : testHost.getInProcessHostMap().keySet()) {
            URI nodeGroupUri = UriUtils.buildUri(hostUri, CUSTOM_NODE_GROUP);
            NodeGroupState ngs = getDestinationHost().getServiceState(null, NodeGroupState.class,
                    nodeGroupUri);
            URI factoryUri = UriUtils.buildUri(
                    nodeGroupUri,
                    CUSTOM_EXAMPLE_FACTORY_LINK);
            if (isDestination) {
                if (ngs.nodes.get(ngs.documentOwner).options.contains(NodeOption.OBSERVER)) {
                    this.exampleWithCustomSelectorDestinationFactoryOnObserver = factoryUri;
                    this.destinationCustomNodeGroupOnObserver = nodeGroupUri;
                } else {
                    // use the factory link on a non observer node
                    this.exampleWithCustomSelectorDestinationFactory = factoryUri;
                    this.destinationCustomNodeGroupOnObserver = nodeGroupUri;
                }
            } else {
                if (!ngs.nodes.get(ngs.documentOwner).options.contains(NodeOption.OBSERVER)) {
                    // use the factory link on a non observer node
                    this.exampleWithCustomSelectorSourceFactory = factoryUri;
                }
            }
        }
    }

    private void setupCustomNodeGroup(VerificationHost testHost) throws Throwable {

        URI observerHostUri = testHost.getPeerHostUri();
        ServiceHostState observerHostState = testHost.getServiceState(null,
                ServiceHostState.class,
                UriUtils.buildUri(observerHostUri, ServiceUriPaths.CORE_MANAGEMENT));
        Map<URI, NodeState> selfStatePerNode = new HashMap<>();
        NodeState observerSelfState = new NodeState();
        observerSelfState.id = observerHostState.id;
        observerSelfState.options = EnumSet.of(NodeOption.OBSERVER);

        selfStatePerNode.put(observerHostUri, observerSelfState);
        testHost.createCustomNodeGroupOnPeers(CUSTOM_NODE_GROUP_NAME, selfStatePerNode);

        // start a node selector attached to the custom group
        for (VerificationHost h : testHost.getInProcessHostMap().values()) {
            NodeSelectorState initialState = new NodeSelectorState();
            initialState.nodeGroupLink = CUSTOM_NODE_GROUP;
            h.startServiceAndWait(new ConsistentHashingNodeSelectorService(),
                    CUSTOM_GROUP_NODE_SELECTOR, initialState);
            // start the factory that is attached to the custom group selector
            h.startServiceAndWait(ExampleFactoryServiceWithCustomSelector.class,
                    CUSTOM_EXAMPLE_FACTORY_LINK);
        }

        URI customNodeGroupOnObserver = UriUtils
                .buildUri(observerHostUri, CUSTOM_NODE_GROUP);
        Map<URI, EnumSet<NodeOption>> expectedOptionsPerNode = new HashMap<>();
        expectedOptionsPerNode.put(customNodeGroupOnObserver,
                observerSelfState.options);

        testHost.joinNodesAndVerifyConvergence(CUSTOM_NODE_GROUP, this.nodeCount,
                this.nodeCount, expectedOptionsPerNode);
        // one of the nodes is observer, so we must set quorum to 2 explicitly
        testHost.setNodeGroupQuorum(2, customNodeGroupOnObserver);
        testHost.waitForNodeSelectorQuorumConvergence(CUSTOM_GROUP_NODE_SELECTOR, 2);
        testHost.waitForNodeGroupIsAvailableConvergence(CUSTOM_NODE_GROUP);

    }

    private VerificationHost getDestinationHost() {
        return destinationHost.getInProcessHostMap().values().iterator().next();
    }

    private VerificationHost getSourceHost() {
        return this.host.getInProcessHostMap().values().iterator().next();
    }

    @After
    public void cleanUp() throws Throwable {
        for (VerificationHost host : this.host.getInProcessHostMap().values()) {
            checkReusableHostAndCleanup(host);
        }

        for (VerificationHost host : destinationHost.getInProcessHostMap().values()) {
            checkReusableHostAndCleanup(host);
        }

        // need to reset the maintenance intervals on the hosts otherwise clean up can fail
        // between tests due to the very low maintenance interval set in the test for
        // continuous migration
        for (VerificationHost host : this.host.getInProcessHostMap().values()) {
            host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        }
        for (VerificationHost host : destinationHost.getInProcessHostMap().values()) {
            host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        }
    }

    @AfterClass
    public static void afterClass() throws Throwable {
        destinationHost.tearDownInProcessPeers();
        destinationHost.tearDown();
        destinationHost.stop();
    }

    private State validMigrationState() throws Throwable {
        return validMigrationState("");
    }

    private State validMigrationState(String factory) throws Throwable {
        State state = new State();
        state.destinationFactoryLink = factory;
        state.destinationNodeGroupReference
            = UriUtils.buildUri(getDestinationHost().getPublicUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);
        state.sourceFactoryLink = factory;
        state.sourceNodeGroupReference
            = UriUtils.buildUri(getSourceHost().getPublicUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);
        state.maintenanceIntervalMicros = TimeUnit.MILLISECONDS.toMicros(100);
        return state;
    }


    private State validMigrationStateForCustomNodeGroup() throws Throwable {
        State state = new State();
        state.destinationFactoryLink = this.exampleWithCustomSelectorDestinationFactory.getPath();
        // intentionally use an observer node for the target. The migration service should retrieve
        // all nodes, filter out the OBSERVER ones, and then send POSTs only on available PEER nodes
        state.destinationNodeGroupReference = this.destinationCustomNodeGroupOnObserver;
        state.sourceFactoryLink = this.exampleWithCustomSelectorSourceFactory.getPath();
        state.sourceNodeGroupReference = UriUtils.buildUri(getSourceHost().getPublicUri(),
                CUSTOM_NODE_GROUP);
        state.maintenanceIntervalMicros = TimeUnit.MILLISECONDS.toMicros(100);
        return state;
    }

    @Test
    public void successCreateTask() throws Throwable {
        State state = validMigrationState();
        final State[] outState = new State[1];

        TestContext ctx = testCreate(1);
        Operation op = Operation.createPost(this.sourceFactoryUri)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    outState[0] = o.getBody(State.class);
                    ctx.completeIteration();
                });

        getSourceHost().send(op);
        testWait(ctx);

        assertNotNull(outState[0]);
        assertEquals(outState[0].destinationFactoryLink, state.destinationFactoryLink);
        assertEquals(outState[0].destinationNodeGroupReference,
                state.destinationNodeGroupReference);
        assertEquals(outState[0].sourceFactoryLink, state.sourceFactoryLink);
        assertEquals(outState[0].sourceNodeGroupReference, state.sourceNodeGroupReference);
    }

    @Test
    public void successMigrateDocuments() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);

        Collection<URI> uris = links.stream().map(link -> UriUtils.buildUri(getSourceHost(), link))
                .collect(Collectors.toList());

        List<SimpleEntry<String, Long>> timePerNode = getSourceHost()
                .getServiceState(EnumSet.noneOf(TestProperty.class), ExampleServiceState.class, uris)
                .values()
                .stream()
                .map(d -> new AbstractMap.SimpleEntry<>(d.documentOwner, d.documentUpdateTimeMicros))
                .collect(Collectors.toList());
        Map<String, Long> times = new HashMap<>();
        for (SimpleEntry<String, Long> entry : timePerNode) {
            times.put(entry.getKey(), Math.max(times.getOrDefault(entry.getKey(), 0L), entry.getValue()));
        }
        long time = times.values().stream().mapToLong(i -> i).min().orElse(0);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);

        TestContext ctx = testCreate(1);
        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx);

        State finalServiceState = waitForServiceCompletion(out[0], getDestinationHost());
        ServiceStats stats = getStats(out[0], getDestinationHost());

        assertEquals(TaskStage.FINISHED, finalServiceState.taskInfo.stage);
        Long processedDocuments = Long.valueOf((long) stats.entries.get(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS).latestValue);
        Long estimatedTotalServiceCount = Long.valueOf((long) stats.entries.get(MigrationTaskService.STAT_NAME_ESTIMATED_TOTAL_SERVICE_COUNT).latestValue);
        assertEquals(Long.valueOf(this.serviceCount), processedDocuments);
        assertEquals(Long.valueOf(this.serviceCount), estimatedTotalServiceCount);
        assertEquals(Long.valueOf(time), finalServiceState.latestSourceUpdateTimeMicros);

        // check if object is in new host
        uris = links.stream().map(link -> UriUtils.buildUri(getDestinationHost(), link))
                .collect(Collectors.toList());
        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
    }

    @Test
    public void successMigrateDocumentsCustomNodeGroupWithObserver() throws Throwable {
        // create object in host, using custom example factory tied to custom node group
        Collection<String> links = createExampleDocuments(
                this.exampleWithCustomSelectorSourceFactory,
                getSourceHost(),
                this.serviceCount);

        Collection<URI> uris = links.stream()
                .map(link -> UriUtils.buildUri(this.exampleWithCustomSelectorSourceFactory, link))
                .collect(Collectors.toList());
        // start migration. using custom node group destination, using default node group
        // and default example service as the source
        MigrationTaskService.State migrationState = validMigrationStateForCustomNodeGroup();

        Operation op = getDestinationHost().getTestRequestSender().sendAndWait(
                Operation.createPost(this.destinationFactoryUri).setBody(migrationState));

        String taskLink = op.getBody(State.class).documentSelfLink;
        State finalServiceState = waitForServiceCompletion(taskLink, getDestinationHost());
        assertEquals(TaskStage.FINISHED, finalServiceState.taskInfo.stage);

        // check if object is in new host
        uris = links.stream().map(link -> {
            link = link.replace(ExampleService.FACTORY_LINK,
                    this.exampleWithCustomSelectorDestinationFactory.getPath());
            URI exampleUriAtDestination = UriUtils.buildUri(getDestinationHost(), link);
            return exampleUriAtDestination;
        }).collect(Collectors.toList());

        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);

        // verify custom factory, on *observer* node, in custom group, has no children
        ServiceDocumentQueryResult res = getDestinationHost()
                .getFactoryState(this.exampleWithCustomSelectorDestinationFactoryOnObserver);
        assertEquals(0L, (long) res.documentCount);
        assertTrue(res.documentLinks.isEmpty());

        // verify custom factory on PEER node, in custom group, has all the children
        URI customExampleFactoryOnPeer = this.exampleWithCustomSelectorDestinationFactory;

        res = getDestinationHost().getFactoryState(customExampleFactoryOnPeer);
        assertEquals(uris.size(), (long) res.documentCount);
        assertEquals(uris.size(), res.documentLinks.size());
    }

    @Test
    public void successNoDocuments() throws Throwable {
        // start migration
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);

        TestContext ctx = testCreate(1);
        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx);

        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        ServiceStats stats = getStats(out[0], getDestinationHost());
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertFalse(stats.entries.containsKey(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS));
    }

    @Test
    public void successNoDocumentsModifiedAfterTime() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);
        Collection<URI> uris = links.stream().map(link -> UriUtils.buildUri(getSourceHost(), link))
                .collect(Collectors.toList());
        long time = getDestinationHost()
                .getServiceState(EnumSet.noneOf(TestProperty.class), ExampleServiceState.class,
                        uris)
                .values()
                .stream()
                .mapToLong(d -> d.documentUpdateTimeMicros * 10)
                .max().orElse(0);
        assertTrue("max upateTime should not be 0", time > 0);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);
        QuerySpecification spec = new QuerySpecification();
        spec.query.addBooleanClause(
                Query.Builder.create().addRangeClause(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                        NumericRange.createGreaterThanRange(time)).build());
        migrationState.querySpec = spec;

        TestContext ctx = testCreate(1);
        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx);

        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        ServiceStats stats = getStats(out[0], getDestinationHost());
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertFalse(stats.entries.containsKey(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS));

        // check that objects were not migrated
        TestContext ctx2 = testCreate(1);
        long[] out_long = new long[1];
        Operation get = Operation.createGet(this.exampleDestinationFactory)
                .setCompletion((o, e) -> {
                    out_long[0] = o.getBody(ServiceDocumentQueryResult.class).documentCount;
                    ctx2.completeIteration();
                });
        getDestinationHost().send(get);
        testWait(ctx2);
        assertEquals(0, out_long[0]);
    }

    @Test
    public void successMigrateMultiPageResult() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);
        QuerySpecification spec = new QuerySpecification();
        spec.resultLimit = (int) (this.serviceCount / 10);
        migrationState.querySpec = spec;

        TestContext ctx = testCreate(1);
        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx);

        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        ServiceStats stats = getStats(out[0], getDestinationHost());
        Long processedDocuments = Long.valueOf((long) stats.entries.get(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS).latestValue);
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(this.serviceCount), processedDocuments);

        // check if object is in new host
        Collection<URI> uris = links.stream()
                .map(link -> UriUtils.buildUri(getDestinationHost(), link))
                .collect(Collectors.toList());
        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
    }

    @Test
    public void successMigrateOnlyDocumentsUpdatedAfterTime() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);
        Collection<URI> uris = links.stream().map(link -> UriUtils.buildUri(getSourceHost(), link))
                .collect(Collectors.toList());
        long time = getSourceHost()
                .getServiceState(EnumSet.noneOf(TestProperty.class), ExampleServiceState.class, uris)
                .values()
                .stream()
                .mapToLong(d -> d.documentUpdateTimeMicros)
                .max().orElse(0);
        assertTrue("max upateTime should not be 0", time > 0);

        Collection<String> newLinks = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount, false);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);
        QuerySpecification spec = new QuerySpecification();
        spec.query.addBooleanClause(
                Query.Builder.create().addRangeClause(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                        NumericRange.createGreaterThanRange(time)).build());
        migrationState.querySpec = spec;

        TestContext ctx = testCreate(1);
        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx);

        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        ServiceStats stats = getStats(out[0], getDestinationHost());
        Long processedDocuments = Long.valueOf((long) stats.entries.get(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS).latestValue);
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertTrue(Long.valueOf(this.serviceCount) + " <= " + processedDocuments, Long.valueOf(this.serviceCount) <= processedDocuments);

        // check if object is in new host
        uris = newLinks.stream().map(link -> UriUtils.buildUri(getDestinationHost(), link))
                .collect(Collectors.toList());
        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);

        // check that objects were not migrated
        TestContext ctx2 = testCreate(1);
        long[] out_long = new long[1];
        Operation get = Operation.createGet(this.exampleDestinationFactory)
                .setCompletion((o, e) -> {
                    out_long[0] = o.getBody(ServiceDocumentQueryResult.class).documentCount;
                    ctx2.completeIteration();
                });
        getDestinationHost().send(get);
        testWait(ctx2);
        assertTrue(newLinks.size() + " <= " + out_long[0], newLinks.size() <= out_long[0]);
        assertTrue(this.serviceCount * 2 + " > " + out_long[0], this.serviceCount * 2 > out_long[0]);
    }

    @Test
    public void successMigrateSameDocumentsTwice() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);

        TestContext ctx = testCreate(1);
        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx);

        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        ServiceStats stats = getStats(out[0], getDestinationHost());
        Long processedDocuments = Long.valueOf((long) stats.entries.get(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS).latestValue);
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(this.serviceCount), processedDocuments);

        TestContext ctx2 = testCreate(1);
        migrationState.documentSelfLink = null;
        op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx2.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx2.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx2);

        waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        stats = getStats(out[0], getDestinationHost());
        assertEquals(waitForServiceCompletion.taskInfo.stage, TaskStage.FINISHED);
        processedDocuments = Long.valueOf((long) stats.entries.get(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS).latestValue);
        assertEquals(Long.valueOf(this.serviceCount), processedDocuments);

        // check if object is in new host
        Collection<URI> uris = links.stream()
                .map(link -> UriUtils.buildUri(getDestinationHost(), link))
                .collect(Collectors.toList());
        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
    }

    @Test
    public void successMigrateSameDocumentsTwiceUsingFallback() throws Throwable {
        // disable idempotent post on destination
        for (VerificationHost host : destinationHost.getInProcessHostMap().values()) {
            host.toggleServiceOptions(UriUtils.buildUri(host, ExampleService.FACTORY_LINK),
                    null, EnumSet.of(ServiceOption.IDEMPOTENT_POST));
        }
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);
        migrationState.migrationOptions = EnumSet.of(MigrationOption.DELETE_AFTER);

        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState);
        op = getDestinationHost().getTestRequestSender().sendAndWait(op);
        String migrationTaskLink = op.getBody(State.class).documentSelfLink;

        State waitForServiceCompletion = waitForServiceCompletion(migrationTaskLink,
                getDestinationHost());
        ServiceStats stats = getStats(migrationTaskLink, getDestinationHost());
        Long processedDocuments = Long.valueOf((long) stats.entries.get(
                MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS).latestValue);
        assertEquals(waitForServiceCompletion.taskInfo.stage, TaskStage.FINISHED);
        assertEquals(Long.valueOf(this.serviceCount), processedDocuments);

        // start second migration, which should use the DELETE -> POST logic since the documents already exist
        for (int i = 0; i < this.iterationCount; i++) {
            this.host.log("Start migration with pre-existing target documents (%d)", i);
            migrationState.documentSelfLink = null;
            op = Operation.createPost(this.destinationFactoryUri)
                    .setBody(migrationState);
            op = getDestinationHost().getTestRequestSender().sendAndWait(op);
            migrationTaskLink = op.getBody(State.class).documentSelfLink;
            this.host.log("Created task %s", migrationTaskLink);
            waitForServiceCompletion = waitForServiceCompletion(migrationTaskLink,
                    getDestinationHost());
            assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
            stats = getStats(migrationTaskLink, getDestinationHost());
            processedDocuments = Long.valueOf((long) stats.entries
                    .get(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS).latestValue);
            assertEquals(Long.valueOf(this.serviceCount), processedDocuments);
            stats = getStats(migrationTaskLink, getDestinationHost());
            processedDocuments = Long.valueOf((long) stats.entries
                    .get(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS).latestValue);
        }

        // check if object is in new host
        Collection<URI> uris = links.stream()
                .map(link -> UriUtils.buildUri(getDestinationHost(), link))
                .collect(Collectors.toList());
        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
    }

    @Test
    public void successMigrateTransformedDocuments() throws Throwable {
        runSuccessfulTransformationTest(ExampleTranformationService.class, TRANSFORMATION,
                EnumSet.noneOf(MigrationOption.class), "-transformed", true);
    }

    @Test
    public void successMigrateTransformedDocumentsUsingTransformRequest() throws Throwable {
        runSuccessfulTransformationTest(ExampleTransformationServiceV2.class, TRANSFORMATION_V2,
                EnumSet.of(MigrationOption.USE_TRANSFORM_REQUEST), "-transformed-v2", true);
    }

    @Test
    public void successMigrateTransformResultNoDocuments() throws Throwable {
        runSuccessfulTransformationTest(ExampleTransformationServiceV3.class, TRANSFORMATION_V3,
                EnumSet.of(MigrationOption.USE_TRANSFORM_REQUEST), "-transformed-v3", false);
    }

    private void runSuccessfulTransformationTest(
            Class<? extends StatelessService> transformServiceClass, String transformPath,
            EnumSet<MigrationOption> migrationOptions, String expectedTransformedSuffix, boolean isVerifyMigration)
            throws Throwable {
        // start transformation service
        URI u = UriUtils.buildUri(getDestinationHost(), transformPath);
        Operation post = Operation.createPost(u);
        for (VerificationHost host : destinationHost.getInProcessHostMap().values()) {
            host.startService(post, transformServiceClass.newInstance());
            host.waitForServiceAvailable(transformPath);
        }

        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);
        migrationState.transformationServiceLink = transformPath;
        migrationState.migrationOptions = migrationOptions;

        TestContext ctx = testCreate(1);
        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx);

        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        ServiceStats stats = getStats(out[0], getDestinationHost());
        assertEquals(waitForServiceCompletion.taskInfo.stage, TaskStage.FINISHED);

        if (isVerifyMigration) {
            Long processedDocuments = Long.valueOf((long) stats.entries
                    .get(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS).latestValue);
            assertEquals(Long.valueOf(this.serviceCount), processedDocuments);

            // check if object is in new host and transformed
            Collection<URI> uris = links.stream()
                    .map(link -> UriUtils.buildUri(getDestinationHost(), link))
                    .collect(Collectors.toList());
            getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                    ExampleServiceState.class, uris)
                    .values()
                    .stream()
                    .forEach(state -> {
                        assertTrue(state.name.endsWith(expectedTransformedSuffix));
                    });
        }
    }

    @Test
    public void successTaskRestartedByMaintenance() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);

        Collection<URI> uris = links.stream().map(link -> UriUtils.buildUri(getSourceHost(), link))
                .collect(Collectors.toList());

        List<SimpleEntry<String, Long>> timePerNode = getSourceHost()
                .getServiceState(EnumSet.noneOf(TestProperty.class), ExampleServiceState.class, uris)
                .values()
                .stream()
                .map(d -> new AbstractMap.SimpleEntry<>(d.documentOwner, d.documentUpdateTimeMicros))
                .collect(Collectors.toList());
        Map<String, Long> times = new HashMap<>();
        for (SimpleEntry<String, Long> entry : timePerNode) {
            times.put(entry.getKey(), Math.max(times.getOrDefault(entry.getKey(), 0L), entry.getValue()));
        }
        long time = times.values().stream().mapToLong(i -> i).min().orElse(0);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);
        // start the task as canceled to make sure it does not do anything
        migrationState.taskInfo = TaskState.createAsCancelled();
        migrationState.continuousMigration = Boolean.TRUE;

        getDestinationHost().setMaintenanceIntervalMicros(
                migrationState.maintenanceIntervalMicros / 10);

        TestContext ctx = testCreate(1);
        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx);

        Set<TaskStage> finalStages = new HashSet<>(Arrays.asList(TaskStage.FAILED, TaskStage.FINISHED));
        State finalServiceState = waitForServiceCompletion(out[0], getDestinationHost(), finalStages);
        ServiceStats stats = getStats(out[0], getDestinationHost());

        assertEquals(TaskStage.FINISHED, finalServiceState.taskInfo.stage);
        Long processedDocuments = Long.valueOf((long) stats.entries.get(MigrationTaskService.STAT_NAME_PROCESSED_DOCUMENTS).latestValue);
        Long estimatedTotalServiceCount = Long.valueOf((long) stats.entries.get(MigrationTaskService.STAT_NAME_ESTIMATED_TOTAL_SERVICE_COUNT).latestValue);
        assertTrue(Long.valueOf(this.serviceCount) + " <= " + processedDocuments, Long.valueOf(this.serviceCount) <= processedDocuments);
        assertTrue(Long.valueOf(this.serviceCount) + " <= " + estimatedTotalServiceCount, Long.valueOf(this.serviceCount) <= estimatedTotalServiceCount);
        assertEquals(Long.valueOf(time), finalServiceState.latestSourceUpdateTimeMicros);

        // check if object is in new host
        uris = links.stream().map(link -> UriUtils.buildUri(getDestinationHost(), link))
                .collect(Collectors.toList());
        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
    }

    @Test
    public void failOnSourceNodeFailureBeforeIssuingQuery() throws Throwable {
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);

        migrationState.sourceNodeGroupReference = FAKE_URI;
        migrationState.destinationNodeGroupReference = UriUtils.extendUri(getDestinationHost().getPublicUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);

        TestContext ctx = testCreate(1);
        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx);

        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        assertEquals(TaskStage.FAILED, waitForServiceCompletion.taskInfo.stage);
    }

    @Test
    public void failOnDestinationNodeFailureBeforeIssuingQuery() throws Throwable {
        createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);

        migrationState.sourceNodeGroupReference = UriUtils.extendUri(getSourceHost().getPublicUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);
        migrationState.destinationNodeGroupReference = FAKE_URI;

        TestContext ctx = testCreate(1);
        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.log("Post service error: %s", Utils.toString(e));
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    ctx.completeIteration();
                });
        getDestinationHost().send(op);
        testWait(ctx);

        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        assertEquals(TaskStage.FAILED, waitForServiceCompletion.taskInfo.stage);
    }

    public static class ExampleTransformationServiceV2 extends StatelessService {

        @Override
        public void handlePost(Operation postOperation) {
            MigrationTaskService.TransformRequest request = postOperation.getBody(MigrationTaskService.TransformRequest.class);
            ExampleServiceState state = Utils.fromJson(request.originalDocument, ExampleServiceState.class);
            state.name = state.name + "-transformed-v2";

            MigrationTaskService.TransformResponse response = new MigrationTaskService.TransformResponse();
            response.destinationLinks = new HashMap<>();
            response.destinationLinks.put(Utils.toJson(state), request.destinationLink);
            postOperation.setBody(response).complete();
        }
    }

    /**
     * Transformation service which returns empty documents
     */
    public static class ExampleTransformationServiceV3 extends StatelessService {
        @Override
        public void handlePost(Operation postOperation) {
            MigrationTaskService.TransformResponse response = new MigrationTaskService.TransformResponse();
            response.destinationLinks = new HashMap<>();
            postOperation.setBody(response).complete();
        }
    }

    public static class ExampleTranformationService extends StatelessService {
        @Override
        public void handlePost(Operation postOperation) {
            Map<Object, String> result = new HashMap<>();
            Map<?, ?> body = postOperation.getBody(Map.class);
            for (Map.Entry<?, ?> entry : body.entrySet()) {
                ExampleServiceState state = Utils.fromJson(entry.getKey(), ExampleServiceState.class);
                state.name = state.name + "-transformed";
                result.put(
                        Utils.toJson(state),
                        Utils.fromJson(entry.getValue(), String.class));
            }
            postOperation.setBody(result).complete();
        }
    }

    private State waitForServiceCompletion(String selfLink, VerificationHost host)
            throws Throwable {
        Set<TaskStage> finalStages = new HashSet<>(Arrays.asList(TaskStage.CANCELLED, TaskStage.FAILED,
                TaskStage.FINISHED));
        return waitForServiceCompletion(selfLink, host, finalStages);
    }

    private State waitForServiceCompletion(String selfLink, VerificationHost host, Set<TaskStage> finalStages)
            throws Throwable {
        URI uri = UriUtils.buildUri(host, selfLink);
        State[] currentState = new State[1];
        host.waitFor("waiting for MigrationService To Finish", new WaitHandler() {
            @Override
            public boolean isReady() throws Throwable {
                currentState[0] = host.getServiceState(EnumSet.noneOf(TestProperty.class),
                        MigrationTaskService.State.class, uri);
                return finalStages.contains(currentState[0].taskInfo.stage);
            }
        });
        return currentState[0];
    }

    private Collection<String> createExampleDocuments(URI exampleSourceFactory,
            VerificationHost host, long documentNumber) throws Throwable {
        return createExampleDocuments(exampleSourceFactory, host, documentNumber, true);
    }

    private Collection<String> createExampleDocuments(URI exampleSourceFactory,
            VerificationHost host, long documentNumber, boolean assertOnEmptyFactory)
            throws Throwable {
        if (assertOnEmptyFactory) {
            ServiceDocumentQueryResult r = this.host.getFactoryState(exampleSourceFactory);
            this.host.log("Example collection before creation:%s", Utils.toJsonHtml(r));
            assertTrue(r.documentLinks == null || r.documentLinks.isEmpty());
        }
        Collection<String> links = new ArrayList<>();
        Collection<Operation> ops = new ArrayList<>();
        TestContext ctx = testCreate((int) documentNumber);
        for (; documentNumber > 0; documentNumber--) {
            ExampleServiceState exampleServiceState = new ExampleService.ExampleServiceState();
            exampleServiceState.name = UUID.randomUUID().toString();
            exampleServiceState.documentSelfLink = exampleServiceState.name;
            exampleServiceState.counter = Long.valueOf(documentNumber);
            ops.add(Operation.createPost(exampleSourceFactory)
                    .setBody(exampleServiceState)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.log("Post service error: %s", Utils.toString(e));
                            ctx.failIteration(e);
                            return;
                        }
                        ExampleServiceState st = o.getBody(ExampleServiceState.class);
                        this.host.log("Created %s on %s", st.documentSelfLink, st.documentOwner);
                        synchronized (links) {
                            links.add(st.documentSelfLink);
                        }
                        ctx.completeIteration();
                    }));
        }
        ops.stream().forEach(op -> host.send(op));
        testWait(ctx);
        return links;
    }

    private void startMigrationService(VerificationHost host) throws Throwable {
        URI u = UriUtils.buildUri(host, MigrationTaskService.FACTORY_LINK);
        Operation post = Operation.createPost(u);
        host.startService(post, MigrationTaskService.createFactory());
        host.waitForServiceAvailable(MigrationTaskService.FACTORY_LINK);
    }

    private ServiceStats getStats(String documentLink, VerificationHost host) throws Throwable {
        TestContext ctx = testCreate(1);
        ServiceStats[] out = new ServiceStats[1];
        Operation op = Operation.createGet(UriUtils.buildUri(host, documentLink + "/stats"))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(ServiceStats.class);
                    ctx.completeIteration();
                });
        host.send(op);
        testWait(ctx);
        return out[0];
    }

    private void checkReusableHostAndCleanup(VerificationHost host) throws Throwable {
        if (host.isStopping() || !host.isStarted()) {
            host.start();
            startMigrationService(host);
            host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
            host.waitForServiceAvailable(MigrationTaskService.FACTORY_LINK);
        }
        host.deleteAllChildServices(UriUtils.buildUri(host, MigrationTaskService.FACTORY_LINK));
        host.deleteAllChildServices(UriUtils.buildUri(host, ExampleService.FACTORY_LINK));
    }
}
