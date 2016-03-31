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
import org.junit.Ignore;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
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
import com.vmware.xenon.services.common.MigrationTaskService.State;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;

@Ignore("https://www.pivotaltracker.com/story/show/116498189")
public class TestMigrationTaskService extends BasicReusableHostTestCase {
    private static final int UNACCESSABLE_PORT = 123;
    private static final URI FAKE_URI = UriUtils.buildUri("127.0.0.1", UNACCESSABLE_PORT, null, null);
    private static final String TRANSFORMATION = "transformation";
    private URI sourceFactoryUri;
    private URI destinationFactoryUri;
    private URI exampleSourceFactory;
    private URI exampleDestinationFactory;

    private static VerificationHost destinationHost;

    public long serviceCount = 10;
    private int nodeCount = 2;

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
        }

        this.sourceFactoryUri = UriUtils.buildUri(getSourceHost(), MigrationTaskService.FACTORY_LINK);
        this.destinationFactoryUri = UriUtils.buildUri(getDestinationHost(),
                MigrationTaskService.FACTORY_LINK);

        this.exampleSourceFactory = UriUtils.buildUri(getSourceHost(), ExampleService.FACTORY_LINK);
        this.exampleDestinationFactory = UriUtils.buildUri(getDestinationHost(),
                ExampleService.FACTORY_LINK);

        this.host.waitForReplicatedFactoryServiceAvailable(this.destinationFactoryUri);
        this.host.waitForReplicatedFactoryServiceAvailable(this.sourceFactoryUri);
        this.host.waitForReplicatedFactoryServiceAvailable(this.exampleSourceFactory);
        this.host.waitForReplicatedFactoryServiceAvailable(this.exampleDestinationFactory);
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
            host.deleteAllChildServices(UriUtils.buildUri(host, MigrationTaskService.FACTORY_LINK));
            host.deleteAllChildServices(UriUtils.buildUri(host, ExampleService.FACTORY_LINK));
        }

        for (VerificationHost host : destinationHost.getInProcessHostMap().values()) {
            host.deleteAllChildServices(UriUtils.buildUri(host, MigrationTaskService.FACTORY_LINK));
            host.deleteAllChildServices(UriUtils.buildUri(host, ExampleService.FACTORY_LINK));
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

    State validMigrationState() throws Throwable {
        return validMigrationState("");
    }

    State validMigrationState(String factory) throws Throwable {
        State state = new State();
        state.destinationFactoryLink = factory;
        state.destinationNodeGroupReference
            = UriUtils.buildUri(getDestinationHost().getPublicUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);
        state.sourceFactoryLink = factory;
        state.sourceNodeGroupReference
            = UriUtils.buildUri(getSourceHost().getPublicUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);
        state.maintenanceIntervalMicros = TimeUnit.MILLISECONDS.toMicros(10);
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
        // wait until migration finished
        State finalServiceState = waitForServiceCompletion(out[0], getDestinationHost());

        assertEquals(TaskStage.FINISHED, finalServiceState.taskInfo.stage);
        assertEquals(Long.valueOf(this.serviceCount), finalServiceState.processedServiceCount);
        assertEquals(Long.valueOf(this.serviceCount), finalServiceState.estimatedTotalServiceCount);
        assertEquals(Long.valueOf(time), finalServiceState.latestSourceUpdateTimeMicros);

        // check if object is in new host
        uris = links.stream().map(link -> UriUtils.buildUri(getDestinationHost(), link))
                .collect(Collectors.toList());
        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
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
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(0), waitForServiceCompletion.processedServiceCount);
    }

    @Test
    public void successNoDcumentsModifiedAfterTime() throws Throwable {
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
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(0), waitForServiceCompletion.processedServiceCount);

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
        spec.resultLimit = 1;
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
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(this.serviceCount), waitForServiceCompletion.processedServiceCount);

        // check if object is in new host
        Collection<URI> uris = links.stream()
                .map(link -> UriUtils.buildUri(getDestinationHost(), link))
                .collect(Collectors.toList());
        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
    }

    @Test
    public void sucessMigrateOnlyDocumentsUpdatedAfterTime() throws Throwable {
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
                .mapToLong(d -> d.documentUpdateTimeMicros)
                .max().orElse(0);
        assertTrue("max upateTime should not be 0", time > 0);

        Collection<String> newLinks = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);

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
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertTrue(Long.valueOf(this.serviceCount) + " <= " + waitForServiceCompletion.processedServiceCount, Long.valueOf(this.serviceCount) <= waitForServiceCompletion.processedServiceCount);

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
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(this.serviceCount), waitForServiceCompletion.processedServiceCount);

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
        assertEquals(waitForServiceCompletion.taskInfo.stage, TaskStage.FINISHED);
        assertEquals(Long.valueOf(this.serviceCount), waitForServiceCompletion.processedServiceCount);

        // check if object is in new host
        Collection<URI> uris = links.stream()
                .map(link -> UriUtils.buildUri(getDestinationHost(), link))
                .collect(Collectors.toList());
        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
    }

    @Test
    public void successMigrateTransformedDocuments() throws Throwable {
        // start transformation service
        URI u = UriUtils.buildUri(getDestinationHost(), TRANSFORMATION);
        Operation post = Operation.createPost(u);
        for (VerificationHost host : destinationHost.getInProcessHostMap().values()) {
            host.startService(post, new ExampleTranformationService());
            host.waitForServiceAvailable(TRANSFORMATION);
        }

        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);
        migrationState.transformationServiceLink = TRANSFORMATION;

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

        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], getDestinationHost());
        assertEquals(waitForServiceCompletion.taskInfo.stage, TaskStage.FINISHED);
        assertEquals(Long.valueOf(this.serviceCount), waitForServiceCompletion.processedServiceCount);

        // check if object is in new host and transformed
        Collection<URI> uris = links.stream()
                .map(link -> UriUtils.buildUri(getDestinationHost(), link))
                .collect(Collectors.toList());
        getDestinationHost().getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris)
                .values()
                .stream()
                .forEach(state -> {
                    assertTrue(state.name.endsWith("-transformed"));
                });
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

        getDestinationHost().setMaintenanceIntervalMicros(migrationState.maintenanceIntervalMicros / 10);

        // wait until migration finished
        Set<TaskStage> finalStages = new HashSet<>(Arrays.asList(TaskStage.FAILED, TaskStage.FINISHED));
        State finalServiceState = waitForServiceCompletion(out[0], getDestinationHost(), finalStages);

        assertEquals(TaskStage.FINISHED, finalServiceState.taskInfo.stage);
        assertTrue(Long.valueOf(this.serviceCount) + " <= " + finalServiceState.processedServiceCount, Long.valueOf(this.serviceCount) <= finalServiceState.processedServiceCount);
        assertTrue(Long.valueOf(this.serviceCount) + " <= " + finalServiceState.estimatedTotalServiceCount, Long.valueOf(this.serviceCount) <= finalServiceState.estimatedTotalServiceCount);
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

        migrationState.resolvedSourceNodeGroupReferences
            = getHostUris(this.host.getInProcessHostMap().values());
        migrationState.resolvedSourceNodeGroupReferences.add(FAKE_URI);
        migrationState.resolvedDestinationNodeGroupReferences
            = getHostUris(destinationHost.getInProcessHostMap().values());

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
    public void failOnSourceNodeFailureDuringResultRetrieval() throws Throwable {
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);

        migrationState.resolvedSourceNodeGroupReferences
            = getHostUris(this.host.getInProcessHostMap().values());
        migrationState.resolvedDestinationNodeGroupReferences
            = getHostUris(destinationHost.getInProcessHostMap().values());
        migrationState.currentPageLinks = new ArrayList<>();
        migrationState.currentPageLinks.add(FAKE_URI);

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

        migrationState.resolvedSourceNodeGroupReferences
            = getHostUris(this.host.getInProcessHostMap().values());
        migrationState.resolvedDestinationNodeGroupReferences
            = new ArrayList<>();
        migrationState.resolvedDestinationNodeGroupReferences.add(FAKE_URI);

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
    public void failOnDestinationNodeFailureDuringResultPosting() throws Throwable {
        createExampleDocuments(this.exampleSourceFactory, getSourceHost(),
                this.serviceCount);
        MigrationTaskService.State migrationState = validMigrationState(
                ExampleService.FACTORY_LINK);

        migrationState.resolvedSourceNodeGroupReferences
            = getHostUris(this.host.getInProcessHostMap().values());
        migrationState.resolvedDestinationNodeGroupReferences = new ArrayList<>();
        migrationState.resolvedDestinationNodeGroupReferences.add(FAKE_URI);
        migrationState.currentPageLinks = computeFirstCurrentPageLink(this.host.getInProcessHostMap().values());

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

    private List<URI> getHostUris(Collection<VerificationHost> hosts) {
        return hosts.stream()
                .map(h -> h.getUri())
                .collect(Collectors.toList());
    }

    private List<URI> computeFirstCurrentPageLink(Collection<VerificationHost> hosts) throws Throwable {
        Query query = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, ExampleService.FACTORY_LINK + "*",
                        QueryTask.QueryTerm.MatchType.WILDCARD)
                .build();
        QuerySpecification spec = new QuerySpecification();
        spec.resultLimit = 1;
        spec.query = query;
        spec.options.add(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);

        QueryTask queryTask = QueryTask.create(spec).setDirect(true);
        List<URI> uris = new ArrayList<>();
        TestContext ctx = testCreate(hosts.size());
        for (VerificationHost host : hosts) {
            Operation post = Operation.createPost(UriUtils.buildUri(host.getUri(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS))
                    .setBody(queryTask)
                    .setCompletion((o, t) -> {
                        if (t != null) {
                            this.host.log("Post service error: %s", Utils.toString(t));
                            ctx.failIteration(t);
                            return;
                        }
                        QueryTask task = o.getBody(QueryTask.class);
                        uris.add(UriUtils.buildUri(host, task.results.nextPageLink));
                        ctx.completeIteration();
                    });
            host.send(post);
        }
        testWait(ctx);
        return uris;
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
                        synchronized (ops) {
                            links.add(o.getBody(
                                    ExampleService.ExampleServiceState.class).documentSelfLink);
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
}
