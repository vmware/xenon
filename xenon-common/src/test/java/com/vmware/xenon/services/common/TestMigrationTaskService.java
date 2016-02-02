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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MigrationTaskService.State;
import com.vmware.xenon.services.common.QueryTask.NumericRange;

public class TestMigrationTaskService extends BasicReusableHostTestCase {
    private static final String TRANSFORMATION = "transformation";
    private URI sourceFactoryUri;
    private URI destinationFactoryUri;
    private URI exampleSourceFactory;
    private URI exampleDestinationFactory;

    private VerificationHost destinationHost;

    @Before
    public void setUp() throws Throwable {
        this.destinationHost = VerificationHost.create(0);
        this.destinationHost.start();
        startMigrationService(this.destinationHost);
        startMigrationService(this.host);

        this.sourceFactoryUri = UriUtils.buildUri(this.host, MigrationTaskFactoryService.SELF_LINK);
        this.destinationFactoryUri = UriUtils.buildUri(this.destinationHost,
                MigrationTaskFactoryService.SELF_LINK);

        this.exampleSourceFactory = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK);
        this.exampleDestinationFactory = UriUtils.buildUri(this.destinationHost,
                ExampleFactoryService.SELF_LINK);
    }

    @After
    public void cleanUp() throws Throwable {
        this.host.deleteAllChildServices(this.sourceFactoryUri);
        this.host.deleteAllChildServices(this.exampleSourceFactory);
        this.destinationHost.deleteAllChildServices(this.destinationFactoryUri);
        this.destinationHost.deleteAllChildServices(this.exampleDestinationFactory);
        this.destinationHost.stop();
    }

    State validMigrationState() throws Throwable {
        return validMigrationState("");
    }

    State validMigrationState(String factory) throws Throwable {
        State state = new State();
        state.destinationFactory = factory;
        state.destinationNode = this.destinationHost.getPublicUri();
        state.sourceFactory = factory;
        state.sourceNode = this.host.getPublicUri();
        return state;
    }

    @Test
    public void testFactoryPost() throws Throwable {
        State state = validMigrationState();
        final State[] outState = new State[1];

        Operation op = Operation.createPost(this.sourceFactoryUri)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    outState[0] = o.getBody(State.class);
                    this.host.completeIteration();
                });

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();

        assertNotNull(outState[0]);
        assertEquals(outState[0].changedSinceEpocMillis, new Long(0));
        assertEquals(outState[0].destinationFactory, state.destinationFactory);
        assertEquals(outState[0].destinationNode, state.destinationNode);
        assertEquals(outState[0].sourceFactory, state.sourceFactory);
        assertEquals(outState[0].sourceNode, state.sourceNode);
    }

    @Test
    public void testMigration() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, this.host, 10);
        Collection<URI> uris = links.stream().map(link -> UriUtils.buildUri(this.host, link))
                .collect(Collectors.toList());
        long time = this.destinationHost
                .getServiceState(EnumSet.noneOf(TestProperty.class), ExampleServiceState.class,
                        uris)
                .values()
                .stream()
                .mapToLong(d -> d.documentUpdateTimeMicros)
                .max().orElse(0);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState();
        migrationState.sourceNode = this.host.getPublicUri();
        migrationState.sourceFactory = ExampleFactoryService.SELF_LINK;
        migrationState.destinationNode = this.destinationHost.getPublicUri();
        migrationState.destinationFactory = ExampleFactoryService.SELF_LINK;

        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.destinationHost.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(op);
        this.destinationHost.testWait();
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], this.destinationHost);
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(10), waitForServiceCompletion.processedEntities);
        assertEquals(Long.valueOf(time), waitForServiceCompletion.lastDocumentChangedEpoc);

        // check if object is in new host
        uris = links.stream().map(link -> UriUtils.buildUri(this.destinationHost, link))
                .collect(Collectors.toList());
        this.destinationHost.getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
    }

    @Test
    public void testMigration_noEntities() throws Throwable {
        // start migration
        MigrationTaskService.State migrationState = validMigrationState();
        migrationState.sourceNode = this.host.getPublicUri();
        migrationState.sourceFactory = ExampleFactoryService.SELF_LINK;
        migrationState.destinationNode = this.destinationHost.getPublicUri();
        migrationState.destinationFactory = ExampleFactoryService.SELF_LINK;

        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.destinationHost.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(op);
        this.destinationHost.testWait();
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], this.destinationHost);
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(0), waitForServiceCompletion.processedEntities);
    }

    @Test
    public void testMigratingObjectsAfterTime_nothingFound() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, this.host, 10);
        Collection<URI> uris = links.stream().map(link -> UriUtils.buildUri(this.host, link))
                .collect(Collectors.toList());
        long time = this.destinationHost
                .getServiceState(EnumSet.noneOf(TestProperty.class), ExampleServiceState.class,
                        uris)
                .values()
                .stream()
                .mapToLong(d -> d.documentUpdateTimeMicros)
                .max().orElse(0);
        assertTrue("max upateTime should not be 0", time > 0);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState();
        migrationState.sourceNode = this.host.getPublicUri();
        migrationState.sourceFactory = ExampleFactoryService.SELF_LINK;
        migrationState.destinationNode = this.destinationHost.getPublicUri();
        migrationState.destinationFactory = ExampleFactoryService.SELF_LINK;
        migrationState.changedSinceEpocMillis = time;

        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.destinationHost.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(op);
        this.destinationHost.testWait();
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], this.destinationHost);
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(0), waitForServiceCompletion.processedEntities);

        // check that objects were not migrated
        long[] out_long = new long[1];
        Operation get = Operation.createGet(this.exampleDestinationFactory)
                .setCompletion((o, e) -> {
                    out_long[0] = o.getBody(ServiceDocumentQueryResult.class).documentCount;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(get);
        this.destinationHost.testWait();
        assertEquals(0, out_long[0]);
    }

    @Test
    public void testMigratingMultiPage() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, this.host, 10);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState();
        migrationState.sourceNode = this.host.getPublicUri();
        migrationState.sourceFactory = ExampleFactoryService.SELF_LINK;
        migrationState.destinationNode = this.destinationHost.getPublicUri();
        migrationState.destinationFactory = ExampleFactoryService.SELF_LINK;
        migrationState.resultLimit = 1;

        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.destinationHost.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(op);
        this.destinationHost.testWait();
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], this.destinationHost);
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(10), waitForServiceCompletion.processedEntities);

        // check if object is in new host
        Collection<URI> uris = links.stream()
                .map(link -> UriUtils.buildUri(this.destinationHost, link))
                .collect(Collectors.toList());
        this.destinationHost.getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
    }

    @Test
    public void testMirgrationObjectsAfterTime() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, this.host, 10);
        Collection<URI> uris = links.stream().map(link -> UriUtils.buildUri(this.host, link))
                .collect(Collectors.toList());
        long time = this.destinationHost
                .getServiceState(EnumSet.noneOf(TestProperty.class), ExampleServiceState.class,
                        uris)
                .values()
                .stream()
                .mapToLong(d -> d.documentUpdateTimeMicros)
                .max().orElse(0);
        assertTrue("max upateTime should not be 0", time > 0);

        Collection<String> newLinks = createExampleDocuments(this.exampleSourceFactory, this.host,
                10);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState();
        migrationState.sourceNode = this.host.getPublicUri();
        migrationState.sourceFactory = ExampleFactoryService.SELF_LINK;
        migrationState.destinationNode = this.destinationHost.getPublicUri();
        migrationState.destinationFactory = ExampleFactoryService.SELF_LINK;
        migrationState.changedSinceEpocMillis = time;

        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.destinationHost.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(op);
        this.destinationHost.testWait();
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], this.destinationHost);
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);
        assertEquals(Long.valueOf(10), waitForServiceCompletion.processedEntities);

        // check if object is in new host
        uris = newLinks.stream().map(link -> UriUtils.buildUri(this.destinationHost, link))
                .collect(Collectors.toList());
        this.destinationHost.getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);

        // check that objects were not migrated
        long[] out_long = new long[1];
        Operation get = Operation.createGet(this.exampleDestinationFactory)
                .setCompletion((o, e) -> {
                    out_long[0] = o.getBody(ServiceDocumentQueryResult.class).documentCount;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(get);
        this.destinationHost.testWait();
        assertEquals(newLinks.size(), out_long[0]);
    }

    @Test
    public void testMigration_MigrateTwice() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, this.host, 10);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState();
        migrationState.sourceNode = this.host.getPublicUri();
        migrationState.sourceFactory = ExampleFactoryService.SELF_LINK;
        migrationState.destinationNode = this.destinationHost.getPublicUri();
        migrationState.destinationFactory = ExampleFactoryService.SELF_LINK;

        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.destinationHost.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(op);
        this.destinationHost.testWait();
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], this.destinationHost);
        assertEquals(waitForServiceCompletion.taskInfo.stage, TaskStage.FINISHED);
        assertEquals(Long.valueOf(10), waitForServiceCompletion.processedEntities);

        this.destinationHost.testStart(1);
        this.destinationHost.send(op);
        this.destinationHost.testWait();
        waitForServiceCompletion = waitForServiceCompletion(out[0], this.destinationHost);
        assertEquals(waitForServiceCompletion.taskInfo.stage, TaskStage.FINISHED);
        assertEquals(Long.valueOf(10), waitForServiceCompletion.processedEntities);

        // check if object is in new host
        Collection<URI> uris = links.stream()
                .map(link -> UriUtils.buildUri(this.destinationHost, link))
                .collect(Collectors.toList());
        this.destinationHost.getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);
    }

    @Test
    public void testMigration_transformation() throws Throwable {
        // start transformation service
        URI u = UriUtils.buildUri(this.destinationHost, TRANSFORMATION);
        Operation post = Operation.createPost(u);
        this.destinationHost.startService(post, new ExampleTranformationService());
        this.destinationHost.waitForServiceAvailable(TRANSFORMATION);

        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, this.host, 10);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState();
        migrationState.sourceNode = this.host.getPublicUri();
        migrationState.sourceFactory = ExampleFactoryService.SELF_LINK;
        migrationState.destinationNode = this.destinationHost.getPublicUri();
        migrationState.destinationFactory = ExampleFactoryService.SELF_LINK;
        migrationState.transformationService = TRANSFORMATION;

        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.destinationHost.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(op);
        this.destinationHost.testWait();

        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], this.destinationHost);
        assertEquals(waitForServiceCompletion.taskInfo.stage, TaskStage.FINISHED);
        assertEquals(Long.valueOf(10), waitForServiceCompletion.processedEntities);

        // check if object is in new host and transformed
        Collection<URI> uris = links.stream()
                .map(link -> UriUtils.buildUri(this.destinationHost, link))
                .collect(Collectors.toList());
        this.destinationHost.getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris)
                .values()
                .stream()
                .forEach(state -> {
                    assertTrue(state.name.endsWith("-transformed"));
                });
        ;
    }

    public static class ExampleTranformationService extends StatelessService {
        @Override
        public void handlePost(Operation postOperation) {
            ExampleServiceState state = postOperation.getBody(ExampleServiceState.class);
            state.name = state.name + "-transformed";
            postOperation.setBody(state).complete();
        }
    }

    @Test
    public void testMigration_customQueryTerms() throws Throwable {
        // create object in host
        Collection<String> links = createExampleDocuments(this.exampleSourceFactory, this.host, 10);
        Collection<URI> uris = links.stream().map(link -> UriUtils.buildUri(this.host, link))
                .collect(Collectors.toList());
        long time = this.destinationHost
                .getServiceState(EnumSet.noneOf(TestProperty.class), ExampleServiceState.class,
                        uris)
                .values()
                .stream()
                .mapToLong(d -> d.documentUpdateTimeMicros)
                .max().orElse(0);
        assertTrue("max upateTime should not be 0", time > 0);
        QueryTask.Query customTerm = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS)
                .setNumericRange(NumericRange.createGreaterThanRange(time));

        Collection<String> newLinks = createExampleDocuments(this.exampleSourceFactory, this.host,
                10);

        // start migration
        MigrationTaskService.State migrationState = validMigrationState();
        migrationState.sourceNode = this.host.getPublicUri();
        migrationState.sourceFactory = ExampleFactoryService.SELF_LINK;
        migrationState.destinationNode = this.destinationHost.getPublicUri();
        migrationState.destinationFactory = ExampleFactoryService.SELF_LINK;
        migrationState.additionalQueryTerms = new ArrayList<>(Arrays.asList(customTerm));

        String[] out = new String[1];
        Operation op = Operation.createPost(this.destinationFactoryUri)
                .setBody(migrationState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.destinationHost.failIteration(e);
                        return;
                    }
                    out[0] = o.getBody(State.class).documentSelfLink;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(op);
        this.destinationHost.testWait();
        // wait until migration finished
        State waitForServiceCompletion = waitForServiceCompletion(out[0], this.destinationHost);
        assertEquals(TaskStage.FINISHED, waitForServiceCompletion.taskInfo.stage);

        // check if object is in new host
        uris = newLinks.stream().map(link -> UriUtils.buildUri(this.destinationHost, link))
                .collect(Collectors.toList());
        this.destinationHost.getServiceState(EnumSet.noneOf(TestProperty.class),
                ExampleServiceState.class, uris);

        // check that objects were not migrated
        long[] out_long = new long[1];
        Operation get = Operation.createGet(this.exampleDestinationFactory)
                .setCompletion((o, e) -> {
                    out_long[0] = o.getBody(ServiceDocumentQueryResult.class).documentCount;
                    this.destinationHost.completeIteration();
                });
        this.destinationHost.testStart(1);
        this.destinationHost.send(get);
        this.destinationHost.testWait();
        assertEquals(newLinks.size(), out_long[0]);
    }

    private State waitForServiceCompletion(String selfLink, VerificationHost host)
            throws Throwable {
        List<TaskStage> finalStates = Arrays.asList(TaskStage.CANCELLED, TaskStage.FAILED,
                TaskStage.FINISHED);
        URI uri = UriUtils.buildUri(host, selfLink);
        Date expiration = host.getTestExpiration();
        State currentState;
        do {
            currentState = host.getServiceState(EnumSet.noneOf(TestProperty.class),
                    MigrationTaskService.State.class, uri);
            if (finalStates.contains(currentState.taskInfo.stage)) {
                break;
            }
            Date now = new Date();
            if (now.after(expiration)) {
                throw new TimeoutException("Task did not complete in time");
            }
            Thread.sleep(100);
        } while (true);

        return currentState;
    }

    private Collection<String> createExampleDocuments(URI exampleSourceFactory,
            VerificationHost host, int documentNumber) throws Throwable {
        Collection<String> links = new ArrayList<>();
        ExampleServiceState exampleServiceState = new ExampleService.ExampleServiceState();
        exampleServiceState.name = "name";
        Collection<Operation> ops = new ArrayList<>();
        for (; documentNumber > 0; documentNumber--) {
            exampleServiceState.counter = Long.valueOf(documentNumber);
            ops.add(Operation.createPost(exampleSourceFactory)
                    .setBody(exampleServiceState)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            host.failIteration(e);
                            return;
                        }
                        synchronized (ops) {
                            links.add(o.getBody(
                                    ExampleService.ExampleServiceState.class).documentSelfLink);
                        }
                        host.completeIteration();
                    }));
        }
        host.testStart(ops.size());
        ops.stream().forEach(op -> host.send(op));
        host.testWait();
        return links;
    }

    private void startMigrationService(VerificationHost host) throws Throwable {
        URI u = UriUtils.buildUri(host, MigrationTaskFactoryService.SELF_LINK);
        Operation post = Operation.createPost(u);
        host.startService(post, new MigrationTaskFactoryService());
        host.waitForServiceAvailable(MigrationTaskFactoryService.SELF_LINK);
    }
}
