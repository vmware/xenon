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

package com.vmware.xenon.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MinimalFactoryTestService;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.ServiceUriPaths;

class TypeMismatchTestFactoryService extends FactoryService {

    public TypeMismatchTestFactoryService() {
        super(ExampleServiceState.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        // intentionally create a child service with a different state type than the one we declare
        // in our constructor, for a negative test
        Service s = new MinimalTestService();
        return s;
    }
}

class SynchTestFactoryService extends FactoryService {
    private static final int MAINTENANCE_DELAY_HANDLE_MICROS = 50;
    public static final String TEST_FACTORY_PATH = "/subpath/testfactory";
    public static final String TEST_SERVICE_PATH = TEST_FACTORY_PATH + "/instanceX";

    public static final String SELF_LINK = TEST_FACTORY_PATH;

    private Runnable pendingTask;

    public void setTaskToRunOnNextMaintenance(Runnable r) {
        synchronized (this.options) {
            this.pendingTask = r;
        }
    }

    SynchTestFactoryService() {
        super(ExampleServiceState.class);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new ExampleService();
    }

    @Override
    public void handleNodeGroupMaintenance(Operation post) {
        Runnable task = null;
        // use a local instance field to make sure the task is reset atomically
        synchronized (this.options) {
            if (this.pendingTask != null) {
                task = this.pendingTask;
                this.pendingTask = null;
            }
        }

        if (task != null) {
            getHost().schedule(task, MAINTENANCE_DELAY_HANDLE_MICROS,
                    TimeUnit.MICROSECONDS);
        }
        super.handleNodeGroupMaintenance(post);
    }
}

public class TestFactoryService extends BasicReusableHostTestCase {

    public static final String FAC_PATH = "/subpath/fff";

    public int hostRestartCount = 10;

    private URI factoryUri;

    private SynchTestFactoryService factoryService;

    @Before
    public void setup() throws Throwable {
        this.factoryUri = UriUtils.buildUri(this.host, SomeFactoryService.class);
        CommandLineArgumentParser.parseFromProperties(this);
    }

    /**
    * This tests a very tricky scenario:
    * Running a node group maintenance of factory service with REPLICATION and PERSISTENCE.
    * When running maintenance, and at the same time a child service was deleted and short after created with API's DELETE and POST,
    * the maintenance will try to re-create the deleted (and stopped) child service.
    * Test verifies that with such race condition no issues should happen.
    */
    @Test
    public void synchronizationWithIdempotentPostAndDelete() throws Throwable {
        for (int i = 0; i < this.hostRestartCount; i++) {
            this.host.log("iteration %s", i);
            createHostAndServicePostDeletePost();
        }
    }

    private void createHostAndServicePostDeletePost() throws Throwable {
        TemporaryFolder tmp = new TemporaryFolder();
        tmp.create();
        ServiceHost.Arguments args = new ServiceHost.Arguments();
        args.port = 0;
        args.sandbox = tmp.getRoot().toPath();
        VerificationHost h = VerificationHost.create(args);
        try {
            h.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(50));
            h.setTemporaryFolder(tmp);
            // we will kick of synchronization to avoid a race: if maintenance handler is called
            // before we set the task below, the test will timeout/hang
            h.setPeerSynchronizationEnabled(false);
            h.start();

            this.factoryUri = UriUtils.buildUri(h, SynchTestFactoryService.class);
            this.factoryService = startSynchFactoryService(h);

            ExampleServiceState doc = new ExampleServiceState();
            doc.documentSelfLink = SynchTestFactoryService.TEST_SERVICE_PATH;
            doc.name = doc.documentSelfLink;
            TestContext ctx = testCreate(1);
            doPost(h, doc, (e) -> {
                if (e != null) {
                    ctx.failIteration(e);
                    return;
                }
                this.factoryService.setTaskToRunOnNextMaintenance(() -> {
                    doDelete(h, doc.documentSelfLink, (e1) -> {

                        if (e1 != null) {
                            ctx.failIteration(e1);
                            return;
                        }

                        doPost(h, doc, (e2) -> {
                            if (e2 != null) {
                                ctx.failIteration(e2);
                                return;
                            }
                            ctx.completeIteration();
                        });
                    });
                });
                // trigger maintenance
                h.scheduleNodeGroupChangeMaintenance(ServiceUriPaths.DEFAULT_NODE_SELECTOR);
            });

            testWait(ctx);
        } finally {
            h.tearDown();
        }
    }

    private void doPost(VerificationHost h, ExampleServiceState doc, Consumer<Throwable> callback) {
        h.send(Operation
                .createPost(this.factoryUri)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                .setBody(doc)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        callback.accept(e);
                    } else {
                        callback.accept(null);
                    }
                }));
    }

    private void doDelete(VerificationHost h, String documentSelfLink, Consumer<Throwable> callback) {
        this.host.send(Operation.createDelete(
                UriUtils.buildUri(h, documentSelfLink))
                .setBody(new ExampleServiceState())
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                callback.accept(e);
                            } else {
                                callback.accept(null);
                            }
                        }));
    }

    private SynchTestFactoryService startSynchFactoryService(VerificationHost h) throws Throwable {
        SynchTestFactoryService factoryService = new SynchTestFactoryService();

        h.startService(
                Operation.createPost(this.factoryUri), factoryService);
        h.waitForServiceAvailable(SynchTestFactoryService.SELF_LINK);

        return factoryService;
    }

    @Test
    public void factoryWithChildServiceStateTypeMismatch() {
        this.host.toggleNegativeTestMode(true);
        Operation post = Operation
                .createPost(UriUtils.buildUri(this.host, UUID.randomUUID().toString()))
                .setCompletion(this.host.getExpectedFailureCompletion());
        this.host.startService(post, new TypeMismatchTestFactoryService());
        this.host.toggleNegativeTestMode(false);
    }

    @Test
    public void factoryClonePostExpectFailure() throws Throwable {
        MinimalFactoryTestService f = new MinimalFactoryTestService();
        MinimalFactoryTestService factoryService = (MinimalFactoryTestService) this.host
                .startServiceAndWait(f, UUID.randomUUID().toString(), null);

        // create a child service
        MinimalTestServiceState initState = (MinimalTestServiceState) this.host
                .buildMinimalTestState();
        initState.documentSelfLink = UUID.randomUUID().toString();

        this.host.testStart(1);
        this.host.send(Operation.createPost(factoryService.getUri())
                .setBody(initState)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();

        ServiceDocumentQueryResult rsp = this.host.getFactoryState(factoryService.getUri());

        // create a clone POST, by setting the source link
        initState = new MinimalTestServiceState();
        initState.documentSelfLink = UUID.randomUUID().toString();
        initState.documentSourceLink = rsp.documentLinks.iterator().next();

        // we expect this to fail since the minimal factory service does not support clone
        this.host.testStart(1);
        this.host.send(Operation.createPost(factoryService.getUri())
                .setBody(initState)
                .setCompletion(this.host.getExpectedFailureCompletion()));
        this.host.testWait();
    }

    @Ignore("https://www.pivotaltracker.com/story/show/116217439")
    @Test
    public void factoryDurableServicePostWithDeleteRestart() throws Throwable {
        // first create the factory service
        long count = this.serviceCount;
        MinimalFactoryTestService f = new MinimalFactoryTestService();
        f.setChildServiceCaps(EnumSet.of(ServiceOption.PERSISTENCE));
        MinimalFactoryTestService factoryService = (MinimalFactoryTestService) this.host
                .startServiceAndWait(f, UUID.randomUUID().toString(), null);

        doFactoryServiceChildCreation(EnumSet.of(ServiceOption.PERSISTENCE),
                EnumSet.of(TestProperty.DELETE_DURABLE_SERVICE), count,
                factoryService.getUri());
        // do one more pass to verify the previous services, even if durable,
        // have their documents marked deleted in the index
        doFactoryServiceChildCreation(EnumSet.of(ServiceOption.PERSISTENCE),
                EnumSet.of(TestProperty.DELETE_DURABLE_SERVICE), count,
                factoryService.getUri());

        // do it all again, but with durable, replicated services
        f = new MinimalFactoryTestService();
        EnumSet<ServiceOption> caps = EnumSet.of(ServiceOption.PERSISTENCE,
                ServiceOption.REPLICATION);
        f.setChildServiceCaps(caps);
        factoryService = (MinimalFactoryTestService) this.host
                .startServiceAndWait(f, UUID.randomUUID().toString(), null);

        doFactoryServiceChildCreation(caps,
                EnumSet.of(TestProperty.DELETE_DURABLE_SERVICE), count,
                factoryService.getUri());
        // do one more pass to verify the previous services, even if durable,
        // have their documents marked deleted in the index
        doFactoryServiceChildCreation(caps,
                EnumSet.of(TestProperty.DELETE_DURABLE_SERVICE), count,
                factoryService.getUri());
    }

    @Ignore("https://www.pivotaltracker.com/story/show/116217439")
    @Test
    public void factoryDurableServicePostNoCaching()
            throws Throwable {

        // disable caching. This makes everything a lot slower, but verifies the
        // index returns the most up to date state, after each update operation
        this.host.setServiceStateCaching(false);

        long count = this.host.isStressTest() ? 1000 : 10;
        MinimalFactoryTestService f = new MinimalFactoryTestService();
        // set a custom load query limit
        f.setSelfQueryResultLimit(FactoryService.SELF_QUERY_RESULT_LIMIT / 10);
        assertEquals(FactoryService.SELF_QUERY_RESULT_LIMIT / 10, f.getSelfQueryResultLimit());
        f.toggleOption(ServiceOption.PERSISTENCE, true);

        MinimalFactoryTestService factoryService = (MinimalFactoryTestService) this.host
                .startServiceAndWait(f, UUID.randomUUID().toString(), null);

        factoryService.setChildServiceCaps(EnumSet.of(ServiceOption.PERSISTENCE));
        doFactoryServiceChildCreation(EnumSet.of(ServiceOption.PERSISTENCE),
                EnumSet.of(TestProperty.DELETE_DURABLE_SERVICE), count,
                factoryService.getUri());
    }

    private void doFactoryServiceChildCreation(long count, URI factoryUri)
            throws Throwable {
        doFactoryServiceChildCreation(EnumSet.noneOf(ServiceOption.class),
                EnumSet.noneOf(TestProperty.class), count, factoryUri);
    }

    private void doFactoryServiceChildCreation(EnumSet<TestProperty> props,
            long count, URI factoryUri) throws Throwable {
        doFactoryServiceChildCreation(EnumSet.noneOf(ServiceOption.class), props,
                count, factoryUri);
    }

    private void doFactoryServiceChildCreation(EnumSet<ServiceOption> caps,
            EnumSet<TestProperty> props, long count, URI factoryUri)
            throws Throwable {
        if (props == null) {
            props = EnumSet.noneOf(TestProperty.class);
        }

        this.host.log("creating services");
        this.host.testStart(count);
        URI[] childUris = new URI[(int) count];
        AtomicInteger uriCount = new AtomicInteger();
        Map<URI, MinimalTestServiceState> initialStates = new HashMap<>();

        for (int i = 0; i < count; i++) {
            MinimalTestServiceState initialState = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();

            initialState.documentSelfLink = UUID.randomUUID().toString();
            initialStates.put(UriUtils.extendUri(factoryUri,
                    initialState.documentSelfLink), initialState);

            // create a start service POST with an initial state
            Operation post = Operation
                    .createPost(factoryUri)
                    .setBody(initialState)
                    .setCompletion(
                            (o, e) -> {
                                if (e != null) {
                                    this.host.failIteration(e);
                                    return;
                                }
                                try {
                                    MinimalTestServiceState s = o
                                            .getBody(MinimalTestServiceState.class);
                                    childUris[uriCount.getAndIncrement()] = UriUtils
                                            .buildUri(this.host,
                                                    s.documentSelfLink);
                                    this.host.completeIteration();
                                } catch (Throwable e1) {
                                    this.host.failIteration(e1);
                                }
                            });
            if (props.contains(TestProperty.FORCE_REMOTE)) {
                post.forceRemote();
            }
            this.host.send(post);
        }

        this.host.testWait();
        this.host.logThroughput();

        // get service state from child service and verify it is the same as the initial state
        Map<URI, MinimalTestServiceState> childServiceStates = this.host
                .getServiceState(null, MinimalTestServiceState.class, childUris);

        validateBeforeAfterServiceStates(caps, count, factoryUri.getPath(),
                initialStates, childServiceStates);

        if (caps.contains(ServiceOption.PERSISTENCE)) {

            this.host.log("GET on factory");
            this.host.testStart(1);
            ServiceDocumentQueryResult res = new ServiceDocumentQueryResult();
            // now get the child state URIs through a GET on the factory and
            // confirm
            // we get the same results
            URI factoryUriWithExpand = UriUtils.extendUriWithQuery(factoryUri,
                    UriUtils.URI_PARAM_ODATA_EXPAND,
                    ServiceDocument.FIELD_NAME_SELF_LINK);
            Operation get = Operation.createGet(factoryUriWithExpand).forceRemote().setCompletion(
                    (o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        ServiceDocumentQueryResult rsp = o
                                .getBody(ServiceDocumentQueryResult.class);
                        res.documents = rsp.documents;
                        res.documentLinks = rsp.documentLinks;
                        this.host.completeIteration();
                    });
            this.host.send(get);
            this.host.testWait();

            assertTrue(res.documentLinks != null);
            assertTrue(res.documentLinks.size() == childServiceStates.size());

            childServiceStates.clear();
            for (Object d : res.documents.values()) {
                MinimalTestServiceState expandedState = Utils.fromJson(d,
                        MinimalTestServiceState.class);
                childServiceStates.put(
                        UriUtils.buildUri(factoryUri, expandedState.documentSelfLink),
                        expandedState);
            }

            validateBeforeAfterServiceStates(caps, count, factoryUri.getPath(),
                    initialStates, childServiceStates);

        }

        // now do N PATCHs per child service so we can confirm version
        // increments and is restored after restart
        int patchCount = 10;
        this.host.testStart("Issuing parallel PATCH requests", null, childUris.length * patchCount);
        for (URI u : childUris) {
            for (int i = 0; i < patchCount; i++) {
                Operation patch = Operation.createPatch(u)
                        .setBody(this.host.buildMinimalTestState())
                        .setCompletion(this.host.getCompletion());
                this.host.send(patch);
            }
        }
        this.host.testWait();
        this.host.logThroughput();

        childServiceStates = this.host.getServiceState(null,
                MinimalTestServiceState.class, childUris);
        int mismatchCount = 0;
        for (MinimalTestServiceState s : childServiceStates.values()) {
            if (s.documentVersion != patchCount) {
                this.host.log("expected %d got %d for %s", patchCount, s.documentVersion,
                        s.documentSelfLink);
                mismatchCount++;
            }
        }

        if (mismatchCount > 0) {
            this.host.log("%d documents did not converge to latest version", mismatchCount);
            throw new IllegalStateException();
        }

        deleteServices(caps, props, childUris);

        if (!caps.contains(ServiceOption.PERSISTENCE)) {
            return;
        }

        this.host.log("Deleting durable factory");
        // we need to do restart of durable child services verification
        // we just stopped all child services. Stop the factory service now
        this.host.testStart(1);
        this.host.send(Operation.createDelete(factoryUri).setCompletion(
                this.host.getCompletion()));
        this.host.testWait();

        this.host.log("Restarting durable factory");
        this.host.testStart(1);
        // restart factory service, using the same URI
        MinimalFactoryTestService factoryService = new MinimalFactoryTestService();
        factoryService.setChildServiceCaps(caps);
        for (ServiceOption c : caps) {
            factoryService.toggleOption(c, true);
        }
        this.host.startService(
                Operation.createPost(factoryUri).setCompletion(
                        this.host.getCompletion()), factoryService);
        this.host.testWait();

        if (props.contains(TestProperty.DELETE_DURABLE_SERVICE)) {
            validateDurableServiceRestartAfterDelete(factoryUri, childUris,
                    childServiceStates, patchCount);
            deleteServices(caps, props, childUris);
        } else {
            // the services should be all recreated by the time the factory
            // service
            // is marked available. Get the states and compare
            this.host.log("Making sure all states are available after restart");
            Map<URI, MinimalTestServiceState> childServiceStatesAfterRestart = this.host
                    .getServiceState(null, MinimalTestServiceState.class,
                            childUris);

            validateBeforeAfterServiceStates(caps, count, factoryUri.getPath(),
                    childServiceStates, childServiceStatesAfterRestart);
        }

    }

    private void deleteServices(EnumSet<ServiceOption> caps,
            EnumSet<TestProperty> props, URI[] childUris) throws Throwable {
        this.host.log("Deleting %d services", childUris.length);
        this.host.testStart(childUris.length);
        for (URI u : childUris) {
            Operation delete = Operation.createDelete(u).setCompletion(
                    this.host.getCompletion());
            if (caps.contains(ServiceOption.PERSISTENCE)) {
                if (!props.contains(TestProperty.DELETE_DURABLE_SERVICE)) {
                    // simply stop the service, do not mark deleted
                    delete.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE);
                }
            }
            this.host.send(delete);
        }
        this.host.testWait();
    }

    private void validateDurableServiceRestartAfterDelete(URI factoryUri,
            URI[] childUris,
            Map<URI, MinimalTestServiceState> childServiceStates,
            int patchCount) throws Throwable {

        this.host.waitForServiceAvailable(factoryUri);

        // since we stopped AND marked each child service state deleted, the
        // factory should have not re-created any service. Confirm.

        this.host.testStart(1);
        this.host
                .send(Operation
                        .createGet(factoryUri)
                        .setCompletion(
                                (o, e) -> {
                                    if (!o.hasBody()) {
                                        this.host.completeIteration();
                                        return;
                                    }
                                    ServiceDocumentQueryResult r = o
                                            .getBody(ServiceDocumentQueryResult.class);
                                    if (r.documentLinks != null
                                            && !r.documentLinks.isEmpty()) {
                                        this.host
                                                .failIteration(new IllegalStateException(
                                                        "Child services are present after restart, not expected"));
                                        return;
                                    }
                                    this.host.completeIteration();
                                }));
        this.host.testWait();

        // re create child service using the *same* selflink, so they get
        // associated with the same document history
        // create a start service POST with an initial state
        this.host.testStart(childServiceStates.size());
        for (URI u : childServiceStates.keySet()) {
            MinimalTestServiceState newState = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            String selfLink = u.getPath();
            newState.documentSelfLink = selfLink.substring(selfLink
                    .lastIndexOf(UriUtils.URI_PATH_CHAR));
            // version must be higher than previously deleted version
            newState.documentVersion = patchCount * 2;
            Operation post = Operation.createPost(factoryUri).setBody(newState)
                    .setCompletion(this.host.getCompletion());
            this.host.send(post);
        }
        this.host.testWait();

        Map<URI, MinimalTestServiceState> childServiceStatesAfterRestart = this.host
                .getServiceState(null, MinimalTestServiceState.class, childUris);

        for (MinimalTestServiceState s : childServiceStatesAfterRestart
                .values()) {
            MinimalTestServiceState beforeRestart = childServiceStates
                    .get(UriUtils.buildUri(factoryUri, s.documentSelfLink));
            // version should be two more than PATCH count:
            // +1 for the DELETE right before shutdown
            // +1 for the new initial state
            assertTrue(s.documentVersion == beforeRestart.documentVersion + 2);
        }
    }

    private void validateBeforeAfterServiceStates(EnumSet<ServiceOption> caps,
            long count,
            String expectedPrefix,
            Map<URI, MinimalTestServiceState> initialStates,
            Map<URI, MinimalTestServiceState> childServiceStates) throws Throwable {

        MinimalTestService stub = (MinimalTestService) this.host.startServiceAndWait(
                MinimalTestService.class, UUID.randomUUID().toString());
        ServiceDocumentDescription d = stub.getDocumentTemplate().documentDescription;

        for (Entry<URI, MinimalTestServiceState> e : childServiceStates
                .entrySet()) {
            MinimalTestServiceState childServiceState = e.getValue();
            assertTrue(childServiceState.documentSelfLink != null);
            // verify the self link of the child service has the same prefix as
            // the
            // factory service URI

            assertTrue(childServiceState.documentSelfLink
                    .startsWith(expectedPrefix));
            MinimalTestServiceState initialState = initialStates
                    .get(e.getKey());
            if (count == 1) {
                // initial state had no self link when count == 1
                initialState.documentSelfLink = childServiceState.documentSelfLink;
            }

            if (initialState == null) {
                throw new IllegalStateException(
                        "Child service state has self link not seen before");
            }

            assertTrue(initialState.id.equals(childServiceState.id));
            assertTrue(childServiceState.documentKind.equals(Utils
                    .buildKind(MinimalTestServiceState.class)));

            if (caps.contains(ServiceOption.PERSISTENCE)) {
                boolean isEqual = ServiceDocument.equals(d, initialState, childServiceState);
                assertTrue(isEqual);
            }
        }
    }

    @Test
    public void sendWrongContentType() throws Throwable {
        startFactoryService();

        this.host.toggleNegativeTestMode(true);
        // attempt to create service with unrecognized content type and non JSON body
        this.host.testStart(1);
        Operation post = Operation
                .createPost(this.factoryUri)
                .setBody("")
                .setContentType(Operation.MEDIA_TYPE_TEXT_PLAIN)
                .setCompletion(
                        (o, e) -> {
                            if (e == null
                                    || !e.getMessage().contains("Unrecognized Content-Type")) {
                                this.host.failIteration(new IllegalStateException(
                                        "Should have rejected request"));
                            } else {
                                this.host.completeIteration();
                            }
                        });
        this.host.send(post);
        this.host.testWait();

        // attempt to create service with proper content type but garbage body
        this.host.testStart(1);
        post = Operation
                .createPost(this.factoryUri)
                .setBody("")
                .setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON)
                .setCompletion(
                        (o, e) -> {
                            if (e == null) {
                                this.host.failIteration(new IllegalStateException(
                                        "Should have rejected request"));
                            } else {
                                ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                                if (rsp.message == null
                                        || !rsp.message.toLowerCase()
                                                .contains("body is required")) {
                                    this.host.failIteration(new IllegalStateException(
                                            "Invalid error response"));
                                    return;
                                }

                                this.host.completeIteration();
                            }
                        });
        this.host.send(post);
        this.host.testWait();
        this.host.toggleNegativeTestMode(false);
    }

    @Test
    public void sendBadJson() throws Throwable {
        startFactoryService();
        this.host.testStart(1);
        // attempt to create service with bad content type
        Operation post = Operation
                .createPost(this.factoryUri)
                .setBody("{\"whatever\": 3}}")
                .setContentType("application/json")
                .setCompletion(
                        (o, e) -> {
                            if (e == null || !e.getMessage().contains("Unparseable JSON body")) {
                                this.host.failIteration(new IllegalStateException(
                                        "Should have rejected request"));
                            } else {
                                this.host.completeIteration();
                            }
                        });
        this.host.send(post);
        this.host.testWait();
    }

    @Test
    public void factoryServiceRemotePost() throws Throwable {
        // first create the factory service
        long count = 100;
        URI factoryUri = this.host.startServiceAndWait(
                MinimalFactoryTestService.class, UUID.randomUUID().toString())
                .getUri();
        EnumSet<TestProperty> props = EnumSet.of(TestProperty.FORCE_REMOTE);
        doFactoryServiceChildCreation(props, count, factoryUri);
    }

    @Test
    public void throughputFactoryServicePost() throws Throwable {
        // first create the factory service
        long count = this.serviceCount;
        if (count < 1) {
            count = this.host.computeIterationsFromMemory(10) / 20;
        }
        URI factoryUri = this.host.startServiceAndWait(
                MinimalFactoryTestService.class, UUID.randomUUID().toString())
                .getUri();

        doFactoryServiceChildCreation(count, factoryUri);
        doFactoryServiceChildCreation(count, factoryUri);
    }

    @Test
    public void duplicateFactoryPost() throws Throwable {

        MinimalFactoryTestService factory = (MinimalFactoryTestService) this.host
                .startServiceAndWait(
                        MinimalFactoryTestService.class, UUID.randomUUID().toString());

        URI factoryUri = factory.getUri();
        factory.toggleOption(ServiceOption.IDEMPOTENT_POST, true);

        String selfLink = UUID.randomUUID().toString();
        // issue two POSTs to the factory, using the same self link. The first one will create
        // the service, the second one should be automatically converted to a PUT, and
        // update the service state

        MinimalTestServiceState lastState = null;
        for (int i = 0; i < 2; i++) {
            this.host.testStart(1);
            MinimalTestServiceState initialState = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            initialState.id = UUID.randomUUID().toString();
            initialState.documentSelfLink = selfLink;
            lastState = initialState;
            Operation post = Operation
                    .createPost(factoryUri)
                    .setBody(initialState)
                    .setCompletion(this.host.getCompletion());
            this.host.send(post);
            this.host.testWait();
        }

        // disable capability, expect failure
        factory.toggleOption(ServiceOption.IDEMPOTENT_POST, false);
        this.host.testStart(1);
        MinimalTestServiceState initialState = (MinimalTestServiceState) this.host
                .buildMinimalTestState();
        initialState.id = UUID.randomUUID().toString();
        initialState.documentSelfLink = selfLink;
        Operation post = Operation
                .createPost(factoryUri)
                .setBody(initialState)
                .setCompletion(
                        (o, e) -> {
                            if (o.getStatusCode() != Operation.STATUS_CODE_CONFLICT
                                    || e == null) {
                                this.host.failIteration(new IllegalStateException());
                                return;
                            }
                            this.host.completeIteration();
                        });
        this.host.send(post);
        this.host.testWait();

        factory.toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        int count = 16;
        this.host.testStart(count);
        // now do it concurrently N times
        for (int i = 0; i < count; i++) {
            initialState = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            initialState.id = lastState.id;
            initialState.documentSelfLink = selfLink;
            lastState = initialState;
            post = Operation
                    .createPost(factoryUri)
                    .setBody(initialState)
                    .setCompletion(this.host.getCompletion());
            this.host.send(post);
        }
        this.host.testWait();

        // get service state, verify it matches the state sent in the second POST
        MinimalTestServiceState currentState = this.host.getServiceState(null,
                MinimalTestServiceState.class, UriUtils.extendUri(factoryUri, selfLink));
        assertTrue("Expected version " + count + 1, currentState.documentVersion == count + 1);
        assertTrue("Expected id " + lastState.id, currentState.id.equals(lastState.id));
    }

    @Test
    public void duplicateFactoryPostWithInitialFailure() throws Throwable {

        MinimalFactoryTestService factory = (MinimalFactoryTestService) this.host
                .startServiceAndWait(
                        MinimalFactoryTestService.class, UUID.randomUUID().toString());

        URI factoryUri = factory.getUri();

        // issue a request that should fail in handleStart()
        String selfLink = UUID.randomUUID().toString();
        this.host.testStart(1);
        MinimalTestServiceState initialState = (MinimalTestServiceState) this.host
                .buildMinimalTestState();
        initialState.id = null;
        initialState.documentSelfLink = selfLink;

        Operation post = Operation
                .createPost(factoryUri)
                .setBody(initialState)
                .setCompletion(this.host.getExpectedFailureCompletion());
        this.host.send(post);
        this.host.testWait();

        // verify GET to the service fails
        this.host.testStart(1);
        this.host.send(Operation.createGet(UriUtils.extendUri(factoryUri, selfLink)).setCompletion(
                this.host.getExpectedFailureCompletion()));
        this.host.testWait();

        // now post again, this time, it should succeed
        this.host.testStart(1);
        initialState = (MinimalTestServiceState) this.host.buildMinimalTestState();
        initialState.documentSelfLink = selfLink;

        post.setBody(initialState).setCompletion(this.host.getCompletion());
        this.host.send(post);
        this.host.testWait();
    }

    @Test
    public void testFactoryPostHandling() throws Throwable {
        startFactoryService();

        this.host.testStart(4);
        idempotentPostReturnsUpdatedOpBody();
        checkDerivedSelfLinkWhenProvidedSelfLinkIsJustASuffix();
        checkDerivedSelfLinkWhenProvidedSelfLinkAlreadyContainsAPath();
        checkDerivedSelfLinkWhenProvidedSelfLinkLooksLikeItContainsAPathButDoesnt();
        this.host.testWait();
    }

    @Test
    public void odataSupport() throws Throwable {
        URI factoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);

        this.host.startService(Operation.createPost(factoryUri),
                ExampleService.createFactory());
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        Supplier<Stream<ExampleServiceState>> emptySupplier = () -> LongStream.range(0, 0).mapToObj(i -> {
            return new ExampleServiceState();
        });

        validateCount(emptySupplier, true);
        validateCount(emptySupplier, false);
        validateLimit(emptySupplier, 1, true);
        validateLimit(emptySupplier, 5, false);
        validateOrderBy(emptySupplier, "counter", true, true);
        validateLimitAndOrderBy(emptySupplier, 1, true, "counter", true, true);
        validateLimitAndOrderBy(emptySupplier, 5, false, "counter", false, true);

        Supplier<Stream<ExampleServiceState>> stateSupplier = () -> LongStream.range(0, 5).mapToObj(i -> {
            ExampleServiceState state = new ExampleServiceState();
            state.counter = i;
            state.name = i + "-abcd";
            return state;
        });

        this.host.testStart(1);
        OperationJoin
                .create(stateSupplier.get().map(state -> {
                    return Operation
                        .createPost(factoryUri)
                        .setReferer(this.host.getUri())
                        .setBody(state);
                }))
                .setCompletion((os, es) -> {
                    if (es != null && !es.isEmpty()) {
                        this.host.failIteration(es.values().iterator().next());
                        return;
                    }
                    this.host.completeIteration();
                })
                .sendWith(this.host);
        this.host.testWait();

        validateCount(stateSupplier, true);
        validateCount(stateSupplier, false);
        validateLimit(stateSupplier, 1, true);
        validateLimit(stateSupplier, 5, false);
        validateLimit(stateSupplier, 10, true);
        validateOrderBy(stateSupplier, "counter", true, true);
        validateOrderBy(stateSupplier, "name", false, false);
        validateLimitAndOrderBy(stateSupplier, 1, true, "counter", true, true);
        validateLimitAndOrderBy(stateSupplier, 5, false, "counter", false, true);
        validateLimitAndOrderBy(stateSupplier, 10, false, "name", true, false);
    }

    private void validateCount(Supplier<Stream<ExampleServiceState>> stateSupplier,
            boolean count) throws Throwable {
        String queryString = String.format("$count=%s", count);
        ODataFactoryQueryResult result = getResult(queryString);
        assertTrue(result.documentCount == stateSupplier.get().count());
        assertTrue(result.totalCount == stateSupplier.get().count());
    }

    private void validateLimit(Supplier<Stream<ExampleServiceState>> stateSupplier, long limit,
            boolean count) throws Throwable {
        String queryString = String.format("$limit=%s&$count=%s", limit, count);
        ODataFactoryQueryResult result = getResult(queryString);

        long current = result.documentCount;

        assertTrue(current <= limit);
        assertTrue(result.totalCount == stateSupplier.get().count());

        String nextPageLink = result.nextPageLink;
        while (nextPageLink != null) {
            ServiceDocumentQueryResult nextResult = getNextResult(nextPageLink);
            nextPageLink = nextResult.nextPageLink;
            assertTrue(nextResult.documentCount <= limit);
            current += nextResult.documentCount;
        }

        assertTrue(current == stateSupplier.get().count());
    }

    private void validateOrderBy(Supplier<Stream<ExampleServiceState>> stateSupplier,
            String fieldName, boolean asc, boolean filter) throws Throwable {
        String queryString = String.format("$orderby=%s %s", fieldName, asc ? "asc" : "desc");
        if (filter) {
            queryString += String.format("&$filter=%s lt %s", fieldName, stateSupplier.get().count());
        }
        ServiceDocumentQueryResult result = getResult(queryString);

        if (!asc) {
            Collections.reverse(result.documentLinks);
        }
        Field field = ExampleServiceState.class.getField(fieldName);
        Iterator<String> iterator = result.documentLinks.iterator();
        stateSupplier.get().forEachOrdered((state) -> {
            ExampleServiceState resultState = Utils.fromJson(
                    result.documents.get(iterator.next()), ExampleServiceState.class);
            try {
                assertEquals(field.get(state), field.get(resultState));
            } catch (Exception ex) {
                throw new IllegalArgumentException(ex);
            }
        });
    }

    private void validateLimitAndOrderBy(Supplier<Stream<ExampleServiceState>> stateSupplier, long limit,
            boolean count, String fieldName, boolean asc, boolean filter) throws Throwable {
        String queryString = String.format("$limit=%s&$count=%s&$orderby=%s %s",
                limit, count, fieldName, asc ? "asc" : "desc");
        if (filter) {
            queryString += String.format("&$filter=%s lt %s", fieldName, stateSupplier.get().count());
        }
        ODataFactoryQueryResult result = getResult(queryString);
        long current = result.documentLinks.size();

        assertTrue(current <= limit);
        assertTrue(result.totalCount == stateSupplier.get().count());

        if (!asc) {
            Collections.reverse(result.documentLinks);
        }
        Field field = ExampleServiceState.class.getField(fieldName);
        Iterator<String> iterator = result.documentLinks.iterator();
        stateSupplier.get().limit(limit).forEachOrdered((state) -> {
            ExampleServiceState resultState = Utils.fromJson(
                    result.documents.get(iterator.next()), ExampleServiceState.class);
            try {
                assertEquals(field.get(state), field.get(resultState));
            } catch (Exception ex) {
                throw new IllegalArgumentException(ex);
            }
        });

        String nextPageLink = result.nextPageLink;
        while (nextPageLink != null) {
            ServiceDocumentQueryResult nextResult = getNextResult(nextPageLink);
            nextPageLink = nextResult.nextPageLink;
            assertTrue(nextResult.documentCount <= limit);

            if (!asc) {
                Collections.reverse(nextResult.documentLinks);
            }
            Iterator<String> nextIterator = nextResult.documentLinks.iterator();
            stateSupplier.get().skip(current).limit(limit).forEachOrdered((state) -> {
                ExampleServiceState resultState = Utils.fromJson(
                        nextResult.documents.get(nextIterator.next()), ExampleServiceState.class);
                try {
                    assertEquals(field.get(state), field.get(resultState));
                } catch (Exception ex) {
                    throw new IllegalArgumentException(ex);
                }
            });

            current += nextResult.documentCount;
        }

        assertTrue(current == stateSupplier.get().count());
    }

    private ODataFactoryQueryResult getResult(String queryString) throws Throwable {
        AtomicReference<ODataFactoryQueryResult> result = new AtomicReference<>();

        this.host.testStart(1);
        Operation.createGet(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK, queryString))
            .setCompletion((o, e) -> {
                if (e != null) {
                    this.host.failIteration(e);
                    return;
                }
                result.set(o.getBody(ODataFactoryQueryResult.class));
                this.host.completeIteration();
            })
            .setReferer(this.host.getUri())
            .sendWith(this.host);
        this.host.testWait();

        assertNotNull(result.get());
        return result.get();
    }

    private ServiceDocumentQueryResult getNextResult(String nextPageLink) throws Throwable {
        AtomicReference<ServiceDocumentQueryResult> result = new AtomicReference<>();

        this.host.testStart(1);
        Operation.createGet(UriUtils.buildUri(this.host, nextPageLink))
            .setCompletion((o, e) -> {
                if (e != null) {
                    this.host.failIteration(e);
                    return;
                }

                result.set(o.getBody(ServiceDocumentQueryResult.class));
                this.host.completeIteration();
            })
            .setReferer(this.host.getUri())
            .sendWith(this.host);
        this.host.testWait();

        return result.get();
    }

    private void startFactoryService() throws Throwable {
        if (this.host.getServiceStage(this.factoryUri.getPath()) != null) {
            return;
        }
        this.host.startService(
                Operation.createPost(this.factoryUri),
                new SomeFactoryService());
        this.host.waitForServiceAvailable(SomeFactoryService.SELF_LINK);
    }

    @Test
    public void postFactoryQueueing() throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "/subpath-" + UUID.randomUUID().toString();

        if (this.host.checkServiceAvailable(this.factoryUri.getPath())) {
            this.host.testStart(1);
            this.host.send(Operation.createDelete(this.factoryUri).setCompletion(
                    this.host.getCompletion()));
            this.host.testWait();
        }

        this.host.testStart(1);
        Operation post = Operation
                .createPost(UriUtils.buildUri(this.factoryUri))
                .setBody(doc)
                .setCompletion(
                        (op, ex) -> {
                            if (op.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                                this.host.completeIteration();
                                return;
                            }

                            this.host.failIteration(new Throwable(
                                    "Expected Operation.STATUS_CODE_NOT_FOUND"));
                        });

        this.host.send(post);
        this.host.testWait();

        this.host.testStart(2);
        post = Operation
                .createPost(this.factoryUri)
                .setBody(doc)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)
                .setCompletion(
                        (op, ex) -> {
                            if (op.getStatusCode() == Operation.STATUS_CODE_OK) {
                                this.host.completeIteration();
                                return;
                            }

                            this.host.failIteration(new Throwable(
                                    "Expected Operation.STATUS_CODE_OK"));
                        });
        this.host.send(post);
        this.host.startService(
                Operation.createPost(this.factoryUri),
                new SomeFactoryService());
        this.host.registerForServiceAvailability(this.host.getCompletion(),
                SomeFactoryService.SELF_LINK);
        this.host.testWait();

    }

    private void idempotentPostReturnsUpdatedOpBody() throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "/subpath/fff/apple";
        doc.value = 2;

        this.host.send(Operation.createPost(this.factoryUri)
                .setBody(doc)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            this.host.send(Operation.createPost(this.factoryUri)
                                    .setBody(doc)
                                    .setCompletion(
                                            (o2, e2) -> {
                                                if (e2 != null) {
                                                    this.host.failIteration(e2);
                                                    return;
                                                }

                                                SomeDocument doc2 = o2.getBody(SomeDocument.class);
                                                try {
                                                    assertNotNull(doc2);
                                                    assertEquals(4, doc2.value);
                                                    this.host.completeIteration();
                                                } catch (AssertionError e3) {
                                                    this.host.failIteration(e3);
                                                }
                                            }));
                        }));
    }

    private void checkDerivedSelfLinkWhenProvidedSelfLinkIsJustASuffix() throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "freddy/x1";

        this.host.send(Operation.createPost(this.factoryUri)
                .setBody(doc)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    String selfLink = o.getBody(SomeDocument.class).documentSelfLink;
                    URI opUri = o.getUri();

                    String expectedPath = "/subpath/fff/freddy/x1";
                    try {
                        assertEquals(expectedPath, selfLink);
                        assertEquals(UriUtils.buildUri(this.host, expectedPath), opUri);
                        this.host.completeIteration();
                    } catch (Throwable e2) {
                        this.host.failIteration(e2);
                    }
                }));
    }

    private void checkDerivedSelfLinkWhenProvidedSelfLinkAlreadyContainsAPath() throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "/subpath/fff/freddy/x2";

        this.host.send(Operation.createPost(this.factoryUri)
                .setBody(doc)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    String selfLink = o.getBody(SomeDocument.class).documentSelfLink;
                    URI opUri = o.getUri();

                    String expectedPath = "/subpath/fff/freddy/x2";
                    try {
                        assertEquals(expectedPath, selfLink);
                        assertEquals(UriUtils.buildUri(this.host, expectedPath), opUri);
                        this.host.completeIteration();
                    } catch (Throwable e2) {
                        this.host.failIteration(e2);
                    }
                }));
    }

    private void checkDerivedSelfLinkWhenProvidedSelfLinkLooksLikeItContainsAPathButDoesnt()
            throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "/subpath/fffreddy/x3";

        this.host.send(Operation.createPost(this.factoryUri)
                .setBody(doc)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    String selfLink = o.getBody(SomeDocument.class).documentSelfLink;
                    URI opUri = o.getUri();

                    String expectedPath = "/subpath/fff/subpath/fffreddy/x3";
                    try {
                        assertEquals(expectedPath, selfLink);
                        assertEquals(UriUtils.buildUri(this.host, expectedPath), opUri);
                        this.host.completeIteration();
                    } catch (Throwable e2) {
                        this.host.failIteration(e2);
                    }
                }));
    }

    public static class SomeFactoryService extends FactoryService {

        public static final String SELF_LINK = FAC_PATH;

        SomeFactoryService() {
            super(SomeDocument.class);
            toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new SomeStatefulService();
        }

    }

    public static class SomeStatefulService extends StatefulService {

        SomeStatefulService() {
            super(SomeDocument.class);
        }

        @Override
        public void handlePut(Operation put) {
            SomeDocument a = put.getBody(SomeDocument.class);
            SomeDocument b = new SomeDocument();
            a.copyTo(b);
            b.value = 2 + a.value;
            put.setBody(b).complete();
        }

    }

    public static class SomeDocument extends ServiceDocument {

        public int value;

    }

}
