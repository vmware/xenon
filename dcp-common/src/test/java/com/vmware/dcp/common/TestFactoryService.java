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

package com.vmware.dcp.common;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.Service.ServiceOption;
import com.vmware.dcp.common.test.MinimalTestServiceState;
import com.vmware.dcp.common.test.TestProperty;
import com.vmware.dcp.services.common.ExampleService.ExampleServiceState;
import com.vmware.dcp.services.common.MinimalFactoryTestService;
import com.vmware.dcp.services.common.MinimalTestService;

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

public class TestFactoryService extends BasicReusableHostTestCase {

    public static final String FAC_PATH = "/subpath/fff";

    private URI factoryUri;

    @Before
    public void setup() throws Throwable {
        this.factoryUri = UriUtils.buildUri(this.host, SomeFactoryService.class);
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

    @Test
    public void factoryDurableServicePostWithDeleteRestart() throws Throwable {
        // first create the factory service
        long count = this.host.isStressTest() ? 10000 : 10;
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

    @Test
    public void factoryDurableServicePostNoCaching()
            throws Throwable {

        // disable caching. This makes everything a lot slower, but verifies the
        // index returns the most up to date state, after each update operation
        this.host.setServiceStateCaching(false);

        long count = this.host.isStressTest() ? 1000 : 10;
        MinimalFactoryTestService f = new MinimalFactoryTestService();
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
                childServiceStates.put(UriUtils.buildUri(factoryUri.getHost(),
                        factoryUri.getPort(), expandedState.documentSelfLink, null), expandedState);
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
        int i = 0;
        for (URI u : childServiceStates.keySet()) {
            MinimalTestServiceState newState = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            String selfLink = u.getPath();
            newState.documentSelfLink = selfLink.substring(selfLink
                    .lastIndexOf(UriUtils.URI_PATH_CHAR));
            // request version check on deleted document, on every other POST
            boolean doVersionCheck = i++ % 2 == 0;
            if (doVersionCheck) {
                // if version check is requested version must be higher than previously deleted version
                newState.documentVersion = patchCount * 2;
            }
            Operation post = Operation.createPost(factoryUri).setBody(newState)
                    .setCompletion(this.host.getCompletion());
            if (doVersionCheck) {
                post.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK);
            }
            this.host.send(post);
        }
        this.host.testWait();

        Map<URI, MinimalTestServiceState> childServiceStatesAfterRestart = this.host
                .getServiceState(null, MinimalTestServiceState.class, childUris);

        for (MinimalTestServiceState s : childServiceStatesAfterRestart
                .values()) {
            MinimalTestServiceState beforeRestart = childServiceStates
                    .get(UriUtils.buildUri(factoryUri.getHost(), factoryUri.getPort(),
                            s.documentSelfLink, null));
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
        this.host.testStart(1);
        // attempt to create service with unrecognized content type
        Operation post = Operation
                .createPost(this.factoryUri)
                .setBody("")
                .setContentType("text/plain")
                .setCompletion(
                        (o, e) -> {
                            if (e == null || !e.getMessage().contains("Unrecognized Content-Type")) {
                                this.host.failIteration(new IllegalStateException(
                                        "Should have rejected request"));
                            } else {
                                ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                                if (rsp.message == null
                                        || rsp.message.toLowerCase().contains("exception")) {
                                    this.host.failIteration(new IllegalStateException(
                                            "Invalid error response"));
                                    return;
                                }

                                this.host.completeIteration();
                            }
                        });
        this.host.send(post);
        this.host.testWait();
    }

    @Test
    public void sendBadJson() throws Throwable {
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
        this.host.startService(
                Operation.createPost(this.factoryUri),
                new SomeFactoryService());
        this.host.waitForServiceAvailable(SomeFactoryService.SELF_LINK);

        this.host.testStart(4);
        idempotentPostReturnsUpdatedOpBody();
        checkDerivedSelfLinkWhenProvidedSelfLinkIsJustASuffix();
        checkDerivedSelfLinkWhenProvidedSelfLinkAlreadyContainsAPath();
        checkDerivedSelfLinkWhenProvidedSelfLinkLooksLikeItContainsAPathButDoesnt();
        this.host.testWait();
    }

    @Test
    public void postFactoryQueueing() throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "/subpath";

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
