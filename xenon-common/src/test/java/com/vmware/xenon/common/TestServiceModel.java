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

import static com.vmware.xenon.common.Service.STAT_NAME_OPERATION_DURATION;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.junit.Test;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.MinimalFactoryTestService;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Test GetDocument when ServiceDocument specified an illegal type
 */
class GetIllegalDocumentService extends StatefulService {
    public static class IllegalServiceState extends ServiceDocument {
        // This is illegal since parameters ending in Link should be of type String
        public URI myLink;
    }

    public GetIllegalDocumentService() {
        super(IllegalServiceState.class);
    }
}

public class TestServiceModel extends BasicReusableHostTestCase {

    private static final String STAT_NAME_HANDLE_PERIODIC_MAINTENANCE = "handlePeriodicMaintenance";
    private static final int PERIODIC_MAINTENANCE_MAX = 2;

    /**
     * Parameter that specifies if this run should be a stress test.
     */
    public boolean isStressTest;

    /**
     * Parameter that specifies the request count to use for throughput tests. If zero, request count
     * will be computed based on available memory
     */
    public long requestCount = 0;

    /**
     * Parameter that specifies the service instance count
     */
    public long serviceCount = 0;

    public static class ArgumentParsingTestTarget {
        public int intField = Integer.MIN_VALUE;
        public long longField = Long.MIN_VALUE;
        public double doubleField = Double.MIN_VALUE;
        public String stringField = "";
        public boolean booleanField = false;
        public String[] stringArrayField = null;

    }

    @Test
    public void commandLineArgumentParsing() {
        ArgumentParsingTestTarget t = new ArgumentParsingTestTarget();
        int intValue = 1234;
        long longValue = 1234567890L;
        double doubleValue = Double.MAX_VALUE;
        boolean booleanValue = true;
        String stringValue = "" + longValue;
        String stringArrayValue = "10.1.1.1,10.1.1.2";
        String[] splitStringArrayValue = stringArrayValue.split(",");
        String[] args = { "--intField=" + intValue,
                "--doubleField=" + doubleValue, "--longField=" + longValue,
                "--booleanField=" + booleanValue,
                "--stringField=" + stringValue,
                "--stringArrayField=" + stringArrayValue };

        t.stringArrayField = new String[0];
        CommandLineArgumentParser.parse(t, args);

        assertEquals(t.intField, intValue);
        assertEquals(t.longField, longValue);
        assertTrue(t.doubleField == doubleValue);
        assertEquals(t.booleanField, booleanValue);
        assertEquals(t.stringField, stringValue);
        assertEquals(t.stringArrayField.length, splitStringArrayValue.length);
        for (int i = 0; i < t.stringArrayField.length; i++) {
            assertEquals(t.stringArrayField[i], splitStringArrayValue[i]);
        }
    }

    @Test
    public void serviceStop() throws Throwable {
        MinimalTestService serviceToBeDeleted = new MinimalTestService();
        MinimalTestService serviceToBeStopped = new MinimalTestService();
        MinimalFactoryTestService factoryService = new MinimalFactoryTestService();
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = UUID.randomUUID().toString();
        this.host.startServiceAndWait(serviceToBeDeleted, UUID.randomUUID().toString(), body);
        this.host.startServiceAndWait(serviceToBeStopped, UUID.randomUUID().toString(), body);
        this.host.startServiceAndWait(factoryService, UUID.randomUUID().toString(), null);

        body.id = MinimalTestService.STRING_MARKER_FAIL_REQUEST;
        // first issue a delete with a body (used as a hint to fail delete), and it should be aborted.
        // Verify service is still running if it fails delete
        Operation delete = Operation.createDelete(serviceToBeDeleted.getUri())
                .setBody(body);
        Operation response = this.host.waitForResponse(delete);
        assertNotNull(response);
        assertEquals(Operation.STATUS_CODE_INTERNAL_ERROR, response.getStatusCode());

        // try a delete that should be aborted with the factory service
        delete = Operation.createDelete(factoryService.getUri())
                .setBody(body);
        response = this.host.waitForResponse(delete);
        assertNotNull(response);
        assertEquals(Operation.STATUS_CODE_INTERNAL_ERROR, response.getStatusCode());

        // verify services are still running
        assertEquals(ProcessingStage.AVAILABLE,
                this.host.getServiceStage(factoryService.getSelfLink()));
        assertEquals(ProcessingStage.AVAILABLE,
                this.host.getServiceStage(serviceToBeDeleted.getSelfLink()));

        delete = Operation.createDelete(serviceToBeDeleted.getUri());
        response = this.host.waitForResponse(delete);
        assertNotNull(response);
        assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());

        assertTrue(serviceToBeDeleted.gotDeleted);
        assertTrue(serviceToBeDeleted.gotStopped);

        try {
            // stop the host, observe stop only on remaining service
            this.host.tearDown();
            assertTrue(!serviceToBeStopped.gotDeleted);
            assertTrue(serviceToBeStopped.gotStopped);
            assertTrue(factoryService.gotStopped);
            this.host = null;
        } finally {
            setUpOnce();
        }
    }

    @Test
    public void stopServiceWithSynch() throws Throwable {
        // Wait for synchronization to happen.
        this.host.waitForReplicatedFactoryServiceAvailable(
                UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK),
                ServiceUriPaths.DEFAULT_NODE_SELECTOR);

        long count = Math.max(this.serviceCount, 10);

        // Determine the current count of pending service deletions.
        Map<String, ServiceStat> stats = this.host.getServiceStats(this.host.getManagementServiceUri());
        ServiceStat stat = stats.get(
                ServiceHostManagementService.STAT_NAME_PENDING_SERVICE_DELETION_COUNT);
        double oldCount = (stat != null) ? stat.latestValue : 0;

        // create example services.
        List<URI> exampleUris = this.host.createExampleServices(this.host, count, null);

        // do GETs on the example services to make sure all have started successfully.
        this.host.getServiceState(null, ExampleService.ExampleServiceState.class, exampleUris);

        // Create DELETE and SYNCH_OWNER requests for each service
        TestContext ctx = this.host.testCreate(exampleUris.size() * 2);
        List<Operation> operations = new ArrayList<>(exampleUris.size() * 2);
        for (URI exampleUri : exampleUris) {

            // It's ok if the synch request fails
            ServiceDocument doc = new ServiceDocument();
            doc.documentSelfLink = exampleUri.getPath();
            Operation synchPost = Operation
                    .createPost(this.host, ExampleService.FACTORY_LINK)
                    .setBody(doc)
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_OWNER)
                    .setReferer(this.host.getUri())
                    .setCompletion((o, e) -> {
                        this.host.log(Level.INFO, "Sync completed for %s. Failure: %s",
                                exampleUri, e != null ? e.getMessage() : "none");
                        ctx.completeIteration();
                    });
            operations.add(synchPost);

            // Deletes should not fail.
            Operation delete = Operation
                    .createDelete(exampleUri)
                    .setReferer(this.host.getUri())
                    .setCompletion(ctx.getCompletion());
            operations.add(delete);
        }

        // Send all DELETE and SYNCH_OWNER requests together to maximize their
        // chance of getting intervleaved.
        for (Operation op : operations) {
            this.host.send(op);
        }
        ctx.await();

        // Do gets on each service and make sure each one returns a 404 error.
        for (URI exampleUri : exampleUris) {
            Operation op = Operation.createGet(exampleUri);
            TestRequestSender.FailureResponse response = this.host
                    .getTestRequestSender().sendAndWaitFailure(op);
            assertEquals(Operation.STATUS_CODE_NOT_FOUND, response.op.getStatusCode());
        }

        // Because we are using a shared host, the new pending deletion count should be anywhere
        // from zero to the count we recorded at the start of this test.
        this.host.waitFor("pendingServiceCount did not reach expected value", () -> {
            Map<String, ServiceStat> newStats = this.host.getServiceStats(this.host.getManagementServiceUri());
            ServiceStat newStat = newStats.get(ServiceHostManagementService.STAT_NAME_PENDING_SERVICE_DELETION_COUNT);
            return newStat.latestValue >= 0 && newStat.latestValue <= oldCount;
        });
    }

    /**
     * This test ensures that the service framework tracks per operation stats properly and more
     * importantly, it ensures that every single operation is seen by various stages of the
     * processing code path the proper number of times.
     *
     * @throws Throwable
     */
    @Test
    public void getRuntimeStatsReporting() throws Throwable {
        int serviceCount = 1;
        List<Service> services = this.host.doThroughputServiceStart(
                serviceCount, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.of(Service.ServiceOption.INSTRUMENTATION), null);
        long c = this.host.computeIterationsFromMemory(
                EnumSet.noneOf(TestProperty.class), serviceCount);
        c /= 10;
        this.host.doPutPerService(c, EnumSet.noneOf(TestProperty.class),
                services);
        URI[] statUris = buildStatsUris(serviceCount, services);

        Map<URI, ServiceStats> results = this.host.getServiceState(null,
                ServiceStats.class, statUris);

        for (ServiceStats s : results.values()) {
            assertTrue(s.documentSelfLink != null);
            assertTrue(s.entries != null && s.entries.size() > 1);
            // we expect at least GET and PUT specific operation stats
            for (ServiceStat st : s.entries.values()) {
                this.host.log("Stat\n: %s", Utils.toJsonHtml(st));
                if (st.name.startsWith(Action.GET.toString())) {
                    // the PUT throughput test does 2 gets
                    assertTrue(st.version == 2);
                }

                if (st.name.startsWith(Action.PUT.toString())) {
                    assertTrue(st.version == c);

                }

                if (st.name.toLowerCase().contains("micros")) {
                    assertTrue(st.logHistogram != null);
                    long totalCount = 0;
                    for (long binCount : st.logHistogram.bins) {
                        totalCount += binCount;
                    }
                    if (st.name.contains("GET")) {
                        assertTrue(totalCount == 2);
                    } else {
                        assertTrue(totalCount == c);
                    }
                }
            }
        }
    }

    private URI[] buildStatsUris(long serviceCount, List<Service> services) {
        URI[] statUris = new URI[(int) serviceCount];
        int i = 0;
        for (Service s : services) {
            statUris[i++] = UriUtils.extendUri(s.getUri(),
                    ServiceHost.SERVICE_URI_SUFFIX_STATS);
        }
        return statUris;
    }

    public static class ParentContextIdTestService extends StatefulService {

        public static final String SELF_LINK = "/parentTestService";
        private List<Service> childServices;
        private String expectedContextId;

        public ParentContextIdTestService() {
            super(ServiceDocument.class);
            super.toggleOption(ServiceOption.PERSISTENCE, true);
        }

        public void setChildService(List<Service> services) {
            this.childServices = services;
        }

        public void setExpectedContextId(String id) {
            this.expectedContextId = id;
        }

        @Override
        public void handleGet(final Operation get) {
            VerificationHost h = (VerificationHost) getHost();
            final String error = "context id not set in completion";
            List<Operation> ops = new ArrayList<>();
            for (Service s : this.childServices) {
                Operation op = Operation.createGet(s.getUri())
                        .setCompletion((completedOp, failure) -> {
                            if (!this.expectedContextId.equals(get.getContextId())) {
                                h.failIteration(new IllegalStateException(error));
                            }
                            h.completeIteration();
                        });
                ops.add(op);
            }

            if (!this.expectedContextId.equals(get.getContextId())) {
                h.failIteration(new IllegalStateException(error));
            }
            final OperationJoin operationJoin = OperationJoin.create(ops)
                    .setCompletion((s, failures) -> {
                        super.handleGet(get);
                    });
            operationJoin.sendWith(this);
        }
    }

    public static class ChildTestService extends StatefulService {

        public static final String FACTORY_LINK = "/childTestService";
        private String expectedContextId;

        public ChildTestService() {
            super(ChildTestServiceState.class);
            super.toggleOption(ServiceOption.PERSISTENCE, true);
        }

        public static class ChildTestServiceState extends ServiceDocument {
        }

        public void setExpectedContextId(String id) {
            this.expectedContextId = id;
        }

        @Override
        public void handleGet(final Operation get) {
            if (!this.expectedContextId.equals(get.getContextId())) {
                get.fail(new IllegalStateException("incorrect context id in child service"));
                return;
            }
            get.complete();
        }
    }

    @Test
    public void contextIdMultiServiceParallelFlow() throws Throwable {
        int count = Utils.DEFAULT_THREAD_COUNT * 2;
        final List<Service> childServices = this.host.doThroughputServiceStart(count,
                ChildTestService.class,
                new ServiceDocument(),
                EnumSet.noneOf(Service.ServiceOption.class),
                null);

        String contextId = UUID.randomUUID().toString();
        ParentContextIdTestService parent = new ParentContextIdTestService();
        parent.setExpectedContextId(contextId);
        for (Service c : childServices) {
            ((ChildTestService) c).setExpectedContextId(contextId);
        }
        parent.setChildService(childServices);
        this.host.startServiceAndWait(parent, UUID.randomUUID().toString(), new ServiceDocument());

        // expect N completions, from the parent, when it receives completions to child
        // operation
        this.host.testStart(count);
        Operation parentOp = Operation.createGet(parent.getUri())
                .setContextId(contextId);
        this.host.send(parentOp);
        this.host.testWait();

        // try again, force remote
        this.host.testStart(count);
        parentOp = Operation.createGet(parent.getUri())
                .setContextId(contextId).forceRemote();
        this.host.send(parentOp);
        this.host.testWait();
    }

    @Test
    public void contextIdFlowThroughService() throws Throwable {

        int serviceCount = 40;

        ContextIdTestService.State stateWithContextId = new ContextIdTestService.State();
        stateWithContextId.taskInfo = new TaskState();
        stateWithContextId.taskInfo.stage = TaskState.TaskStage.STARTED;
        stateWithContextId.startContextId = TestProperty.SET_CONTEXT_ID.toString();
        stateWithContextId.getContextId = UUID.randomUUID().toString();
        stateWithContextId.patchContextId = UUID.randomUUID().toString();
        stateWithContextId.putContextId = UUID.randomUUID().toString();

        List<Service> servicesWithContextId = this.host.doThroughputServiceStart(
                EnumSet.of(TestProperty.SET_CONTEXT_ID),
                serviceCount,
                ContextIdTestService.class,
                stateWithContextId,
                null,
                EnumSet.of(ServiceOption.CONCURRENT_UPDATE_HANDLING));

        ContextIdTestService.State stateWithOutContextId = new ContextIdTestService.State();
        stateWithOutContextId.taskInfo = new TaskState();
        stateWithOutContextId.taskInfo.stage = TaskState.TaskStage.STARTED;

        List<Service> servicesWithOutContextId = this.host.doThroughputServiceStart(
                EnumSet.noneOf(TestProperty.class),
                serviceCount,
                ContextIdTestService.class,
                stateWithOutContextId,
                null,
                null);

        // test get
        this.host.testStart(serviceCount * 4);
        doOperationWithContextId(servicesWithContextId, Action.GET,
                stateWithContextId.getContextId);
        doOperationWithContextId(servicesWithContextId, Action.GET,
                stateWithContextId.getContextId);
        doOperationWithContextId(servicesWithOutContextId, Action.GET, null);
        doOperationWithContextId(servicesWithOutContextId, Action.GET, null);
        this.host.testWait();

        // test put
        this.host.testStart(serviceCount * 4);
        doOperationWithContextId(servicesWithContextId, Action.PUT,
                stateWithContextId.putContextId);
        doOperationWithContextId(servicesWithContextId, Action.PUT,
                stateWithContextId.putContextId);
        doOperationWithContextId(servicesWithOutContextId, Action.PUT, null);
        doOperationWithContextId(servicesWithOutContextId, Action.PUT, null);
        this.host.testWait();

        // test patch
        this.host.testStart(serviceCount * 2);
        doOperationWithContextId(servicesWithContextId, Action.PATCH,
                stateWithContextId.patchContextId);
        doOperationWithContextId(servicesWithOutContextId, Action.PATCH, null);
        this.host.testWait();

        // check end state
        doCheckServicesState(servicesWithContextId);
        doCheckServicesState(servicesWithOutContextId);
    }

    public void doCheckServicesState(List<Service> services) throws Throwable {
        for (Service service : services) {
            ContextIdTestService.State resultState = null;
            Date expiration = this.host.getTestExpiration();

            while (new Date().before(expiration)) {
                resultState = this.host.getServiceState(
                        EnumSet.of(TestProperty.DISABLE_CONTEXT_ID_VALIDATION),
                        ContextIdTestService.State.class,
                        service.getUri());
                if (resultState.taskInfo.stage != TaskState.TaskStage.STARTED) {
                    break;
                }

                Thread.sleep(100);
            }
            assertNotNull(resultState);
            assertNotNull(resultState.taskInfo);
            assertEquals(TaskState.TaskStage.FINISHED, resultState.taskInfo.stage);
        }
    }

    public void doOperationWithContextId(List<Service> services, Service.Action action,
            String contextId) {
        for (Service service : services) {
            Operation op;
            switch (action) {
            case GET:
                op = Operation.createGet(service.getUri());
                break;
            case PUT:
                op = Operation.createPut(service.getUri());
                break;
            case PATCH:
                op = Operation.createPatch(service.getUri());
                break;
            default:
                throw new RuntimeException("Unsupported action");
            }

            op.forceRemote()
                    .setBody(new ContextIdTestService.State())
                    .setContextId(contextId)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }

                        this.host.completeIteration();
                    });
            this.host.send(op);
        }
        // reset context id, since its set in the main thread
        OperationContext.setContextId(null);
    }

    @Test
    public void throughputInMemoryServiceStart() throws Throwable {
        long c = this.host.computeIterationsFromMemory(100);
        this.host.doThroughputServiceStart(c, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);
        this.host.doThroughputServiceStart(c, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);
    }

    @Test
    public void queryInMemoryServices() throws Throwable {
        long c = this.host.computeIterationsFromMemory(100);

        // create a lot of service instances that are NOT indexed or durable
        this.host.doThroughputServiceStart(c / 2, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);

        // create some more, through a factory

        URI factoryUri = this.host.startServiceAndWait(
                MinimalFactoryTestService.class, UUID.randomUUID().toString())
                .getUri();

        this.host.testStart(c / 2);
        for (int i = 0; i < c / 2; i++) {
            // create a start service POST with an initial state
            Operation post = Operation.createPost(factoryUri)
                    .setBody(this.host.buildMinimalTestState())
                    .setCompletion(this.host.getCompletion());
            this.host.send(post);
        }

        this.host.testWait();

        this.host.testStart(1);
        // issue a single GET to the factory URI, with expand, and expect to see
        // c / 2 services
        this.host.send(Operation.createGet(UriUtils.buildExpandLinksQueryUri(factoryUri))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    ServiceDocumentQueryResult r = o
                            .getBody(ServiceDocumentQueryResult.class);
                    if (r.documentLinks.size() == c / 2) {
                        this.host.completeIteration();
                        return;
                    }

                    this.host.failIteration(new IllegalStateException(
                            "Un expected number of self links"));

                }));
        this.host.testWait();
    }

    @Test
    public void getDocumentTemplate() throws Throwable {
        URI uri = UriUtils.buildUri(this.host, "testGetDocumentInstance");

        // starting the service will call getDocumentTemplate - which should throw a RuntimeException, which causes
        // post to fail.
        Operation post = Operation.createPost(uri);
        this.host.startService(post, new GetIllegalDocumentService());
        assertEquals(500, post.getStatusCode());
        assertTrue(post.getErrorResponseBody().message.contains("myLink"));
    }

    public static class PrefixDispatchService extends StatelessService {

        public PrefixDispatchService() {
            super.toggleOption(ServiceOption.URI_NAMESPACE_OWNER, true);
        }


        private void validateAndComplete(Operation op) {
            if (!op.getUri().getPath().startsWith(getSelfLink())) {
                op.fail(new IllegalArgumentException("request must start with self link"));
                return;
            }
            op.complete();
        }

        @Override
        public void handlePost(Operation post) {
            validateAndComplete(post);
        }

        @Override
        public void handleOptions(Operation op) {
            validateAndComplete(op);
        }

        @Override
        public void handleDelete(Operation delete) {
            validateAndComplete(delete);
        }

        @Override
        public void handlePut(Operation op) {
            ServiceDocument body = new ServiceDocument();
            body.documentSelfLink = getSelfLink();
            op.setBody(body);
            validateAndComplete(op);
        }

        @Override
        public void handlePatch(Operation op) {
            validateAndComplete(op);
        }
    }

    @Test
    public void prefixDispatchingWithUriNamespaceOwner() throws Throwable {
        String prefix = UUID.randomUUID().toString();
        PrefixDispatchService s = new PrefixDispatchService();
        this.host.startServiceAndWait(s, prefix, null);

        PrefixDispatchService s1 = new PrefixDispatchService();
        String longerMatchedPrefix = prefix + "/" + "child";
        this.host.startServiceAndWait(s1, longerMatchedPrefix, null);

        // start a service that is a parent of s1
        PrefixDispatchService sParent = new PrefixDispatchService();
        String prefixMinus = prefix.substring(0, prefix.length() - 3);
        this.host.startServiceAndWait(sParent, prefixMinus, null);

        // start a service that is "under" the name space of the prefix.
        MinimalTestService s2 = new MinimalTestService();
        String prefixPlus = prefix + "/" + UUID.randomUUID();
        this.host.startServiceAndWait(s2, prefixPlus, null);

        // verify that a independent service (like a factory child) can register under the
        // prefix name space, and still receive requests, since the runtime should do
        // most specific match first
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = UUID.randomUUID().toString();
        Operation patch = Operation.createPatch(s2.getUri())
                .setCompletion(this.host.getCompletion())
                .setBody(body);
        this.host.sendAndWait(patch);

        // verify state updated
        MinimalTestServiceState st = this.host.getServiceState(null, MinimalTestServiceState.class,
                s2.getUri());
        assertEquals(body.id, st.id);

        CompletionHandler c = (o, e) -> {
            if (e != null) {
                this.host.failIteration(e);
                return;
            }

            ServiceDocument d = o.getBody(ServiceDocument.class);
            if (!s1.getSelfLink().equals(d.documentSelfLink)) {
                this.host.failIteration(new IllegalStateException(
                        "Wrong service replied: " + d.documentSelfLink));

            } else {
                this.host.completeIteration();
            }
        };

        // verify that the uri namespace owner with the longest match takes priority (s1 service)
        Operation put = Operation.createPut(s1.getUri())
                .setBody(new ServiceDocument())
                .setCompletion(c);

        this.host.testStart(1);
        this.host.send(put);
        this.host.testWait();

        List<Service> namespaceOwners = new ArrayList<>();
        namespaceOwners.add(sParent);
        namespaceOwners.add(s1);
        namespaceOwners.add(s);

        for (Service nsOwner : namespaceOwners) {
            List<URI> uris = new ArrayList<>();
            // build some example child URIs. Do not include one with exact prefix, since
            // that will be tested separately
            uris.add(UriUtils.extendUri(nsOwner.getUri(), "/1"));
            uris.add(UriUtils.extendUri(nsOwner.getUri(), "/1/"));
            uris.add(UriUtils.extendUri(nsOwner.getUri(), "/1?k=v&k1=v1"));
            uris.add(UriUtils.extendUri(nsOwner.getUri(), "/1/2/3"));
            uris.add(UriUtils.extendUri(nsOwner.getUri(), "/1/2/3/?k=v&k1=v1"));
            uris.add(UriUtils.extendUri(nsOwner.getUri(), "/1/2/3/?k=v&k1=v1"));

            EnumSet<Action> actions = EnumSet.allOf(Action.class);
            verifyAllActions(uris, actions, false);

            // these should all fail, do not start with prefix
            uris.clear();
            uris.add(UriUtils.extendUri(this.host.getUri(), "/1"));
            uris.add(UriUtils.extendUri(this.host.getUri(), "/1/"));
            uris.add(UriUtils.extendUri(this.host.getUri(), "/1?k=v&k1=v1"));
            uris.add(UriUtils.extendUri(this.host.getUri(), "/1/2/3"));
            uris.add(UriUtils.extendUri(this.host.getUri(), "/1/2/3/?k=v&k1=v1"));
            uris.add(UriUtils.extendUri(this.host.getUri(), "/1/2/3/?k=v&k1=v1"));
            verifyAllActions(uris, actions, true);
        }

        verifyDeleteOnNamespaceOwner(s);
        verifyDeleteOnNamespaceOwner(sParent);
        verifyDeleteOnNamespaceOwner(s1);

    }

    private void verifyDeleteOnNamespaceOwner(PrefixDispatchService s) throws Throwable {
        // finally, verify we can actually target the service itself, using a DELETE
        Operation delete = Operation.createDelete(s.getUri())
                .setCompletion(this.host.getCompletion());
        this.host.testStart(1);
        this.host.send(delete);
        this.host.testWait();

        assertTrue(this.host.getServiceStage(s.getSelfLink()) == null);
    }

    private void verifyAllActions(List<URI> uris, EnumSet<Action> actions, boolean expectFailure)
            throws Throwable {
        CompletionHandler c = expectFailure ? this.host.getExpectedFailureCompletion()
                : this.host.getCompletion();
        this.host.testStart(actions.size() * uris.size());
        for (Action a : actions) {
            for (URI u : uris) {
                this.host.log("Trying %s on %s", a, u);
                Operation op = Operation.createGet(u)
                        .setAction(a)
                        .setCompletion(c);

                if (a != Action.GET && a != Action.OPTIONS) {
                    op.setBody(new ServiceDocument());
                }
                this.host.send(op);
            }
        }
        this.host.testWait();
    }

    @Test
    public void options() throws Throwable {
        URI serviceUri = UriUtils.buildUri(this.host, UriUtils.buildUriPath(ServiceUriPaths.CORE, "test-service"));
        MinimalTestServiceState state = new MinimalTestServiceState();
        state.id = UUID.randomUUID().toString();

        CompletionHandler c = (o, e) -> {
            if (e != null) {
                this.host.failIteration(e);
                return;
            }

            ServiceDocumentQueryResult res = o.getBody(ServiceDocumentQueryResult.class);
            if (res.documents != null) {
                this.host.completeIteration();
                return;
            }
            ServiceDocument doc = o.getBody(ServiceDocument.class);
            if (doc.documentDescription != null) {
                this.host.completeIteration();
                return;
            }

            this.host.failIteration(new IllegalStateException("expected description"));
        };

        this.host.startServiceAndWait(new MinimalTestService(), serviceUri.getPath(), state);
        this.host.testStart(1);
        this.host.send(Operation.createOperation(Action.OPTIONS, serviceUri)
                .setCompletion(c));
        this.host.testWait();

        // try also on a stateless service like the example factory
        serviceUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        this.host.testStart(1);
        this.host.send(Operation.createOperation(Action.OPTIONS, serviceUri)
                .setCompletion(c));
        this.host.testWait();
    }

    public static class PeriodicMaintenanceTestStatelessService extends StatelessService {
        public PeriodicMaintenanceTestStatelessService() {
            this.setMaintenanceIntervalMicros(
                    TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS * 3)
            );
            this.toggleOption(ServiceOption.INSTRUMENTATION, true);
            this.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        }

        @Override
        public void handlePeriodicMaintenance(Operation post) {
            doHandlePeriodicMaintenanceImpl(this, post);
        }
    }

    public static class PeriodicMaintenanceTestStatefulService extends StatefulService {
        public PeriodicMaintenanceTestStatefulService() {
            super(ServiceDocument.class);
            this.setMaintenanceIntervalMicros(
                    TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS * 3)
            );
            this.toggleOption(ServiceOption.INSTRUMENTATION, true);
            this.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        }

        @Override
        public void handlePeriodicMaintenance(Operation post) {
            doHandlePeriodicMaintenanceImpl(this, post);
        }
    }

    private static void doHandlePeriodicMaintenanceImpl(Service s, Operation post) {
        ServiceMaintenanceRequest request = post.getBody(ServiceMaintenanceRequest.class);
        if (!request.reasons.contains(
                ServiceMaintenanceRequest.MaintenanceReason.PERIODIC_SCHEDULE)) {
            post.fail(new IllegalArgumentException("expected PERIODIC_SCHEDULE reason"));
            return;
        }

        post.complete();

        ServiceStat stat = s.getStat(STAT_NAME_HANDLE_PERIODIC_MAINTENANCE);
        s.adjustStat(stat, 1);
        if (stat.latestValue >= PERIODIC_MAINTENANCE_MAX) {
            s.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
        }
    }

    @Test
    public void periodicMaintenance() throws Throwable {
        try {
            this.host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(50));
            // Check StatelessService
            doCheckPeriodicMaintenance(new PeriodicMaintenanceTestStatelessService());

            // Check StatefulService
            doCheckPeriodicMaintenance(new PeriodicMaintenanceTestStatefulService());

            // Check StatelessService with dynamic toggle
            Service s = new PeriodicMaintenanceTestStatelessService();
            s.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
            doCheckPeriodicMaintenance(s);

            // Check StatefulService with dynamic toggle
            s = new PeriodicMaintenanceTestStatefulService();
            s.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
            doCheckPeriodicMaintenance(s);
        } finally {
            this.host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(
                    VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        }
    }

    private void doCheckPeriodicMaintenance(Service s) throws Throwable {
        // Start service
        Service service = this.host.startServiceAndWait(s, UUID.randomUUID().toString(), null);

        int expectedMaintCount = PERIODIC_MAINTENANCE_MAX;
        if (!s.hasOption(ServiceOption.PERIODIC_MAINTENANCE)) {
            this.host.log("Toggling %s on", ServiceOption.PERIODIC_MAINTENANCE);
            this.host.toggleServiceOptions(s.getUri(),
                    EnumSet.of(ServiceOption.PERIODIC_MAINTENANCE), null);
            expectedMaintCount = 1;
        }

        this.host.log("waiting for maintenance stat increment, expecting %d repeats",
                expectedMaintCount);
        final int limit = expectedMaintCount;
        this.host.waitFor("maint. count incorrect", () -> {
            ServiceStat stat = service.getStat(STAT_NAME_HANDLE_PERIODIC_MAINTENANCE);
            if (stat.latestValue < limit) {
                return false;
            }
            return true;
        });
    }

    @Test
    public void getStatelessServiceOperationStats() throws Throwable {
        MinimalFactoryTestService factoryService = new MinimalFactoryTestService();
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = UUID.randomUUID().toString();
        this.host.startServiceAndWait(factoryService, UUID.randomUUID().toString(), body);
        // try a post on the factory service and assert that the stats are collected for the post operation.
        this.host.waitFor("stats not found", () -> {
            Operation post = Operation.createPost(factoryService.getUri())
                    .setBody(body);
            Operation response = this.host.waitForResponse(post);
            assertNotNull(response);
            // the stat is updated after the operation is completed, so we might miss the initial
            // stat
            ServiceStats testStats = host.getServiceState(null, ServiceStats.class, UriUtils
                    .buildStatsUri(factoryService.getUri()));
            if (testStats == null) {
                return false;
            }

            ServiceStat serviceStat = testStats.entries
                    .get(Action.POST + STAT_NAME_OPERATION_DURATION);
            if (serviceStat == null || serviceStat.latestValue == 0) {
                return false;
            }
            host.log(Utils.toJsonHtml(testStats));
            return true;
        });
    }

}
