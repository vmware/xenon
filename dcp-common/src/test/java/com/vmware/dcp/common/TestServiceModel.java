/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.Service.Action;
import com.vmware.dcp.common.Service.ServiceOption;
import com.vmware.dcp.common.ServiceStats.ServiceStat;
import com.vmware.dcp.common.test.MinimalTestServiceState;
import com.vmware.dcp.common.test.TestProperty;
import com.vmware.dcp.common.test.VerificationHost;
import com.vmware.dcp.services.common.ExampleFactoryService;
import com.vmware.dcp.services.common.ExampleService.ExampleServiceState;
import com.vmware.dcp.services.common.MinimalFactoryTestService;
import com.vmware.dcp.services.common.MinimalTestService;
import com.vmware.dcp.services.common.ServiceUriPaths;

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

class DeleteVerificationTestService extends StatefulService {

    public DeleteVerificationTestService() {
        super(ExampleServiceState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleDelete(Operation delete) {
        if (!delete.hasBody()) {
            delete.fail(new IllegalStateException("Expected service state in expiration DELETE"));
            return;
        }

        ExampleServiceState state = delete.getBody(ExampleServiceState.class);
        if (state.name == null) {
            delete.fail(new IllegalStateException("Invalid service state in expiration DELETE"));
            return;
        }

        if (getState(delete) != null) {
            delete.fail(new IllegalStateException("Linked state must be null in expiration DELETE"));
            return;
        }
        ServiceStat s = new ServiceStat();
        s.name = getSelfLink();
        s.latestValue = 1;
        URI factoryStats = UriUtils.buildStatsUri(UriUtils.buildUri(getHost(),
                DeleteVerificationTestFactoryService.class));
        sendRequest(Operation.createPost(factoryStats).setBody(s));
        delete.complete();
    }
}

class DeleteVerificationTestFactoryService extends FactoryService {
    public static final String SELF_LINK = ServiceUriPaths.CORE + "/tests/deleteverification";

    public DeleteVerificationTestFactoryService() {
        super(ExampleServiceState.class);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        Service s = new DeleteVerificationTestService();
        return s;
    }
}

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

public class TestServiceModel {

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

    public VerificationHost host;


    @Before
    public void setUp() throws Exception {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = VerificationHost.create(0, null);
        this.host.setStressTest(this.isStressTest);

        try {
            this.host.start();
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

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
    public void factoryWithChildServiceStateTypeMismatch() {
        this.host.toggleNegativeTestMode(true);
        Operation post = Operation
                .createPost(UriUtils.buildUri(this.host, UUID.randomUUID().toString()))
                .setCompletion(this.host.getExpectedFailureCompletion());
        this.host.startService(post, new TypeMismatchTestFactoryService());
        this.host.toggleNegativeTestMode(false);
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
                stateWithContextId.getContextId, false);
        doOperationWithContextId(servicesWithContextId, Action.GET,
                stateWithContextId.getContextId, true);
        doOperationWithContextId(servicesWithOutContextId, Action.GET, null, false);
        doOperationWithContextId(servicesWithOutContextId, Action.GET, null, true);
        this.host.testWait();

        // test put
        this.host.testStart(serviceCount * 4);
        doOperationWithContextId(servicesWithContextId, Action.PUT,
                stateWithContextId.putContextId, false);
        doOperationWithContextId(servicesWithContextId, Action.PUT,
                stateWithContextId.putContextId, true);
        doOperationWithContextId(servicesWithOutContextId, Action.PUT, null, false);
        doOperationWithContextId(servicesWithOutContextId, Action.PUT, null, true);
        this.host.testWait();

        // test patch
        this.host.testStart(serviceCount * 2);
        doOperationWithContextId(servicesWithContextId, Action.PATCH,
                stateWithContextId.patchContextId, false);
        doOperationWithContextId(servicesWithOutContextId, Action.PATCH, null, false);
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
            String contextId, boolean useCallback) {
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

            op
                    .forceRemote()
                    .setBody(new ContextIdTestService.State())
                    .setContextId(contextId)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }

                        this.host.completeIteration();
                    });

            if (useCallback) {
                this.host.sendRequestWithCallback(op.setReferer(this.host.getReferer()));
            } else {
                this.host.send(op);
            }
        }
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
    public void throughputDurableServiceStart() throws Throwable {
        long c = this.serviceCount;
        if (c < 1) {
            c = this.host.computeIterationsFromMemory(1);
        }
        this.host.doThroughputServiceStart(c, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.of(ServiceOption.PERSISTENCE), null);
        this.host.doThroughputServiceStart(c, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.of(ServiceOption.PERSISTENCE), null);
    }

    @Test
    public void serviceStopWithInflightRequests() throws Throwable {
        long c = 100;

        this.host.waitForServiceAvailable(ExampleFactoryService.SELF_LINK);

        List<Service> services = this.host.doThroughputServiceStart(c,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.of(ServiceOption.PERSISTENCE), null);

        ExampleServiceState body = new ExampleServiceState();
        body.name = UUID.randomUUID().toString();

        // we want to verify that a service and service host will either complete or fail all
        // requests sent to it even if its in the process of being stopped

        // first send a PATCH that will induce document expiration, and in parallel, issue more
        // DELETEs, PATCHs, etc. We expect
        // failure on most of them, what we do not want to see is a timeout...
        body.documentExpirationTimeMicros = 1;
        for (Service s : services) {
            this.host.send(Operation.createPatch(s.getUri()).setBody(body));
        }
        c = 10;

        CompletionHandler ch = this.host.getSuccessOrFailureCompletion();

        this.host.setTimeoutSeconds(20);
        this.host.toggleNegativeTestMode(true);
        this.host.testStart(c * 4 * services.size());
        for (Service s : services) {
            for (int i = 0; i < c; i++) {
                this.host.send(Operation.createPatch(s.getUri()).setBody(body).setCompletion(ch));
                if (i >= 0) {
                    this.host.send(Operation.createDelete(s.getUri()).setBody(body)
                            .setCompletion(ch));
                } else {
                    this.host.send(Operation.createDelete(s.getUri()).setBody(body)
                            .setCompletion(ch)
                            .forceRemote());
                }
                this.host.send(Operation.createPut(s.getUri()).setBody(body).setCompletion(ch));
                this.host.send(Operation.createGet(s.getUri()).setCompletion(ch));
            }
        }
        this.host.testWait();
        this.host.toggleNegativeTestMode(false);
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
    public void throughputInMemoryServicePutConcurrentSend()
            throws Throwable {
        EnumSet<TestProperty> props = EnumSet.of(TestProperty.CONCURRENT_SEND);
        Class<? extends StatefulService> type = MinimalTestService.class;
        EnumSet<Service.ServiceOption> caps = EnumSet
                .noneOf(Service.ServiceOption.class);

        doThroughputPutTest(props, type, caps);
    }

    @Test
    public void throughputInMemoryServicePut() throws Throwable {
        EnumSet<TestProperty> props = EnumSet.noneOf(TestProperty.class);
        Class<? extends StatefulService> type = MinimalTestService.class;
        EnumSet<Service.ServiceOption> caps = EnumSet
                .noneOf(Service.ServiceOption.class);
        doThroughputPutTest(props, type, caps);
    }

    @Test
    public void throughputInMemoryInstrumentedServicePut() throws Throwable {
        EnumSet<TestProperty> props = EnumSet.noneOf(TestProperty.class);
        Class<? extends StatefulService> type = MinimalTestService.class;
        EnumSet<Service.ServiceOption> caps = EnumSet
                .of(Service.ServiceOption.INSTRUMENTATION);
        doThroughputPutTest(props, type, caps);
    }

    private void doThroughputPutTest(EnumSet<TestProperty> props,
            Class<? extends StatefulService> type,
            EnumSet<Service.ServiceOption> caps)
            throws Throwable {
        long sc = this.serviceCount;
        if (sc < 1) {
            sc = 16;
        }
        // start services
        List<Service> services = this.host.doThroughputServiceStart(
                sc, type, this.host.buildMinimalTestState(), caps, null);

        long c = this.requestCount;
        if (c < 1) {
            c = this.host.computeIterationsFromMemory((int) sc);
        }
        this.host.doPutPerService(c, props, services);
        this.host.doPutPerService(c, props, services);
        this.host.doPutPerService(c, props, services);
    }

    @Test
    public void throughputInMemoryStrictUpdateCheckingServiceRemotePut() throws Throwable {
        int serviceCount = 4;
        int updateCount = 100;
        List<Service> services = this.host.doThroughputServiceStart(
                serviceCount, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.of(Service.ServiceOption.STRICT_UPDATE_CHECKING), null);
        List<Service> durableServices = this.host.doThroughputServiceStart(
                serviceCount, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.of(ServiceOption.STRICT_UPDATE_CHECKING, ServiceOption.PERSISTENCE), null);

        this.host.log("starting Local test");
        for (int i = 0; i < 3; i++) {
            this.host.doPutPerService(updateCount, EnumSet.noneOf(TestProperty.class),
                    durableServices);
            this.host.doPutPerService(updateCount, EnumSet.noneOf(TestProperty.class),
                    services);
        }

        this.host.log("starting remote test");
        for (int i = 0; i < 3; i++) {
            this.host.doPutPerService(updateCount, EnumSet.of(TestProperty.FORCE_REMOTE),
                    services);
            this.host.doPutPerService(updateCount, EnumSet.of(TestProperty.FORCE_REMOTE),
                    services);
        }

        this.host.log("starting expected failure test");
        this.host.toggleNegativeTestMode(true);
        int count = 2;
        this.host.doPutPerService(count,
                EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.FORCE_FAILURE),
                services);
        this.host.doPutPerService(count,
                EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.FORCE_FAILURE),
                durableServices);

        this.host.toggleNegativeTestMode(false);
    }

    @Test
    public void remotePutNotModified() throws Throwable {
        int serviceCount = 10;
        List<Service> services = this.host.doThroughputServiceStart(
                serviceCount, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);

        MinimalTestServiceState body = (MinimalTestServiceState) this.host.buildMinimalTestState();
        for (int pass = 0; pass < 2; pass++) {
            this.host.testStart(serviceCount);
            for (Service s : services) {
                final int finalPass = pass;
                Operation put = Operation
                        .createPatch(s.getUri())
                        .forceRemote()
                        .setBody(body)
                        .setCompletion((o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            if (finalPass == 1
                                    && o.getStatusCode() != Operation.STATUS_CODE_NOT_MODIFIED) {
                                this.host.failIteration(new IllegalStateException(
                                        "Expected not modified status"));
                                return;
                            }

                            this.host.completeIteration();
                        });

                this.host.send(put);
            }
            this.host.testWait();
        }

    }

    @Test
    public void expirationInducedDeleteHandlerVerification()
            throws Throwable {
        long count = 10;
        DeleteVerificationTestFactoryService f = new DeleteVerificationTestFactoryService();

        DeleteVerificationTestFactoryService factoryService = (DeleteVerificationTestFactoryService) this.host
                .startServiceAndWait(f, DeleteVerificationTestFactoryService.SELF_LINK, null);

        Map<URI, ExampleServiceState> services = this.host.doFactoryChildServiceStart(null, count,
                ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState s = new ExampleServiceState();
                    s.name = UUID.randomUUID().toString();
                    s.documentExpirationTimeMicros = Utils.getNowMicrosUtc();
                    o.setBody(s);
                }, factoryService.getUri());

        // services should expire, and we will confirm the delete handler was called. We only expire when we try to access
        // a document, so do a factory get ...

        this.host.getServiceState(null, ServiceDocumentQueryResult.class, factoryService.getUri());

        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            Set<String> deletedServiceStats = new HashSet<>();
            ServiceStats factoryStats = this.host.getServiceState(null, ServiceStats.class,
                    UriUtils.buildStatsUri(factoryService.getUri()));
            for (String statName : factoryStats.entries.keySet()) {
                if (statName.startsWith(DeleteVerificationTestFactoryService.SELF_LINK)) {
                    deletedServiceStats.add(statName);
                }
            }
            if (deletedServiceStats.size() == services.size()) {
                return;
            }
            Thread.sleep(100);
        }

        throw new TimeoutException();
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
    public void getDocumentTemplate() throws Throwable {
        URI uri = UriUtils.buildUri(this.host, "testGetDocumentInstance");

        // starting the service will call getDocumentTemplate - which should throw a RuntimeException, which causes
        // post to fail.
        Operation post = Operation.createPost(uri);
        this.host.startService(post, new GetIllegalDocumentService());
        assertEquals(500, post.getStatusCode());
        assertTrue(post.getBody(ServiceErrorResponse.class).message.contains("myLink"));
    }

    @Test
    public void options() throws Throwable {
        URI serviceUri = UriUtils.buildUri(this.host, UriUtils.buildUriPath(ServiceUriPaths.CORE, "test-service"));
        MinimalTestServiceState state = new MinimalTestServiceState();
        state.id = UUID.randomUUID().toString();
        this.host.startServiceAndWait(new MinimalTestService(), serviceUri.getPath(), state);
        this.host.testStart(1);
        this.host.sendRequest(Operation.createOperation(Action.OPTIONS, serviceUri)
                .setCompletion((o, e) -> this.host.completeIteration()));
        this.host.testWait();
    }

    @After
    public void tearDown() {
        this.host.tearDown();
    }

}
