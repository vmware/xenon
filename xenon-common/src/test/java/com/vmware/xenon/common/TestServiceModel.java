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

import java.net.URI;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.services.common.MinimalFactoryTestService;
import com.vmware.xenon.services.common.MinimalTestService;
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

public class TestServiceModel extends BasicTestCase {

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
}
