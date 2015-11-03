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

import java.net.URI;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.Service.ServiceOption;
import com.vmware.dcp.common.ServiceStats.ServiceStat;
import com.vmware.dcp.common.test.MinimalTestServiceState;
import com.vmware.dcp.common.test.TestProperty;
import com.vmware.dcp.services.common.ExampleFactoryService;
import com.vmware.dcp.services.common.ExampleService.ExampleServiceState;
import com.vmware.dcp.services.common.MinimalTestService;
import com.vmware.dcp.services.common.ServiceUriPaths;

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
            delete.fail(
                    new IllegalStateException("Linked state must be null in expiration DELETE"));
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

public class TestStatefulService extends BasicReusableHostTestCase {
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

        this.host.testStart(services.size());
        for (Service s : services) {
            ServiceConfigUpdateRequest body = ServiceConfigUpdateRequest.create();
            body.operationQueueLimit = (int) c;
            URI configUri = UriUtils.buildConfigUri(s.getUri());
            this.host.send(Operation.createPatch(configUri).setBody(body)
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        this.host.doPutPerService(c, props, services);
        this.host.doPutPerService(c, props, services);
        this.host.doPutPerService(c, props, services);
    }

    @Test
    public void throughputInMemoryStrictUpdateCheckingServiceRemotePut() throws Throwable {
        int serviceCount = 4;
        int updateCount = 20;
        List<Service> services = this.host.doThroughputServiceStart(
                serviceCount, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.of(Service.ServiceOption.STRICT_UPDATE_CHECKING), null);
        List<Service> durableServices = this.host.doThroughputServiceStart(
                serviceCount, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.of(ServiceOption.STRICT_UPDATE_CHECKING, ServiceOption.PERSISTENCE), null);

        this.host.log("starting remote test");
        for (int i = 0; i < 3; i++) {
            this.host.doPutPerService(updateCount, EnumSet.of(TestProperty.FORCE_REMOTE),
                    services);
            this.host.doPutPerService(updateCount, EnumSet.of(TestProperty.FORCE_REMOTE),
                    durableServices);
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
                } , factoryService.getUri());

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
    public void operationQueueLimit() throws Throwable {
        Service lifoService = new MinimalTestService();
        lifoService.toggleOption(ServiceOption.LIFO_QUEUE, true);
        lifoService = this.host.startServiceAndWait(lifoService, UUID.randomUUID().toString(),
                null);

        Service fifoService = new MinimalTestService();
        fifoService = this.host.startServiceAndWait(lifoService, UUID.randomUUID().toString(),
                null);

        int limit = 2;
        this.host.log("Verifying LIFO service");
        this.host.setOperationQueueLimit(lifoService.getUri(), limit);
        verifyOperationQueueLimit(lifoService.getUri(), limit);

        this.host.log("Verifying FIFO service");
        this.host.setOperationQueueLimit(fifoService.getUri(), limit);
        verifyOperationQueueLimit(fifoService.getUri(), limit);
    }

    private void verifyOperationQueueLimit(URI serviceUri, int limit) throws Throwable {
        // testing that limit was applied is tricky: the runtime can process over 1M ops/sec on a
        // modern machine, so we need to make sure we issue enough that some fail before the queue is
        // serviced below the limit. Either way we must ensure all operations complete, with at least
        // one of them failing with the proper error

        AtomicInteger cancelledOpCount = new AtomicInteger();
        int count = 100;
        MinimalTestServiceState body = (MinimalTestServiceState) this.host.buildMinimalTestState();
        body.id = MinimalTestService.STRING_MARKER_DELAY_COMPLETION;
        Operation patch = Operation.createPatch(serviceUri)
                .setBody(body)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        if (o.getStatusCode() != Operation.STATUS_CODE_UNAVAILABLE) {
                            this.host.failIteration(
                                    new IllegalStateException("unexpected status code"));
                            return;
                        }
                        String retrySeconds = o.getResponseHeader(Operation.RETRY_AFTER_HEADER);
                        if (retrySeconds == null || Integer.parseInt(retrySeconds) < 1) {
                            this.host.failIteration(
                                    new IllegalStateException("missing or unexpected retry-after"));
                            return;
                        }

                        cancelledOpCount.incrementAndGet();
                        this.host.completeIteration();
                        return;
                    }

                    this.host.completeIteration();
                });

        this.host.testStart(count);
        for (int i = 0; i < count; i++) {
            this.host.send(patch);
        }
        this.host.testWait();

        this.host.log("Ops cancelled: %d", cancelledOpCount.get());
        if (cancelledOpCount.get() < limit / 20) {
            throw new IllegalStateException("not enough operations where cancelled");
        }

        // make sure no operations are cancelled if we are below the limit
        this.host.testStart(limit - 1);
        for (int i = 0; i < limit - 1; i++) {
            this.host.send(patch.setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

    }
}
