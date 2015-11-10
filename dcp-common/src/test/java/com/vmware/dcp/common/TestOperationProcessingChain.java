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

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.Service.Action;
import com.vmware.dcp.common.Service.OperationProcessingStage;
import com.vmware.dcp.common.TestOperationProcessingChain.CounterService.CounterServiceRequest;
import com.vmware.dcp.common.TestOperationProcessingChain.CounterService.CounterServiceState;
import com.vmware.dcp.common.test.VerificationHost;
import com.vmware.dcp.services.common.ServiceUriPaths;

public class TestOperationProcessingChain extends BasicTestCase {

    static final int COUNT = 10;

    public static class OperationLogger implements Predicate<Operation> {
        @Override
        public boolean test(Operation op) {
            Utils.log(getClass(), getClass().getName(), Level.INFO, "Operation: %s", op);
            return true;
        }
    }

    public static class OperationSilentPatchDropper implements Predicate<Operation> {
        @Override
        public boolean test(Operation op) {
            return Action.PATCH != op.getAction();
        }
    }

    public static class OperationNextFiltersBypasser implements Predicate<Operation> {
        private Service service;

        public OperationNextFiltersBypasser(Service service) {
            this.service = service;
        }

        @Override
        public boolean test(Operation op) {
            this.service.getHost().run(() -> {
                this.service.handleRequest(op,
                        OperationProcessingStage.EXECUTING_SERVICE_HANDLER);

            });
            return false;
        }
    }

    public static class CounterFactoryService extends FactoryService {
        public static final String SELF_LINK = ServiceUriPaths.SAMPLES + "/counter";

        public CounterFactoryService() {
            super(CounterService.CounterServiceState.class);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new CounterService();
        }
    }

    public static class CounterService extends StatefulService {
        public static final String DEFAULT_SELF_LINK = "default";

        public static class CounterServiceState extends ServiceDocument {
            public int counter;
        }

        public static class CounterServiceRequest {
            public int incrementBy;
        }

        public CounterService() {
            super(CounterServiceState.class);
            toggleOption(ServiceOption.PERSISTENCE, true);
        }

        @Override
        public void handlePatch(Operation patch) {
            CounterServiceState currentState = getState(patch);
            CounterServiceRequest body = patch.getBody(CounterServiceRequest.class);
            currentState.counter += body.incrementBy;
            patch.setBody(currentState);
            patch.complete();
        }

    }

    @Override
    public void beforeHostStart(VerificationHost host) {
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(100));
    }

    @Before
    public void setUp() throws Exception {
        try {
            this.host.startServiceAndWait(CounterFactoryService.class,
                    CounterFactoryService.SELF_LINK);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCounterServiceWithOperationFilters() throws Throwable {
        Service counterService = createCounterService();
        OperationProcessingChain opProcessingChain = new OperationProcessingChain();

        opProcessingChain.add(new OperationLogger());
        counterService.setOperationProcessingChain(opProcessingChain);
        for (int i = 0; i < COUNT; i++) {
            incrementCounter(false);
        }
        int counter = getCounter();
        assertEquals(COUNT, counter);

        this.host.setOperationTimeOutMicros(TimeUnit.MILLISECONDS.toMicros(250));
        opProcessingChain.add(new OperationSilentPatchDropper());
        incrementCounter(true);

        counter = getCounter();
        assertEquals(COUNT, counter);
    }

    @Test
    public void testCounterServiceJumpOperationProcessingStage() throws Throwable {
        Service counterService = createCounterService();
        OperationProcessingChain opProcessingChain = new OperationProcessingChain();
        opProcessingChain = new OperationProcessingChain();
        opProcessingChain.add(new OperationLogger());
        opProcessingChain.add(new OperationNextFiltersBypasser(counterService));
        opProcessingChain.add(new OperationSilentPatchDropper());
        counterService.setOperationProcessingChain(opProcessingChain);

        for (int i = 0; i < COUNT; i++) {
            incrementCounter(false);
        }
        int counter = getCounter();
        assertEquals(COUNT, counter);
    }

    private Service createCounterService() throws Throwable {
        this.host.testStart(1);
        URI counterServiceFactoryUri = UriUtils.buildUri(this.host, CounterFactoryService.class);
        CounterServiceState initialState = new CounterServiceState();
        initialState.documentSelfLink = CounterService.DEFAULT_SELF_LINK;
        Operation post = Operation.createPost(counterServiceFactoryUri).setBody(initialState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.send(post);
        this.host.testWait();

        return this.host.findService(getDefaultCounterServiceUriPath());
    }

    private void incrementCounter(boolean expectFailure) throws Throwable {
        this.host.testStart(1);
        URI counterServiceUri = UriUtils.buildUri(this.host, getDefaultCounterServiceUriPath());
        CounterServiceRequest body = new CounterServiceRequest();
        body.incrementBy = 1;
        Operation patch = Operation.createPatch(counterServiceUri)
                .forceRemote()
                .setBody(body)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        if (expectFailure) {
                            this.host.completeIteration();
                        } else {
                            this.host.failIteration(e);
                        }
                        return;
                    }

                    if (expectFailure) {
                        this.host.failIteration(e);
                    } else {
                        this.host.completeIteration();
                    }
                });
        this.host.send(patch);
        this.host.testWait();
    }

    private int getCounter() throws Throwable {
        this.host.testStart(1);
        URI counterServiceUri = UriUtils.buildUri(this.host, getDefaultCounterServiceUriPath());
        int[] counters = new int[1];
        Operation get = Operation.createGet(counterServiceUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    CounterServiceState state = o.getBody(CounterServiceState.class);
                    counters[0] = state.counter;
                    this.host.completeIteration();
                });
        this.host.send(get);
        this.host.testWait();

        return counters[0];
    }

    private String getDefaultCounterServiceUriPath() {
        return CounterFactoryService.SELF_LINK + "/" + CounterService.DEFAULT_SELF_LINK;
    }

}
