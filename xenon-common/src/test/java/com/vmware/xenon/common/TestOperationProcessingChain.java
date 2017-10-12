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

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

import io.opentracing.ActiveSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalActiveSpanSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import com.vmware.xenon.common.OperationProcessingChain.Filter;
import com.vmware.xenon.common.OperationProcessingChain.FilterReturnCode;
import com.vmware.xenon.common.OperationProcessingChain.OperationProcessingContext;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.OperationProcessingStage;
import com.vmware.xenon.common.TestOperationProcessingChain.CounterService.CounterServiceRequest;
import com.vmware.xenon.common.TestOperationProcessingChain.CounterService.CounterServiceState;
import com.vmware.xenon.common.filters.LambdaFilter;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestOperationProcessingChain extends BasicTestCase {

    static final int COUNT = 10;

    public static class OperationLogger implements Filter {
        @Override
        public FilterReturnCode processRequest(Operation op, OperationProcessingContext context) {
            Utils.log(getClass(), getClass().getName(), Level.INFO, "Operation: %s", op);
            return FilterReturnCode.CONTINUE_PROCESSING;
        }
    }

    public static class OperationPatchDropper implements Filter {
        @Override
        public FilterReturnCode processRequest(Operation op, OperationProcessingContext context) {
            if (Action.PATCH == op.getAction()) {
                op.fail(new IllegalArgumentException());
                return FilterReturnCode.FAILED_STOP_PROCESSING;
            }

            return FilterReturnCode.CONTINUE_PROCESSING;
        }
    }

    public static class OperationNextFiltersBypasser implements Filter {
        private Service service;

        public OperationNextFiltersBypasser(Service service) {
            this.service = service;
        }

        @Override
        public FilterReturnCode processRequest(Operation op, OperationProcessingContext context) {
            this.service.getHost().run(() -> {
                this.service.handleRequest(op,
                        OperationProcessingStage.EXECUTING_SERVICE_HANDLER);

            });
            return FilterReturnCode.SUSPEND_PROCESSING;
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
        OperationProcessingChain opProcessingChain = OperationProcessingChain.create(
                new OperationLogger());
        counterService.setOperationProcessingChain(opProcessingChain);
        for (int i = 0; i < COUNT; i++) {
            incrementCounter(false);
        }
        int counter = getCounter();
        assertEquals(COUNT, counter);

        this.host.setOperationTimeOutMicros(TimeUnit.MILLISECONDS.toMicros(250));
        opProcessingChain = OperationProcessingChain.create(
                new OperationLogger(),
                new OperationPatchDropper());
        counterService.setOperationProcessingChain(opProcessingChain);
        incrementCounter(true);

        counter = getCounter();
        assertEquals(COUNT, counter);
    }

    @Test
    public void testCounterServiceJumpOperationProcessingStage() throws Throwable {
        Service counterService = createCounterService();
        OperationProcessingChain opProcessingChain = OperationProcessingChain.create(
                new OperationLogger(),
                new OperationNextFiltersBypasser(counterService),
                new OperationPatchDropper());
        counterService.setOperationProcessingChain(opProcessingChain);

        for (int i = 0; i < COUNT; i++) {
            incrementCounter(false);
        }
        int counter = getCounter();
        assertEquals(COUNT, counter);
    }

    @Test
    public void testFiltersWithTracing() throws Throwable {
        // Model for spans and filters:
        // filters proceed forward
        // state is aggregated on the operation
        // filters that spin out separate work with its own span should work
        //   - but we don't need to have multiple filters replacing the span
        //     for future filters
        // To make the chain unaware of tracing, we need to have a callout around each invocation of a filter
        // which we'd use to set/clear the tracing context; and that would need to have its own abstract state in
        // OperationProcessingContext - at a level that is getting very awkward.
        // So we split the difference: we teach OperationProcessingChain how to work with spans, but
        // we don't make them itself.

        //
        // Use cases:
        // A span creating filter needs to be able to:
        // create a span - refcount +1
        //   easy: has access to the operation
        // handoff the span to the operation for gathering the http status code
        //   easy: nestCompletion on the operation
        //   easy: try/finally in the filter stage
        // have that picked up (via activeSpan) by the next filter
        //   - both after CONTINUE_PROCESSING
        //   - and SUSPEND_PROCESSING
        // clear the span at the end of the filter process no matter how we exit

        // We'll need a tracer to provide spans and to validate against
        MockTracer tracer = new MockTracer(new ThreadLocalActiveSpanSource(), MockTracer.Propagator.TEXT_MAP);

        // Integration checks to make:
        // A - filter that sets a span results in span being seen from next filter in sync mode.
        // B - after suspending no active span is set - its contained.
        // C - resumed filter can see the span.
        // D - completion function can see the span
        //
        // E, F - after stop/fail the span is emitted
        Filter makeSpan = new LambdaFilter((op, context) -> {
            Assert.assertEquals(null, tracer.activeSpan());
            // Make span: e.g. new request received.
            try (ActiveSpan span = tracer.buildSpan("outer").startActive()) {
                context.setSpan(span);
                return FilterReturnCode.CONTINUE_PROCESSING;
            }
        });
        Filter checkAndSuspend = new LambdaFilter((op, context) -> {
            // Case A
            Assert.assertNotEquals(null, tracer.activeSpan());
            return FilterReturnCode.SUSPEND_PROCESSING;
        });
        Filter checkAndContinue = new LambdaFilter((op, context) -> {
            // Case C
            Assert.assertNotEquals(null, tracer.activeSpan());
            return FilterReturnCode.CONTINUE_PROCESSING;
        });
        Filter checkAndFail = new LambdaFilter((op, context) -> {
            Assert.assertNotEquals(null, tracer.activeSpan());
            return FilterReturnCode.FAILED_STOP_PROCESSING;
        });
        Filter checkAndStop = new LambdaFilter((op, context) -> {
            Assert.assertNotEquals(null, tracer.activeSpan());
            return FilterReturnCode.SUCCESS_STOP_PROCESSING;
        });
        // completions should always see the span we test with.
        Consumer<Operation> completion = o -> {
            // Case D
            Assert.assertNotEquals(null, tracer.activeSpan());
        };
        Runnable assertEmitted = () -> {
            // At this point the span must have been emitted and no span should be active.
            Assert.assertEquals(null, tracer.activeSpan());
            Assert.assertEquals(1, tracer.finishedSpans().size());
        };
        Runnable assertNotEmitted = () -> {
            // At this point the span must have been emitted and no span should be active.
            Assert.assertEquals(null, tracer.activeSpan());
            Assert.assertEquals(0, tracer.finishedSpans().size());
        };
        // Constant to use to exercise code paths; no callbacks on this are actually fired.
        Operation op = Operation.createPost(this.host, "/");
        // we need a filter after that that nests a span and suspends
        // then completes that span and resumes
        // case A, B, C, D
        // Verify test assumptions;
        assertNotEmitted.run();
        OperationProcessingChain chain = OperationProcessingChain.create(makeSpan, checkAndSuspend, checkAndContinue);
        OperationProcessingContext context = chain.createContext(this.host);
        chain.processRequest(op, context, completion);
        // Case B
        // At this point, the span must not have been emitted, and the chain is suspended so no span should be active.
        assertNotEmitted.run();
        chain.resumeProcessingRequest(op,context);
        // At this point the span must have been emitted and no span should be active.
        assertEmitted.run();

        // Need a new chain for each of the other cases.
        // Case E
        tracer.reset();
        chain = OperationProcessingChain.create(makeSpan, checkAndStop);
        context = chain.createContext(this.host);
        chain.processRequest(op, context, completion);
        assertEmitted.run();
        // Case F
        tracer.reset();
        chain = OperationProcessingChain.create(makeSpan, checkAndFail);
        context = chain.createContext(this.host);
        chain.processRequest(op, context, completion);
        assertEmitted.run();
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
                        this.host.failIteration(new IllegalStateException("expected failure"));
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
