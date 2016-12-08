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

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestConcurrency extends BasicReusableHostTestCase {

    public int clientCount = 3;
    public int iterationCount = 10;

    public static class CounterState extends ServiceDocument {
        public int count;
    }

    public static class CounterService extends StatefulService {

        public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/concurrency-test";

        public CounterService() {
            super(CounterState.class);
            //super.toggleOption(ServiceOption.PERSISTENCE, true);
            super.toggleOption(ServiceOption.REPLICATION, true);
            super.toggleOption(ServiceOption.OWNER_SELECTION, true);
        }

        @Override
        public void handlePatch(Operation patch) {
            // we ignore the body, just increment current state
            CounterState currentState = getState(patch);
            currentState.count++;
            setState(patch, currentState);
            patch.complete();
        }

    }

    @Before
    public void setup() {
        if (this.host.getServiceStage(CounterService.FACTORY_LINK) == null) {
            this.host.startFactory(new CounterService());
            this.host.waitForServiceAvailable(CounterService.FACTORY_LINK);
        }
    }

    @Test
    public void concurrentIncrement() throws Throwable {
        // create counter service
        TestContext ctx = testCreate(1);
        CounterState initialState = new CounterState();
        String counterServiceId = UUID.randomUUID().toString();
        initialState.documentSelfLink = counterServiceId;
        Operation post = Operation.createPost(this.host, CounterService.FACTORY_LINK)
                .setBody(initialState).setCompletion(ctx.getCompletion());
        this.host.send(post);
        testWait(ctx);
        String serviceCounterPath = UriUtils.buildUriPath(CounterService.FACTORY_LINK,
                counterServiceId);

        // increment counter, concurrently
        int operationCount = this.clientCount * this.iterationCount;
        TestContext ctx1 = testCreate(operationCount);
        for (int i = 0; i < this.iterationCount; i++) {
            Collection<Operation> ops = new ArrayList<Operation>(this.clientCount);
            for (int j = 0; j < this.clientCount; j++) {
                ops.add(Operation.createPatch(this.host, serviceCounterPath)
                        .setBody(new CounterState()).setCompletion(ctx1.getCompletion()));
            }

            for (Operation op : ops) {
                this.host.send(op);
            }
        }
        testWait(ctx1);

        // get counter service value
        TestContext ctx2 = testCreate(1);
        CounterState[] counterStates = new CounterState[1];
        Operation get = Operation.createGet(this.host, serviceCounterPath)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx2.failIteration(e);
                        return;
                    }

                    counterStates[0] = o.getBody(CounterState.class);
                    ctx2.completeIteration();
                });
        this.host.send(get);
        testWait(ctx2);
        assertEquals(operationCount, counterStates[0].count);
    }

}
