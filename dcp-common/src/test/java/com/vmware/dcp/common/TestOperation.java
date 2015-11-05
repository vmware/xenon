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

import static org.junit.Assert.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.test.MinimalTestServiceState;
import com.vmware.dcp.services.common.MinimalTestService;

public class TestOperation extends BasicReusableHostTestCase {

    @Test
    public void setterValidation() {
        Operation op = Operation.createGet(this.host.getUri());

        Runnable r = () -> {
            op.setRetryCount(Short.MAX_VALUE * 2);
        };
        verifyArgumentException(r);

        r = () -> {
            op.setRetryCount(-10);
        };
        verifyArgumentException(r);

        r = () -> {
            op.addHeader("sadfauisydf", false);
        };
        verifyArgumentException(r);

        r = () -> {
            op.addHeader("", false);
        };
        verifyArgumentException(r);

        r = () -> {
            op.addHeader(null, false);
        };
        verifyArgumentException(r);
    }

    private void verifyArgumentException(Runnable r) {
        try {
            r.run();
            throw new IllegalStateException("Should have failed");
        } catch (IllegalArgumentException e) {
            return;
        }
    }

    @Test
    public void operationDoubleCompletion() throws Throwable {
        AtomicInteger completionCount = new AtomicInteger();
        CompletionHandler c = (o, e) -> {
            completionCount.incrementAndGet();
        };

        int count = 100;
        this.host.toggleNegativeTestMode(true);
        this.host.testStart(count);
        Operation op = Operation.createGet(this.host.getUri()).setCompletion(c);
        for (int i = 0; i < count; i++) {
            this.host.run(() -> {
                op.complete();
                op.fail(new Exception());
                try {
                    Thread.sleep(1);
                } catch (Exception e1) {
                }
                this.host.completeIteration();
            });
        }
        this.host.testWait();
        this.host.toggleNegativeTestMode(false);
        assertTrue(completionCount.get() == 1);

    }

    @Test
    public void operationWithContextId() throws Throwable {
        List<Service> services = this.host.doThroughputServiceStart(1,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);

        // issue a patch to verify contextId received in the services matches the one set
        // by the client on the Operation
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_HAS_CONTEXT_ID;
        body.stringValue = "request-id";

        this.host.testStart(1);
        this.host.send(Operation
                .createPatch(services.get(0).getUri())
                .forceRemote()
                .setBody(body)
                .setContextId(body.stringValue)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                }));
        this.host.testWait();
    }

    @Test
    public void operationWithoutContextId() throws Throwable {
        List<Service> services = this.host.doThroughputServiceStart(1,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);

        // issue a patch request to verify the contextId is 'null'
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_HAS_CONTEXT_ID;

        this.host.testStart(1);
        this.host.send(Operation
                .createPatch(services.get(0).getUri())
                .forceRemote()
                .setBody(body)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.completeIteration();
                        return;
                    }

                    this.host.failIteration(new IllegalStateException(
                            "Request should have failed due to missing contextId"));
                }));
        this.host.testWait();
    }

}
