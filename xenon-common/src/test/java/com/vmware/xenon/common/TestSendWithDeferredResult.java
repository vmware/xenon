/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.ServiceHost.ServiceNotFoundException;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestSendWithDeferredResult extends BasicReusableHostTestCase {
    private static final int DOCUMENT_COUNT = 10;

    @Before
    public void setUpHost() throws Throwable {
        TestContext ctx = this.host.testCreate(1);
        Operation get = Operation.createGet(this.host, ExampleService.FACTORY_LINK);
        DeferredResult<Long> deferredResult = this.host
                .sendWithDeferredResult(get, ServiceDocumentQueryResult.class)
                .thenApply(queryResult -> queryResult.documentCount);
        deferredResult.whenComplete(ctx.getCompletionDeferred());
        ctx.await();
        if (deferredResult.getNow(() -> null) == DOCUMENT_COUNT) {
            // Already setup
            return;
        }

        ctx = this.host.testCreate(DOCUMENT_COUNT);
        for (int i = 0; i < DOCUMENT_COUNT; ++i) {
            ExampleServiceState doc = new ExampleServiceState();
            doc.name = generateName(i);
            doc.id = String.valueOf(i);
            doc.sortedCounter = Long.valueOf(i);
            Operation createDocument = Operation
                    .createPost(host, ExampleService.FACTORY_LINK)
                    .setBody(doc);
            this.host.sendWithDeferredResult(createDocument)
                .whenComplete(ctx.getCompletionDeferred());
        }
        ctx.await();
    }

    private String generateName(int i) {
        return "foo_" + i;
    }

    @Test
    public void testSendWithDeferredResult() throws Throwable {
        Operation get = Operation
                .createGet(host, ExampleService.FACTORY_LINK);
        AtomicInteger invocationCounter = new AtomicInteger();
        this.host.testStart(1);

        DeferredResult<Operation> result = this.host.sendWithDeferredResult(get);
        result
            .thenAccept(op -> {
                Assert.assertEquals(Operation.STATUS_CODE_OK, op.getStatusCode());
                ServiceDocumentQueryResult queryResult = op.getBody(ServiceDocumentQueryResult.class);
                Assert.assertEquals(DOCUMENT_COUNT, queryResult.documentCount.longValue());
                invocationCounter.incrementAndGet();
            })
            .whenComplete(this.host.getCompletionDeferred());
        this.host.testWait();
        Assert.assertEquals(1, invocationCounter.get());
    }

    @Test
    public void testSendWithDeferredResultTyped() throws Throwable {
        Operation get = Operation
                .createGet(host, ExampleService.FACTORY_LINK);
        AtomicInteger invocationCounter = new AtomicInteger();
        this.host.testStart(1);

        DeferredResult<ServiceDocumentQueryResult> result =
                this.host.sendWithDeferredResult(get, ServiceDocumentQueryResult.class);
        result
            .thenAccept(queryResult -> {
                Assert.assertEquals(DOCUMENT_COUNT, queryResult.documentCount.longValue());
                invocationCounter.incrementAndGet();
            })
            .whenComplete(this.host.getCompletionDeferred());
        this.host.testWait();
        Assert.assertEquals(1, invocationCounter.get());
    }

    @Test
    public void testFanOut() throws Throwable {
        Operation get = Operation
                .createGet(host, ExampleService.FACTORY_LINK);
        AtomicInteger invocationCounter = new AtomicInteger();
        this.host.testStart(1);

        this.host
            .sendWithDeferredResult(get, ServiceDocumentQueryResult.class)
            .thenCompose(queryResult -> {
                invocationCounter.incrementAndGet();
                Assert.assertEquals(DOCUMENT_COUNT, queryResult.documentCount.longValue());
                Assert.assertEquals(DOCUMENT_COUNT, queryResult.documentLinks.size());
                List<DeferredResult<ExampleServiceState>> deferredResults =
                        queryResult.documentLinks
                        .stream()
                        .map(link -> Operation.createGet(host, link))
                        .map(getLink -> this.host.sendWithDeferredResult(getLink,
                                ExampleServiceState.class))
                        .collect(Collectors.toList());
                return DeferredResult.allOf(deferredResults);
            })
            .thenAccept(results -> {
                invocationCounter.incrementAndGet();
                // Note: although the ordering of the results is guaranteed,
                // the ordering of the links above is not, hence the use of Set
                Set<String> expectedNames =
                        IntStream.range(0, DOCUMENT_COUNT)
                        .mapToObj(this::generateName)
                        .collect(Collectors.toSet());
                Set<String> names =
                        results.stream()
                        .map(doc -> doc.name)
                        .collect(Collectors.toSet());
                Assert.assertEquals(DOCUMENT_COUNT, names.size());
                Assert.assertEquals(expectedNames, names);
            })
            .whenComplete(this.host.getCompletionDeferred());

        this.host.testWait();
        Assert.assertEquals(2, invocationCounter.get());
    }

    @Test(expected = ServiceNotFoundException.class)
    public void testException() throws Throwable {
        this.host.testStart(1);
        Operation get = Operation
                .createGet(host, UriUtils.buildUriPath(ExampleService.FACTORY_LINK, "unknown"));
        this.host
            .sendWithDeferredResult(get)
            .thenRun(() -> {
                Assert.fail();
            })
            .whenComplete(this.host.getCompletionDeferred());
        this.host.testWait();
    }

    @Test
    public void testRecover() throws Throwable {
        TestContext ctx = this.host.testCreate(1);

        Operation get = Operation
                .createGet(host, UriUtils.buildUriPath(ExampleService.FACTORY_LINK, "unknown"));
        DeferredResult<ExampleServiceState> deferredResult =
                this.host.sendWithDeferredResult(get, ExampleServiceState.class)
                        .exceptionally(ex -> {
                            ExampleServiceState doc = new ExampleServiceState();
                            doc.name = "?";
                            return doc;
                        });

        ctx.await(deferredResult);

        ExampleServiceState doc = deferredResult.getNow(() -> null);
        Assert.assertEquals("?", doc.name);
    }

    @Test(expected = NumberFormatException.class)
    public void testRethrow() throws Throwable {
        AtomicInteger invocationCounter = new AtomicInteger();
        this.host.testStart(1);
        Operation get = Operation
                .createGet(host, UriUtils.buildUriPath(ExampleService.FACTORY_LINK, "unknown"));
        this.host
            .sendWithDeferredResult(get, ExampleServiceState.class)
            .thenRun(() -> {
                Assert.fail();
            })
            .exceptionally(ex -> {
                // Make sure we are capturing the correct exception
                if (ex.getCause() instanceof AssertionError) {
                    throw new CompletionException(ex);
                }
                invocationCounter.incrementAndGet();
                throw new CompletionException(new NumberFormatException());
            })
            .whenComplete(this.host.getCompletionDeferred());
        this.host.testWait();
        Assert.assertEquals(1, invocationCounter.get());
    }
}
