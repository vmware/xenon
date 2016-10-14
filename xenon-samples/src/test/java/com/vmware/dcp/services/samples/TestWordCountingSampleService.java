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

package com.vmware.dcp.services.samples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.services.samples.MockDocumentsService.Document;
import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.samples.LocalWordCountingSampleService;
import com.vmware.xenon.services.samples.WordCountingSampleService;
import com.vmware.xenon.services.samples.WordCountingSampleService.WordCountsResponse;

public class TestWordCountingSampleService {
    private static final int NODE_COUNT = 3;

    private TestNodeGroupManager group;

    @Before
    public void setUp() throws Throwable {
        this.group = new TestNodeGroupManager();
        for (int i = 0; i < NODE_COUNT; ++i) {
            ServiceHost host = createHost();
            this.group.addHost(host);
        }
        this.group.joinNodeGroupAndWaitForConvergence();
    }

    @After
    public void tearDown() {
        this.group.getAllHosts().stream().forEach(ServiceHost::stop);
    }

    public static VerificationHost createHost() throws Throwable {
        VerificationHost host = VerificationHost.create(0);
        host.start();
        host.startServiceAndWait(MockDocumentsService.createFactory(),
                MockDocumentsService.FACTORY_LINK, null);
        Service service = new LocalWordCountingSampleService(MockDocumentsService.FACTORY_LINK,
                MockDocumentsService::contentsMapper);
        host.startServiceAndWait(service, LocalWordCountingSampleService.SELF_LINK, null);
        host.startServiceAndWait(WordCountingSampleService.class,
                WordCountingSampleService.SELF_LINK);
        return host;
    }

    @Test
    public void testMultiNodes() throws Throwable {
        final int count = 100;
        final int failures = 2;
        Document document = new Document();
        document.contents = "Hello world!";
        Document errorDoc = new Document();
        errorDoc.contents = "error";
        errorDoc.documentSelfLink = MockDocumentsService.ERROR_ID;

        List<ServiceHost> peers = new ArrayList<>(this.group.getAllHosts());
        TestContext ctx = TestContext.create(1, TimeUnit.MINUTES.toMicros(1));
        List<DeferredResult<Operation>> deferredPosts = IntStream.range(0, count)
                .mapToObj(i -> {
                    ServiceHost host = peers.get(i % NODE_COUNT);
                    Operation post = Operation.createPost(host, MockDocumentsService.FACTORY_LINK).setBody(document);
                    return host.sendWithDeferredResult(post);
                })
                .collect(Collectors.toList());
        deferredPosts.addAll(IntStream.range(count, count + failures)
                .mapToObj(i -> {
                    ServiceHost host = peers.get(i % NODE_COUNT);
                    Operation post = Operation.createPost(host, MockDocumentsService.FACTORY_LINK).setBody(errorDoc);
                    return host.sendWithDeferredResult(post);
                })
                .collect(Collectors.toList()));
        DeferredResult.allOf(deferredPosts).whenComplete(ctx.getCompletionDeferred());
        ctx.await();

        Map<String, Integer> expected = new HashMap<>();
        expected.put("Hello", count);
        expected.put("world", count);

        queryWordCountsAndVerify(this.group.getHost(), expected, failures);
    }

    @Test
    public void testMultiNodesWithFailure() throws Throwable {
        ServiceHost host = this.group.getHost();
        Operation delService = Operation.createDelete(host, LocalWordCountingSampleService.SELF_LINK);
        new TestRequestSender(host).sendAndWait(delService);

        Document document = new Document();
        document.contents = "Hello world!";
        final int countPerNode = 20;
        final int count = countPerNode * NODE_COUNT;

        List<ServiceHost> peers = new ArrayList<>(this.group.getAllHosts());
        TestContext ctx = TestContext.create(1, TimeUnit.MINUTES.toMicros(1));
        List<DeferredResult<Operation>> deferredPosts = IntStream.range(0, count)
                .mapToObj(i -> {
                    ServiceHost h = peers.get(i % NODE_COUNT);
                    Operation post = Operation.createPost(h, MockDocumentsService.FACTORY_LINK).setBody(document);
                    return h.sendWithDeferredResult(post);
                })
                .collect(Collectors.toList());
        DeferredResult.allOf(deferredPosts).whenComplete(ctx.getCompletionDeferred());
        ctx.await();

        Map<String, Integer> expected = new HashMap<>();
        expected.put("Hello", count - countPerNode);
        expected.put("world", count - countPerNode);

        queryWordCountsAndVerify(this.group.getHost(), expected, 0);
    }

    private static void queryWordCountsAndVerify(ServiceHost host, Map<String, Integer> expected, Integer expectedFailures) {
        TestRequestSender sender = new TestRequestSender(host);
        Operation getWordCounts = Operation.createGet(host, WordCountingSampleService.SELF_LINK);
        Operation result = sender.sendAndWait(getWordCounts);
        WordCountsResponse response = result.getBody(WordCountsResponse.class);
        Assert.assertEquals(expected, response.wordCounts);
        Assert.assertEquals(expectedFailures, response.failedDocsCount);
    }
}
