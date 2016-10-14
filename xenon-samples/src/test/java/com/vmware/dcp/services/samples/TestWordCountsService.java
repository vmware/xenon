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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.samples.DocumentsService;
import com.vmware.xenon.services.samples.DocumentsService.Document;
import com.vmware.xenon.services.samples.LocalWordCountsService;
import com.vmware.xenon.services.samples.WordCountsService;
import com.vmware.xenon.services.samples.WordCountsService.WordCountsResponse;

public class TestWordCountsService extends BasicTestCase {
    private static final int NODE_COUNT = 3;

    @Before
    public void setUp() throws Exception {
        try {
            this.host.setUpPeerHosts(NODE_COUNT);
            this.host.joinNodesAndVerifyConvergence(NODE_COUNT);
            this.host.waitForNodeGroupConvergence(NODE_COUNT);

            for (VerificationHost h : this.host.getInProcessHostMap().values()) {
                h.startServiceAndWait(DocumentsService.createFactory(),
                        DocumentsService.FACTORY_LINK, null);
                h.startServiceAndWait(LocalWordCountsService.class,
                        LocalWordCountsService.SELF_LINK);
                h.startServiceAndWait(WordCountsService.class,
                        WordCountsService.SELF_LINK);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMultiNodes() throws Throwable {
        final int count = 100;
        final int failures = 2;
        Document document = new Document();
        document.contents = "Hello world!";
        Document errorDoc = new Document();
        errorDoc.contents = "Hello world!";
        errorDoc.documentSelfLink = DocumentsService.ERROR_ID;

        List<VerificationHost> peers = new ArrayList<>(this.host.getInProcessHostMap().values());


        TestContext ctx = TestContext.create(1, TimeUnit.MINUTES.toMicros(1));
        List<DeferredResult<Operation>> deferredPosts = IntStream.range(0, count)
                .mapToObj(i -> {
                    VerificationHost host = peers.get(i % NODE_COUNT);
                    Operation post = Operation.createPost(host, DocumentsService.FACTORY_LINK).setBody(document);
                    return host.sendWithDeferredResult(post);
                })
                .collect(Collectors.toList());
        deferredPosts.addAll(IntStream.range(count, count + failures)
                .mapToObj(i -> {
                    VerificationHost host = peers.get(i % NODE_COUNT);
                    Operation post = Operation.createPost(host, DocumentsService.FACTORY_LINK).setBody(errorDoc);
                    return host.sendWithDeferredResult(post);
                })
                .collect(Collectors.toList()));
        DeferredResult.allOf(deferredPosts).whenComplete(ctx.getCompletionDeferred());

        Map<String, Integer> expected = new HashMap<>();
        expected.put("Hello", count);
        expected.put("world", count);

        queryWordCountsAndVerify(this.host.getPeerHost(), expected, failures);
    }

    private static void queryWordCountsAndVerify(VerificationHost host, Map<String, Integer> expected, Integer expectedFailures) {
        Operation getWordCounts = Operation.createGet(host, WordCountsService.SELF_LINK);
        Operation result = host.getTestRequestSender().sendAndWait(getWordCounts);
        WordCountsResponse response = result.getBody(WordCountsResponse.class);
        Assert.assertEquals(expected, response.wordCounts);
        Assert.assertEquals(expectedFailures, response.failedDocsCount);
    }
}
