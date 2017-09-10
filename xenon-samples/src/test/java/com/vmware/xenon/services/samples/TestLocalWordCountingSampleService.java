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

package com.vmware.xenon.services.samples;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.services.samples.MockDocumentsService.Document;
import com.vmware.xenon.services.samples.WordCountingSampleService.WordCountsResponse;

public class TestLocalWordCountingSampleService extends BasicReusableHostTestCase {
    @Before
    public void setUp() throws Throwable {
        // Start a factory for the documents
        this.host.startServiceAndWait(MockDocumentsService.createFactory(),
                MockDocumentsService.FACTORY_LINK, null);
        Service service = new LocalWordCountingSampleService(
                MockDocumentsService.FACTORY_LINK, MockDocumentsService::contentsMapper);
        this.host.startServiceAndWait(service,
                LocalWordCountingSampleService.SELF_LINK, null);
    }

    @After
    public void tearDown() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, MockDocumentsService.FACTORY_LINK));
        TestRequestSender sender = this.host.getTestRequestSender();
        Operation delDocFactory = Operation.createDelete(this.host, MockDocumentsService.FACTORY_LINK);
        Operation delService = Operation.createDelete(this.host, LocalWordCountingSampleService.SELF_LINK);
        sender.sendAndWait(Arrays.asList(delDocFactory, delService));
    }

    @Test
    public void testSingleDocument() throws Throwable {
        Document document = new Document();
        document.contents = "Xenon: Hello World!\nWorld: Hello Xenon!";
        Map<String, Integer> expected = new HashMap<>();
        expected.put("Hello", 2);
        expected.put("World", 2);
        expected.put("Xenon", 2);
        Operation post = Operation.createPost(this.host, MockDocumentsService.FACTORY_LINK).setBody(document);
        host.getTestRequestSender().sendAndWait(post);
        queryWordCountsAndVerify(expected);
    }

    @Test
    public void testMultipleDocuments() throws Throwable {
        int count = 100;

        Document[] docs = new Document[2];
        docs[0] = new Document();
        docs[0].contents = "Hello World!";
        docs[1] = new Document();
        docs[1].contents = "Hello Xenon!";

        URI documentsServiceURI = UriUtils.buildUri(this.host, MockDocumentsService.FACTORY_LINK);
        List<Operation> posts = IntStream.range(0, count)
                .mapToObj(i -> {
                    return Operation.createPost(documentsServiceURI).setBody(docs[i % 2]);
                })
                .collect(Collectors.toList());
        this.host.getTestRequestSender().sendAndWait(posts);

        Map<String, Integer> expected = new HashMap<>();
        expected.put("Hello", count);
        expected.put("World", count / 2);
        expected.put("Xenon", count / 2);

        queryWordCountsAndVerify(expected);
    }

    @Test
    public void testRecoverFromError() throws Throwable {
        Document document = new Document();
        document.contents = "Xenon: Hello World!\nWorld: Hello Xenon!";
        Operation post = Operation.createPost(this.host, MockDocumentsService.FACTORY_LINK).setBody(document);
        this.host.getTestRequestSender().sendAndWait(post);

        Document errorDoc = new Document();
        errorDoc.documentSelfLink = MockDocumentsService.ERROR_ID;
        errorDoc.contents = "whatever";
        post = Operation.createPost(this.host, MockDocumentsService.FACTORY_LINK).setBody(errorDoc);
        this.host.getTestRequestSender().sendAndWait(post);

        Map<String, Integer> expected = new HashMap<>();
        expected.put("Hello", 2);
        expected.put("World", 2);
        expected.put("Xenon", 2);
        this.queryWordCountsAndVerify(expected, 1);
    }

    @Test
    public void testNoDocuments() throws Throwable {
        queryWordCountsAndVerify(Collections.emptyMap());
    }

    @Test
    public void testNullContentsDocument() throws Throwable {
        Document document = new Document();
        Operation post = Operation.createPost(this.host, MockDocumentsService.FACTORY_LINK).setBody(document);
        host.getTestRequestSender().sendAndWait(post);
        queryWordCountsAndVerify(Collections.emptyMap());
    }

    private void queryWordCountsAndVerify(Map<String, Integer> expected) {
        this.queryWordCountsAndVerify(expected, 0);
    }

    private void queryWordCountsAndVerify(Map<String, Integer> expected, Integer expectedFailures) {
        Operation getWordCounts = Operation.createGet(this.host, LocalWordCountingSampleService.SELF_LINK);
        Operation result = this.host.getTestRequestSender().sendAndWait(getWordCounts);
        WordCountsResponse response = result.getBody(WordCountsResponse.class);
        Assert.assertEquals(expected, response.wordCounts);
        Assert.assertEquals(expectedFailures, response.failedDocsCount);
    }
}
