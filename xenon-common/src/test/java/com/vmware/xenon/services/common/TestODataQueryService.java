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

package com.vmware.xenon.services.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestODataQueryService extends BasicReusableHostTestCase {
    public long min = 10;
    public long max = 30;
    public List<String> selfLinks;
    private boolean isFailureExpected;

    @After
    public void tearDown() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildFactoryUri(this.host, ExampleService.class));
    }

    private List<String> postExample(long min, long max) throws Throwable {
        List<String> selfLinks = new ArrayList<>();

        this.host.testStart(max - min + 1);
        for (long i = min; i <= max; i++) {
            ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
            inState.name = String.format("name-%d", i);
            inState.counter = i;
            inState.keyValues = new HashMap<String, String>();
            inState.keyValues.put(String.format("key-%d-A", i), String.format("value-%d-A", i));
            inState.keyValues.put(String.format("key-%d-B", i), String.format("value-%d-B", i));

            this.host.send(Operation.createPost(UriUtils.extendUri(this.host.getUri(),
                    ExampleService.FACTORY_LINK)).setBody(inState)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        ServiceDocument d = o.getBody(ServiceDocument.class);
                        synchronized (selfLinks) {
                            selfLinks.add(d.documentSelfLink);
                        }
                        this.host.completeIteration();
                    }));
        }
        this.host.testWait();
        return selfLinks;
    }

    private void postExample(ExampleService.ExampleServiceState inState) throws Throwable {
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(UriUtils.buildFactoryUri(this.host, ExampleService.class))
                .setBody(inState)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                            }
                            ExampleService.ExampleServiceState s = o
                                    .getBody(ExampleService.ExampleServiceState.class);
                            inState.documentSelfLink = s.documentSelfLink;
                            this.host.completeIteration();
                        }));
        this.host.testWait();
    }

    @Test
    public void count() throws Throwable {
        ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
        int c = 5;
        List<String> expectedOrder = new ArrayList<>();
        for (int i = 0; i < c; i++) {
            inState.documentSelfLink = null;
            inState.counter = 1L;
            inState.name = i + "-abcd";
            postExample(inState);
            expectedOrder.add(inState.name);
        }

        String queryString = "$filter=counter eq 1";
        queryString += "&" + "$count=true";
        ServiceDocumentQueryResult res = doQuery(queryString, true);
        assertTrue(res.documentCount == c);
        assertTrue(res.documentLinks.size() == 0);
        assertTrue(res.documents == null);

        queryString = "$filter=counter eq 1";
        queryString += "&" + "$count=false";
        res = doQuery(queryString, true);
        assertTrue(res.documentCount == c);
        assertTrue(res.documentLinks.size() == c);
        assertTrue(res.documents.size() == c);
    }

    @Test
    public void orderBy() throws Throwable {
        ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
        int c = 5;
        Random r = new Random();
        List<String> expectedOrder = new ArrayList<>();
        for (int i = 0; i < c; i++) {
            inState.documentSelfLink = null;
            inState.counter = 1L;
            inState.sortedCounter = new Long(Math.abs(r.nextLong()));
            inState.name = i + "-abcd";
            postExample(inState);
            expectedOrder.add(inState.name);
        }

        for (int i = 0; i < c; i++) {
            inState.documentSelfLink = null;
            inState.counter = 1000L + i;
            inState.sortedCounter = new Long(Math.abs(r.nextLong()));
            inState.name = i + "-abcd";
            postExample(inState);
        }

        // ascending search first
        String queryString = "$filter=counter eq 1";
        queryString += "&" + "$orderby=name asc";
        doOrderByQueryAndValidateResult(c, expectedOrder, queryString);

        // descending search
        queryString = "$filter=counter eq 1";
        queryString += "&" + "$orderby=name desc";
        queryString += "&" + "$orderbytype=STRING";
        Collections.reverse(expectedOrder);
        doOrderByQueryAndValidateResult(c, expectedOrder, queryString);

        // ascending search (sortedCounter)
        queryString = "$filter=counter eq 1";
        queryString += "&" + "$orderby=sortedCounter asc";
        queryString += "&" + "$orderbytype=LONG";
        doOrderByQueryAndValidateNumericResult(c, queryString,
                ExampleServiceState.FIELD_NAME_SORTED_COUNTER, "asc");

        // descending search (sortedCounter)
        queryString = "$filter=counter eq 1";
        queryString += "&" + "$orderby=sortedCounter desc";
        queryString += "&" + "$orderbytype=LONG";
        doOrderByQueryAndValidateNumericResult(c, queryString,
                ExampleServiceState.FIELD_NAME_SORTED_COUNTER, "desc");

        // ascending search (counter)
        queryString += "$orderby=counter asc";
        queryString += "&" + "$orderbytype=LONG";
        doOrderByQueryAndValidateNumericResult(c, queryString,
                ExampleServiceState.FIELD_NAME_COUNTER, "asc");

        // descending search (sortedCounter)
        queryString += "$orderby=counter desc";
        queryString += "&" + "$orderbytype=LONG";
        doOrderByQueryAndValidateNumericResult(c, queryString,
                ExampleServiceState.FIELD_NAME_COUNTER, "desc");

        // pass a bogus order specifier, expect failure.
        this.isFailureExpected = true;
        try {
            queryString = "$filter=counter eq 1";
            queryString += "&" + "$orderby=name something";
            doOrderByQueryAndValidateResult(c, expectedOrder, queryString);
        } finally {
            this.isFailureExpected = false;
        }

        // pass a bogus orderbytype specifier, expect failure.
        this.isFailureExpected = true;
        try {
            queryString = "$filter=counter eq 1";
            queryString += "&" + "$orderby=name asc";
            queryString += "&" + "$orderbytype=badtypename";
            doOrderByQueryAndValidateResult(c, expectedOrder, queryString);
        } finally {
            this.isFailureExpected = false;
        }
    }

    @Test
    public void top() throws Throwable {
        ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
        int c = 5;
        List<String> expectedOrder = new ArrayList<>();
        for (int i = 0; i < c; i++) {
            inState.documentSelfLink = null;
            inState.counter = 1L;
            inState.name = i + "-abcd";
            postExample(inState);
            expectedOrder.add(inState.name);
        }

        // top + filter
        int topCount = c - 2;
        String queryString = "$filter=counter eq 1";
        queryString += "&" + "$top=" + +topCount;
        ServiceDocumentQueryResult res = doQuery(queryString, true);
        assertTrue(res.documentCount == topCount);
        assertTrue(res.documentLinks.size() == topCount);
        assertTrue(res.documents.size() == topCount);

        // do the same, but through a factory
        URI u = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        u = UriUtils.extendUriWithQuery(u, "$filter", "counter eq 1", "$top", "" + topCount);
        res = this.host.getFactoryState(u);
        assertTrue(res.documentCount == topCount);
        assertTrue(res.documentLinks.size() == topCount);
        assertTrue(res.documents.size() == topCount);

        // top + filter + orderBy
        queryString = "$filter=counter eq 1";
        queryString += "&" + "$orderby=name asc";
        queryString += "&" + "$top=" + topCount;

        doOrderByQueryAndValidateResult(topCount, expectedOrder, queryString);

        // pass a bogus order specifier, expect failure.
        this.isFailureExpected = true;
        try {
            queryString = "$filter=counter eq 1";
            queryString = queryString + "&" + "$top=bogus";
            doOrderByQueryAndValidateResult(c, expectedOrder, queryString);
        } finally {
            this.isFailureExpected = false;
        }
    }

    private void doOrderByQueryAndValidateResult(int c, List<String> expectedOrder,
            String queryString) throws Throwable {
        ServiceDocumentQueryResult res = doQuery(queryString, true);
        if (this.isFailureExpected) {
            return;
        }
        assertEquals(c, res.documentLinks.size());
        assertNotNull(res.documents);

        int i = 0;
        for (String link : res.documentLinks) {
            Object document = res.documents.get(link);
            ExampleServiceState st = Utils.fromJson(document, ExampleServiceState.class);
            String expected = expectedOrder.get(i++);
            if (!expected.equals(st.name)) {
                throw new IllegalStateException("sort order not expected: " + Utils.toJsonHtml(res));
            }
        }
    }

    private void doOrderByQueryAndValidateNumericResult(
            int c, String queryString, String propertyName, String order) throws Throwable {
        ServiceDocumentQueryResult res = doQuery(queryString, true);
        if (this.isFailureExpected) {
            return;
        }
        assertEquals(c, res.documentLinks.size());
        assertNotNull(res.documents);

        long previous = order.equals("asc") ? Long.MIN_VALUE : Long.MAX_VALUE;

        for (String link : res.documentLinks) {
            Object document = res.documents.get(link);
            ExampleServiceState st = Utils.fromJson(document, ExampleServiceState.class);
            long current = Long.MIN_VALUE;
            if (propertyName.equals(ExampleServiceState.FIELD_NAME_COUNTER)) {
                current = st.counter;
            } else if (propertyName.equals(ExampleServiceState.FIELD_NAME_SORTED_COUNTER)) {
                current = st.sortedCounter;
            } else {
                throw new IllegalStateException("Unexpected propertyName passed");
            }
            if ((order.equals("asc") && previous > current) ||
                    (order.equals("desc") && previous < current)) {
                throw new IllegalStateException("Data was not sorted as expected: " + Utils.toJsonHtml(res));
            }
            previous = current;
        }
    }

    @Test
    public void filterQueries() throws Throwable {
        this.selfLinks = postExample(this.min, this.max);
        testSimpleStringQuery();
        testGTQuery();
        testGEQuery();
        testLTQuery();
        testLEQuery();
        testNumericEqQuery();
        testOdataQueryWithUriEncoding();
        testOdataQueryNested();
    }

    private void testSimpleStringQuery() throws Throwable {
        ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
        inState.name = "TEST STRING";
        postExample(inState);

        String queryString = "$filter=name eq 'TEST STRING'";

        Map<String, Object> out = doQuery(queryString, false).documents;
        assertNotNull(out);

        ExampleService.ExampleServiceState outState = Utils.fromJson(
                out.get(inState.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState.name.equals(inState.name));

        out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        outState = Utils.fromJson(
                out.get(inState.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState.name.equals(inState.name));
    }

    private void testGTQuery() throws Throwable {
        // we should get 10 documents back
        String queryString = String.format("$filter=counter gt %d", this.min + 10);

        Map<String, Object> out = doQuery(queryString, false).documents;
        assertNotNull(out);

        assertEquals(10, out.size());

        out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        assertEquals(10, out.size());
    }

    private void testGEQuery() throws Throwable {
        // we should get 10 documents back
        String queryString = String.format("$filter=counter ge %d", this.min + 10);

        Map<String, Object> out = doQuery(queryString, false).documents;
        assertNotNull(out);

        assertEquals(11, out.size());

        out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        assertEquals(11, out.size());
    }

    private void testLTQuery() throws Throwable {
        // we should get 10 documents back
        String queryString = String.format("$filter=counter lt %d", this.min + 10);

        Map<String, Object> out = doQuery(queryString, false).documents;
        assertNotNull(out);

        assertEquals(10, out.size());

        out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        assertEquals(10, out.size());
    }

    private void testLEQuery() throws Throwable {
        // we should get 10 documents back
        String queryString = String.format("$filter=counter le %d", this.min + 10);

        Map<String, Object> out = doQuery(queryString, false).documents;
        assertNotNull(out);

        assertEquals(11, out.size());

        out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        assertEquals(11, out.size());
    }

    private void testNumericEqQuery() throws Throwable {
        ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
        inState.counter = (long) 0xFFFF;
        inState.name = "name required";
        postExample(inState);

        String queryString = String.format("$filter=counter eq %d", inState.counter);

        Map<String, Object> out = doQuery(queryString, false).documents;
        assertNotNull(out);

        ExampleService.ExampleServiceState outState = Utils.fromJson(
                out.get(inState.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertEquals(outState.counter, inState.counter);

        out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        outState = Utils.fromJson(
                out.get(inState.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertEquals(outState.counter, inState.counter);
    }

    private void testOdataQueryWithUriEncoding() throws Throwable {
        ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
        inState.name = "TEST STRING";
        postExample(inState);

        /* Perform URL encoding on the query String */

        String queryString = URLEncoder.encode("$filter=name eq 'TEST STRING'",
                Charset.defaultCharset().toString());

        assert (queryString.contains("+"));

        Map<String, Object> out = doQuery(queryString, true).documents;

        assertNotNull(out);

        ExampleService.ExampleServiceState outState = Utils.fromJson(
                out.get(inState.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState.name.equals(inState.name));

        out = doFactoryServiceQuery(queryString, true);
        assertNotNull(out);
        outState = Utils.fromJson(
                out.get(inState.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState.name.equals(inState.name));
    }

    private void testOdataQueryNested() throws Throwable {
        ExampleServiceState inStateA = new ExampleService.ExampleServiceState();
        inStateA.name = "TEST STRING A";
        inStateA.keyValues = new HashMap<String, String>();
        inStateA.keyValues.put("key", "value-A");
        postExample(inStateA);

        ExampleServiceState inStateB = new ExampleService.ExampleServiceState();
        inStateB.name = "TEST STRING B";
        inStateB.keyValues = new HashMap<String, String>();
        inStateB.keyValues.put("key", "value-B");
        postExample(inStateB);

        // Xenon nested property filter
        testOdataQueryNested("/", inStateA, inStateB);

        // OData specification nested property filter
        testOdataQueryNested(".", inStateA, inStateB);
    }

    private void testOdataQueryNested(String separator, ExampleServiceState stateA,
            ExampleServiceState stateB) throws Throwable {
        String queryString = URLEncoder.encode("$filter=keyValues" + separator
                + "key eq 'value-A'",
                Charset.defaultCharset().toString());
        Map<String, Object> out = doQuery(queryString, true).documents;
        assertEquals(1, out.size());
        ExampleServiceState outState = Utils.fromJson(
                out.get(stateA.documentSelfLink), ExampleServiceState.class);
        assertTrue(outState.name.equals(stateA.name));

        queryString = URLEncoder.encode("$filter=keyValues" + separator
                + "key eq 'value*'",
                Charset.defaultCharset().toString());
        out = doQuery(queryString, true).documents;
        assertEquals(2, out.size());

        outState = Utils.fromJson(out.get(stateA.documentSelfLink), ExampleServiceState.class);
        assertTrue(outState.name.equals(stateA.name));

        outState = Utils.fromJson(out.get(stateB.documentSelfLink), ExampleServiceState.class);
        assertTrue(outState.name.equals(stateB.name));

        queryString = URLEncoder.encode("$filter=keyValues" + separator
                + "key-unexisting eq 'value-C'",
                Charset.defaultCharset().toString());
        out = doQuery(queryString, true).documents;
        assertTrue(out.isEmpty());
    }

    @Test
    public void complexFilterQueries() throws Throwable {
        testSimpleOrQuery();
        testSimpleAndQuery();
        testAndWithNEQuery();
        testOrWithNEQuery();
        testAndWithNestedORQuery();
        testOrWithNestedAndQuery();
        testNEAndNEQuery();
        testNEOrNEQuery();
        testANYQuery();
        testALLQuery();
    }

    private void testSimpleOrQuery() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleService.ExampleServiceState document1 = new ExampleService.ExampleServiceState();
        document1.name = "STRING1";
        postExample(document1);

        ExampleService.ExampleServiceState document2 = new ExampleService.ExampleServiceState();
        document2.name = "STRING2";
        postExample(document2);

        String queryString = "$filter=name eq STRING1 or name eq STRING2";

        Map<String, Object> out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        assertEquals(2, out.keySet().size());
        ExampleService.ExampleServiceState outState1 = Utils.fromJson(
                out.get(document1.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState1.name.equals(document1.name));

        ExampleService.ExampleServiceState outState2 = Utils.fromJson(
                out.get(document2.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState2.name.equals(document2.name));
    }

    private void testSimpleAndQuery() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleService.ExampleServiceState document1 = new ExampleService.ExampleServiceState();
        document1.name = "STRING1";
        document1.counter = 10L;
        postExample(document1);

        ExampleService.ExampleServiceState document2 = new ExampleService.ExampleServiceState();
        document2.name = "STRING2";
        document2.counter = 15L;
        postExample(document2);

        String queryString = "$filter=name eq STRING* and counter lt 13";

        Map<String, Object> out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        assertEquals(1, out.keySet().size());
        ExampleService.ExampleServiceState outState1 = Utils.fromJson(
                out.get(document1.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState1.name.equals(document1.name));
    }

    private void testAndWithNEQuery() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleService.ExampleServiceState document1 = new ExampleService.ExampleServiceState();
        document1.name = "MAPPING1";
        document1.keyValues.put("A","a");
        postExample(document1);

        ExampleService.ExampleServiceState document2 = new ExampleService.ExampleServiceState();
        document2.name = "MAPPING2";
        document2.keyValues.put("B", "b");
        postExample(document2);

        ExampleService.ExampleServiceState document3 = new ExampleService.ExampleServiceState();
        document3.name = "MAPPING3";
        document3.keyValues.put("B", "b");
        postExample(document3);


        String queryString1 = "$filter=name ne MAPPING2 and keyValues.A eq a";

        Map<String, Object> out1 = doFactoryServiceQuery(queryString1, false);
        assertNotNull(out1);
        assertEquals(1, out1.keySet().size());
        ExampleService.ExampleServiceState outState1 = Utils.fromJson(
                out1.get(document1.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState1.name.equals(document1.name));

        String queryString2 = "$filter=name ne MAPPING3 and keyValues.B eq b";

        Map<String, Object> out2 = doFactoryServiceQuery(queryString2, false);
        assertNotNull(out2);
        assertEquals(1, out2.keySet().size());
        ExampleService.ExampleServiceState outState2 = Utils.fromJson(
                out2.get(document2.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState2.name.equals(document2.name));
    }

    private void testOrWithNEQuery() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleService.ExampleServiceState document1 = new ExampleService.ExampleServiceState();
        document1.name = "MAPPING4";
        document1.keyValues.put("P","p");
        postExample(document1);

        ExampleService.ExampleServiceState document2 = new ExampleService.ExampleServiceState();
        document2.name = "MAPPING4";
        document2.keyValues.put("P", "P");
        postExample(document2);

        ExampleService.ExampleServiceState document3 = new ExampleService.ExampleServiceState();
        document3.name = "MAPPING5";
        document3.keyValues.put("Q", "q");
        postExample(document3);

        // Always use 'and' in combination with ne in a sub-clause with OR.
        // 'name eq MAPPING4 or keyValues.P ne P'
        // should be written as
        // name eq MAPPING4 or (name eq MAPPING* and keyValues.P ne P)
        // TO achieve A or Not B write A or (C Not B), choose C such that it is always true.
        // The following example returns all documents
        // name eq MAPPING4 fetches doc1, doc2
        // (name eq MAPPING* and keyValues.P ne P) fetches doc1 and doc3
        // The OR clause adds the results and returns doc1, doc2, doc3
        String queryString1 = "$filter=name eq MAPPING4 or (name eq MAPPING* and keyValues.P ne P)";

        Map<String, Object> out1 = doFactoryServiceQuery(queryString1, false);
        assertNotNull(out1);
        assertEquals(3, out1.keySet().size());
        ExampleService.ExampleServiceState outState1 = Utils.fromJson(
                out1.get(document1.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState1.name.equals(document1.name));
        ExampleService.ExampleServiceState outState2 = Utils.fromJson(
                out1.get(document2.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState2.name.equals(document2.name));
        ExampleService.ExampleServiceState outState3 = Utils.fromJson(
                out1.get(document3.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState3.name.equals(document3.name));
    }

    private void testAndWithNestedORQuery() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleService.ExampleServiceState document1 = new ExampleService.ExampleServiceState();
        document1.name = "Java";
        document1.keyValues.put("version", "7");
        document1.keyValues.put("arch", "arm32");
        postExample(document1);

        ExampleService.ExampleServiceState document2 = new ExampleService.ExampleServiceState();
        document2.name = "Java";
        document2.keyValues.put("version", "8");
        document2.keyValues.put("arch", "arm64");
        postExample(document2);

        ExampleService.ExampleServiceState document3 = new ExampleService.ExampleServiceState();
        document3.name = "GO";
        document3.keyValues.put("version", "1.6");
        document3.keyValues.put("arch", "arm64");
        postExample(document3);

        String queryString1 = "$filter=name eq Java and (keyValues.version eq '7' or keyValues.version eq '8')";

        Map<String, Object> out1 = doFactoryServiceQuery(queryString1, false);
        assertNotNull(out1);
        assertEquals(2, out1.keySet().size());
        ExampleService.ExampleServiceState outState1 = Utils.fromJson(
                out1.get(document1.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState1.name.equals(document1.name));
        assertTrue(outState1.keyValues.get("version").equals("7"));

        ExampleService.ExampleServiceState outState2 = Utils.fromJson(
                out1.get(document2.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState2.name.equals(document2.name));
        assertTrue(outState2.keyValues.get("version").equals("8"));

        String queryString2 = "$filter=name eq Java and (keyValues.arch eq 'arm64' or keyValues.version ne '7')";

        Map<String, Object> out2 = doFactoryServiceQuery(queryString2, false);
        assertNotNull(out2);
        assertEquals(1, out2.keySet().size());
        ExampleService.ExampleServiceState outState3 = Utils.fromJson(
                out2.get(document2.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState3.name.equals(document2.name));
        assertTrue(outState3.keyValues.get("version").equals("8"));
    }

    private void testOrWithNestedAndQuery() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleService.ExampleServiceState document1 = new ExampleService.ExampleServiceState();
        document1.name = "gcc";
        document1.keyValues.put("version", "5");
        document1.keyValues.put("arch", "i686");
        postExample(document1);

        ExampleService.ExampleServiceState document2 = new ExampleService.ExampleServiceState();
        document2.name = "gcc";
        document2.keyValues.put("version", "6");
        document2.keyValues.put("arch", "amd64");
        postExample(document2);

        ExampleService.ExampleServiceState document3 = new ExampleService.ExampleServiceState();
        document3.name = "llvm";
        document3.keyValues.put("version", "3.8");
        document3.keyValues.put("arch", "ppc64le");
        postExample(document3);

        String queryString1 = "$filter=name eq llvm or (name eq gcc and keyValues.version eq '6')";

        Map<String, Object> out1 = doFactoryServiceQuery(queryString1, false);
        assertNotNull(out1);
        assertEquals(2, out1.keySet().size());
        ExampleService.ExampleServiceState outState1 = Utils.fromJson(
                out1.get(document2.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState1.name.equals(document2.name));
        assertTrue(outState1.keyValues.get("version").equals("6"));

        ExampleService.ExampleServiceState outState2 = Utils.fromJson(
                out1.get(document3.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState2.name.equals(document3.name));
        assertTrue(outState2.keyValues.get("version").equals("3.8"));

        String queryString2 = "$filter=name eq llvm or (keyValues.version eq '5' and keyValues.arch ne 'amd64')";

        Map<String, Object> out2 = doFactoryServiceQuery(queryString2, false);
        assertNotNull(out2);
        assertEquals(2, out2.keySet().size());
        ExampleService.ExampleServiceState outState3 = Utils.fromJson(
                out2.get(document1.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState3.name.equals(document1.name));
        assertTrue(outState3.keyValues.get("version").equals("5"));
        assertTrue(outState3.keyValues.get("arch").equals("i686"));
        ExampleService.ExampleServiceState outState4 = Utils.fromJson(
                out2.get(document3.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState4.name.equals(document3.name));
        assertTrue(outState4.keyValues.get("version").equals("3.8"));
        assertTrue(outState4.keyValues.get("arch").equals("ppc64le"));
    }

    private void testNEOrNEQuery() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleService.ExampleServiceState document1 = new ExampleService.ExampleServiceState();
        document1.name = "STRING1";
        postExample(document1);

        ExampleService.ExampleServiceState document2 = new ExampleService.ExampleServiceState();
        document2.name = "STRING2";
        postExample(document2);

        String queryString = "$filter=name ne STRING3 or name ne STRING4";

        Map<String, Object> out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        assertEquals(2, out.keySet().size());
        ExampleService.ExampleServiceState outState1 = Utils.fromJson(
                out.get(document1.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState1.name.equals(document1.name));
        ExampleService.ExampleServiceState outState2 = Utils.fromJson(
                out.get(document2.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState2.name.equals(document2.name));
    }

    private void testNEAndNEQuery() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleService.ExampleServiceState document1 = new ExampleService.ExampleServiceState();
        document1.name = "STRING1";
        postExample(document1);

        ExampleService.ExampleServiceState document2 = new ExampleService.ExampleServiceState();
        document2.name = "STRING2";
        postExample(document2);

        String queryString = "$filter=name ne STRING3 and name ne STRING2";

        Map<String, Object> out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        assertEquals(1, out.keySet().size());
        ExampleService.ExampleServiceState outState1 = Utils.fromJson(
                out.get(document1.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState1.name.equals(document1.name));
    }

    private void testANYQuery() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleService.ExampleServiceState document1 = new ExampleService.ExampleServiceState();
        document1.name = "document1";
        document1.tags.add("STRING ONE");
        document1.tags.add("STRING TWO");
        postExample(document1);

        ExampleService.ExampleServiceState document2 = new ExampleService.ExampleServiceState();
        document2.name = "document2";
        document2.tags.add("STRING X");
        document2.tags.add("STRING Y");
        postExample(document2);

        String queryString = "$filter=tags.item any 'STRING ONE;STRING Y'";

        Map<String, Object> out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        assertEquals(2, out.keySet().size());
        ExampleService.ExampleServiceState outState1 = Utils.fromJson(
                out.get(document1.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState1.name.equals(document1.name));
        ExampleService.ExampleServiceState outState2 = Utils.fromJson(
                out.get(document2.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState2.name.equals(document2.name));
    }

    private void testALLQuery() throws Throwable {
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleService.ExampleServiceState document1 = new ExampleService.ExampleServiceState();
        document1.name = "document1";
        document1.tags.add("STRING ONE");
        document1.tags.add("STRING TWO");
        postExample(document1);

        ExampleService.ExampleServiceState document2 = new ExampleService.ExampleServiceState();
        document2.name = "document2";
        document2.tags.add("STRING X");
        document2.tags.add("STRING Y");
        postExample(document2);


        String queryString = "$filter=tags.item all 'STRING X;STRING Y'";

        Map<String, Object> out = doFactoryServiceQuery(queryString, false);
        assertNotNull(out);
        assertEquals(1, out.keySet().size());
        ExampleService.ExampleServiceState outState1 = Utils.fromJson(
                out.get(document2.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState1.name.equals(document2.name));
    }

    private ServiceDocumentQueryResult doQuery(String query, boolean remote) throws Throwable {
        URI odataQuery = UriUtils.buildUri(this.host, ServiceUriPaths.ODATA_QUERIES, query);

        final ServiceDocumentQueryResult[] qr = { null };
        Operation get = Operation.createGet(odataQuery).setCompletion((ox, ex) -> {
            if (ex != null) {
                if (this.isFailureExpected) {
                    this.host.completeIteration();
                } else {
                    this.host.failIteration(ex);
                }
                return;
            }

            if (this.isFailureExpected) {
                this.host.failIteration(new IllegalStateException("failure was expected"));
                return;
            }

            QueryTask tq = ox.getBody(QueryTask.class);
            qr[0] = tq.results;

            this.host.completeIteration();
        });

        this.host.testStart(1);
        if (remote) {
            get.forceRemote();
        }
        this.host.send(get);
        this.host.testWait();

        if (this.isFailureExpected) {
            return null;
        }

        ServiceDocumentQueryResult res = qr[0];
        assertNotNull(res);
        if (!query.contains(UriUtils.URI_PARAM_ODATA_COUNT)) {
            assertNotNull(res.documents);
        }

        return res;
    }

    private Map<String, Object> doFactoryServiceQuery(String query, boolean remote)
            throws Throwable {
        URI odataQuery = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK, query
                + "&" + UriUtils.URI_PARAM_ODATA_EXPAND);

        final ServiceDocumentQueryResult[] qr = { null };
        Operation get = Operation.createGet(odataQuery).setCompletion((ox, ex) -> {
            if (ex != null) {
                this.host.failIteration(ex);
            }

            ServiceDocumentQueryResult resutl = ox.getBody(ServiceDocumentQueryResult.class);
            qr[0] = resutl;

            this.host.completeIteration();
        });

        this.host.testStart(1);
        if (remote) {
            get.forceRemote();
        }
        this.host.send(get);
        this.host.testWait();
        ServiceDocumentQueryResult res = qr[0];

        assertNotNull(res);
        assertNotNull(res.documents);

        return res.documents;
    }

    @Test
    public void testLimit() throws Throwable {
        ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
        int c = 5;
        List<String> expectedOrder = new ArrayList<>();
        for (int i = 0; i < c; i++) {
            inState.documentSelfLink = null;
            inState.counter = 1L;
            inState.name = i + "-abcd";
            postExample(inState);
            expectedOrder.add(inState.name);
        }

        // limit + filter
        int limit = c - 2;
        String queryString = "$filter=counter eq 1";
        queryString += "&" + "$limit=" + +limit;
        ServiceDocumentQueryResult res = doOdataQuery(queryString, true);
        assertTrue(res.documentCount == 0);
        assertTrue(res.documentLinks.size() == 0);
        assertTrue(res.documents.size() == 0);
        assertTrue(res.nextPageLink != null);

        long counter = 0;
        String nextPageLink = res.nextPageLink;

        while (nextPageLink != null) {
            res = getNextPage(nextPageLink, true);
            nextPageLink = res.nextPageLink;
            if (res.documentCount % limit == 0) {
                assertNotNull(res.documentCount);
                assertEquals(limit, (long) res.documentCount);
                assertEquals(limit, res.documentLinks.size());
                assertEquals(limit, res.documents.size());
            } else {
                long expectedCount = c - counter;
                assertEquals((Long) expectedCount, res.documentCount);
                assertEquals(expectedCount, res.documentLinks.size());
                assertEquals(expectedCount, res.documents.size());
            }
            counter = counter + res.documentCount;
        }

        assertTrue(counter == c);
    }

    @Test
    public void testLimitWithExpiredDoc() throws Throwable {
        int c = 5;
        for (int i = 0; i < c; i++) {
            ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();

            inState.documentSelfLink = null;
            inState.counter = 1L;
            inState.name = i + "-abcd";

            // make doc 1 and 2 expired
            if (i == 1 || i == 2) {
                inState.documentExpirationTimeMicros = 1;
            }
            postExample(inState);
        }

        // limit=2 + filter
        String queryString = "$filter=counter eq 1";
        queryString += "&" + "$limit=2";
        ServiceDocumentQueryResult res = doOdataQuery(queryString, true);
        assertTrue(res.documentCount == 0);
        assertTrue(res.documentLinks.size() == 0);
        assertTrue(res.documents.size() == 0);
        assertTrue(res.nextPageLink != null);

        // get second page
        res = getNextPage(res.nextPageLink, true);
        assertNotNull(res.documentCount);
        assertEquals(2, (long) res.documentCount);
        assertNotNull("link to 3rd page should exist", res.nextPageLink);

        // get third page
        res = getNextPage(res.nextPageLink, true);
        assertNotNull(res.documentCount);
        assertEquals(1, (long) res.documentCount);
        assertNull("this is the last page. should have no nextPageLink", res.nextPageLink);
    }

    private ServiceDocumentQueryResult getNextPage(final String nextPage, final boolean remote)
            throws Throwable {
        URI odataQuery = UriUtils.buildUri(this.host, nextPage);
        return doQuery(odataQuery, remote);
    }

    private ServiceDocumentQueryResult getNextPage(final String page, final String peer,
            final boolean remote) throws Throwable {
        String queryString = String.format("%s=%s&%s=%s", UriUtils.URI_PARAM_ODATA_NODE, peer,
                UriUtils.URI_PARAM_ODATA_SKIP_TO, page);
        return doOdataQuery(queryString, remote);
    }

    private ServiceDocumentQueryResult doOdataQuery(final String query, final boolean remote)
            throws Throwable {
        URI odataQuery = UriUtils.buildUri(this.host, ServiceUriPaths.ODATA_QUERIES, query);
        return doQuery(odataQuery, remote);
    }

    private ServiceDocumentQueryResult doQuery(final URI uri, final boolean remote)
            throws Throwable {
        final ServiceDocumentQueryResult[] qr = { null };
        Operation get = Operation.createGet(uri).setCompletion((ox, ex) -> {
            if (ex != null) {
                if (this.isFailureExpected) {
                    this.host.completeIteration();
                } else {
                    this.host.failIteration(ex);
                }
                return;
            }

            if (this.isFailureExpected) {
                this.host.failIteration(new IllegalStateException("failure was expected"));
                return;
            }

            QueryTask tq = ox.getBody(QueryTask.class);
            qr[0] = tq.results;

            this.host.completeIteration();
        });

        this.host.testStart(1);
        if (remote) {
            get.forceRemote();
        }
        this.host.send(get);
        this.host.testWait();

        if (this.isFailureExpected) {
            return null;
        }

        ServiceDocumentQueryResult res = qr[0];
        assertNotNull(res);
        assertNotNull(res.documents);

        return res;
    }
}
