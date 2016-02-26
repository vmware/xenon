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
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;

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
    public void orderBy() throws Throwable {
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

        // post an example that will not get past the filter
        inState.documentSelfLink = null;
        inState.counter = 10000L;
        inState.name = 0 + "-abcd";
        postExample(inState);

        // ascending search first
        String queryString = "$filter=counter eq 1";
        queryString += "&" + "$orderby=name asc";

        doOrderByQueryAndValidateResult(c, expectedOrder, queryString);

        // descending search
        queryString = "$filter=counter eq 1";
        queryString += "&" + "$orderby=name desc";
        Collections.reverse(expectedOrder);
        doOrderByQueryAndValidateResult(c, expectedOrder, queryString);

        // pass a bogus order specifier, expect failure.
        this.isFailureExpected = true;
        try {
            queryString = "$filter=counter eq 1";
            queryString += "&" + "$orderby=name something";
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
        assertNotNull(res.documents);

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
}
