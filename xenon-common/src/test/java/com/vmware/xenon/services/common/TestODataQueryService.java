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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleFactoryService;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestODataQueryService {
    public VerificationHost host;

    public long min = 10;
    public long max = 30;
    public List<String> selfLinks;

    @Before
    public void setUp() throws Exception {
        CommandLineArgumentParser.parseFromProperties(this);

        this.host = VerificationHost.create(0, null);
        CommandLineArgumentParser.parseFromProperties(this.host);

        try {
            this.host.start();
            this.selfLinks = postExample(this.min, this.max);
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        this.host.tearDown();
    }

    private List<String> postExample(long min, long max) throws Throwable {
        List<String> selfLinks = new ArrayList<>();

        this.host.testStart(max - min + 1);
        for (long i = min; i <= max; i++) {
            ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
            inState.name = String.format("name-%d", i);
            inState.counter = i;

            this.host.send(Operation.createPost(UriUtils.extendUri(this.host.getUri(),
                    ExampleFactoryService.SELF_LINK)).setBody(inState)
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
                .createPost(UriUtils.buildUri(this.host.getUri(), ExampleFactoryService.SELF_LINK))
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
    public void uriEncodedQueries() throws Throwable {
        testSimpleStringQuery();
        testGTQuery();
        testGEQuery();
        testLTQuery();
        testLEQuery();
        testNumericEqQuery();
    }

    private void testSimpleStringQuery() throws Throwable {
        ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
        inState.name = "TEST STRING";
        postExample(inState);

        String queryString = "$filter=name eq 'TEST STRING'";

        Map<String, Object> out = doQuery(queryString);
        assertNotNull(out);

        ExampleService.ExampleServiceState outState = Utils.fromJson(
                out.get(inState.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState.name.equals(inState.name));

        out = doFactoryServiceQuery(queryString);
        assertNotNull(out);
        outState = Utils.fromJson(
                out.get(inState.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertTrue(outState.name.equals(inState.name));
    }

    private void testGTQuery() throws Throwable {
        // we should get 10 documents back
        String queryString = String.format("$filter=counter gt %d", this.min + 10);

        Map<String, Object> out = doQuery(queryString);
        assertNotNull(out);

        assertEquals(10, out.size());

        out = doFactoryServiceQuery(queryString);
        assertNotNull(out);
        assertEquals(10, out.size());
    }

    private void testGEQuery() throws Throwable {
        // we should get 10 documents back
        String queryString = String.format("$filter=counter ge %d", this.min + 10);

        Map<String, Object> out = doQuery(queryString);
        assertNotNull(out);

        assertEquals(11, out.size());

        out = doFactoryServiceQuery(queryString);
        assertNotNull(out);
        assertEquals(11, out.size());
    }

    private void testLTQuery() throws Throwable {
        // we should get 10 documents back
        String queryString = String.format("$filter=counter lt %d", this.min + 10);

        Map<String, Object> out = doQuery(queryString);
        assertNotNull(out);

        assertEquals(10, out.size());

        out = doFactoryServiceQuery(queryString);
        assertNotNull(out);
        assertEquals(10, out.size());
    }

    private void testLEQuery() throws Throwable {
        // we should get 10 documents back
        String queryString = String.format("$filter=counter le %d", this.min + 10);

        Map<String, Object> out = doQuery(queryString);
        assertNotNull(out);

        assertEquals(11, out.size());

        out = doFactoryServiceQuery(queryString);
        assertNotNull(out);
        assertEquals(11, out.size());
    }

    private void testNumericEqQuery() throws Throwable {
        ExampleService.ExampleServiceState inState = new ExampleService.ExampleServiceState();
        inState.counter = (long) 0xFFFF;
        inState.name = "name required";
        postExample(inState);

        String queryString = String.format("$filter=counter eq %d", inState.counter);

        Map<String, Object> out = doQuery(queryString);
        assertNotNull(out);

        ExampleService.ExampleServiceState outState = Utils.fromJson(
                out.get(inState.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertEquals(outState.counter, inState.counter);

        out = doFactoryServiceQuery(queryString);
        assertNotNull(out);
        outState = Utils.fromJson(
                out.get(inState.documentSelfLink), ExampleService.ExampleServiceState.class);
        assertEquals(outState.counter, inState.counter);
    }

    private Map<String, Object> doQuery(String query) throws Throwable {
        URI odataQuery = UriUtils.buildUri(this.host, ServiceUriPaths.ODATA_QUERIES, query);

        final ServiceDocumentQueryResult[] qr = { null };
        Operation get = Operation.createGet(odataQuery).setCompletion((ox, ex) -> {
            if (ex != null) {
                this.host.failIteration(ex);
            }

            QueryTask tq = ox.getBody(QueryTask.class);
            qr[0] = tq.results;

            this.host.completeIteration();
        });

        this.host.testStart(1);
        this.host.send(get);
        this.host.testWait();
        ServiceDocumentQueryResult res = qr[0];

        assertNotNull(res);
        assertNotNull(res.documents);

        return res.documents;
    }

    private Map<String, Object> doFactoryServiceQuery(String query) throws Throwable {
        URI odataQuery = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK, query
                + "&" + UriUtils.URI_PARAM_ODATA_EXPAND + "=true");

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
        this.host.send(get);
        this.host.testWait();
        ServiceDocumentQueryResult res = qr[0];

        assertNotNull(res);
        assertNotNull(res.documents);

        return res.documents;
    }
}
