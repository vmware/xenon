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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.QueryTaskClientHelper.ResultHandler;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

public class TestQueryTaskClientHelper extends BasicReusableHostTestCase {
    private QueryTaskClientHelper<MinimalTestServiceState> queryHelper;
    private List<MinimalTestServiceState> minimalTestStates;
    private String idValue1;
    private String idValue2;
    private List<Service> services;

    @Before
    public void setUp() throws Throwable {
        super.setUpPerMethod();
        this.queryHelper = QueryTaskClientHelper.create(MinimalTestServiceState.class);
        this.minimalTestStates = new ArrayList<>();
        this.idValue1 = "value1" + UUID.randomUUID().toString();
        this.idValue2 = "value2" + UUID.randomUUID().toString();
        this.services = new ArrayList<>();
    }

    @After
    public void tearDown() throws Throwable {
        for (Service service : this.services) {
            delete(service.getUri());
        }
    }

    @Test
    public void testQueryDocument() throws Throwable {
        this.minimalTestStates = queryDocument("testLink");
        assertEquals(0, this.minimalTestStates.size());

        MinimalTestServiceState minimalTestState = new MinimalTestServiceState();
        minimalTestState.id = this.idValue1;
        minimalTestState = doPost(minimalTestState);

        this.minimalTestStates = queryDocument(minimalTestState.documentSelfLink);
        assertEquals(1, this.minimalTestStates.size());
        assertEquals(minimalTestState.documentSelfLink,
                this.minimalTestStates.get(0).documentSelfLink);
        assertEquals(this.idValue1, this.minimalTestStates.get(0).id);

        delete(minimalTestState);
        this.minimalTestStates = queryDocument(minimalTestState.documentSelfLink);
        assertEquals(0, this.minimalTestStates.size());
    }

    @Test
    public void testQueryUpdatedDocumentSince() throws Throwable {
        long futureDate = Utils.getNowMicrosUtc() + TimeUnit.DAYS.toMicros(1);
        try {
            this.queryHelper.setUpdatedSince(futureDate);
            fail("IllegalArgumentException expected when date in future.");
        } catch (IllegalArgumentException e) {
            //expected
        }

        long startTime = Utils.getNowMicrosUtc();
        this.minimalTestStates = queryDocumentUpdatedSince(startTime, "testLink");
        assertEquals(0, this.minimalTestStates.size());

        MinimalTestServiceState minimalTestState = new MinimalTestServiceState();
        minimalTestState.id = this.idValue1;
        minimalTestState = doPost(minimalTestState);

        long timeAfterPost = Utils.getNowMicrosUtc();

        // match time but invalid link
        this.minimalTestStates = queryDocumentUpdatedSince(startTime, "testLink");
        assertEquals(0, this.minimalTestStates.size());

        // match link but invalid time
        this.minimalTestStates = queryDocumentUpdatedSince(timeAfterPost,
                minimalTestState.documentSelfLink);
        assertEquals(0, this.minimalTestStates.size());

        // match link but invalid time
        this.minimalTestStates = queryDocumentUpdatedSince(startTime,
                minimalTestState.documentSelfLink);
        assertEquals(1, this.minimalTestStates.size());

        minimalTestState.id = this.idValue2;
        minimalTestState = doPatch(minimalTestState);
        assertEquals(this.idValue2, minimalTestState.id);

        long timeAfterPatch = Utils.getNowMicrosUtc();

        // the delta for the update should be retrieved
        this.minimalTestStates = queryDocumentUpdatedSince(timeAfterPost,
                minimalTestState.documentSelfLink);
        assertEquals(1, this.minimalTestStates.size());
        assertEquals(this.idValue2, this.minimalTestStates.get(0).id);

        // no updates after patch
        this.minimalTestStates = queryDocumentUpdatedSince(timeAfterPatch,
                minimalTestState.documentSelfLink);
        assertEquals(0, this.minimalTestStates.size());

        delete(minimalTestState);

        long timeAfterDelete = Utils.getNowMicrosUtc();

        this.minimalTestStates = queryDocumentUpdatedSince(timeAfterPatch,
                minimalTestState.documentSelfLink);
        assertEquals(1, this.minimalTestStates.size());
        assertTrue(ServiceDocument.isDeleted(this.minimalTestStates.get(0)));

        this.minimalTestStates = queryDocumentUpdatedSince(timeAfterDelete,
                minimalTestState.documentSelfLink);
        assertEquals(0, this.minimalTestStates.size());
    }

    @Test
    public void testQueryUpdatedSince() throws Throwable {
        long startTime = Utils.getNowMicrosUtc();

        MinimalTestServiceState minimalTestState = new MinimalTestServiceState();
        minimalTestState.id = this.idValue1;
        minimalTestState = doPost(minimalTestState);

        MinimalTestServiceState minimalTestState1 = new MinimalTestServiceState();
        minimalTestState1.id = this.idValue1;
        minimalTestState1 = doPost(minimalTestState1);

        this.minimalTestStates = queryUpdatedSince(startTime);

        int countAfterMinState1 = 2;
        assertEquals(countAfterMinState1, this.minimalTestStates.size());
        long timeAfterMinState1 = Utils.getNowMicrosUtc();

        minimalTestState = new MinimalTestServiceState();
        minimalTestState.id = this.idValue2;
        doPost(minimalTestState);

        MinimalTestServiceState minimalTestState2 = new MinimalTestServiceState();
        minimalTestState2.id = this.idValue2;
        minimalTestState2 = doPost(minimalTestState2);

        this.minimalTestStates = queryUpdatedSince(startTime);

        int countAfterMinState2 = 2;
        assertEquals(countAfterMinState1 + countAfterMinState2, this.minimalTestStates.size());
        long timeAfterMinState2 = Utils.getNowMicrosUtc();

        this.minimalTestStates = queryUpdatedSince(timeAfterMinState1);
        assertEquals(countAfterMinState2, this.minimalTestStates.size());

        boolean match = false;
        for (MinimalTestServiceState state : this.minimalTestStates) {
            if (minimalTestState2.documentSelfLink.equals(state.documentSelfLink)) {
                assertNotNull(minimalTestState2.id);
                match = true;
            }
        }
        assertTrue(match);

        this.minimalTestStates = queryUpdatedSince(timeAfterMinState2);
        assertEquals(0, this.minimalTestStates.size());

        long timeBeforeDeletion = Utils.getNowMicrosUtc();
        delete(minimalTestState1);
        this.minimalTestStates = queryUpdatedSince(startTime);
        assertEquals(countAfterMinState1 + countAfterMinState2, this.minimalTestStates.size());

        boolean deleted = false;
        for (MinimalTestServiceState state : this.minimalTestStates) {
            if (minimalTestState1.documentSelfLink.equals(state.documentSelfLink)) {
                assertTrue(ServiceDocument.isDeleted(state));
                deleted = true;
            }
        }
        assertTrue(deleted);
        delete(minimalTestState2);

        this.minimalTestStates = queryUpdatedSince(timeBeforeDeletion);
        assertEquals(countAfterMinState1 + countAfterMinState2 - 2, this.minimalTestStates.size());
        for (MinimalTestServiceState state : this.minimalTestStates) {
            assertTrue(ServiceDocument.isDeleted(state));
        }

        this.minimalTestStates = queryUpdatedSince(Utils.getNowMicrosUtc());
        assertEquals(0, this.minimalTestStates.size());
    }

    @Test
    public void testQueryGetAllWithPagination() throws Throwable {
        int numberOfServices = 20;
        this.services = this.host.doThroughputServiceStart(numberOfServices,
                MinimalTestService.class,
                this.host.buildMinimalTestState(), EnumSet.of(ServiceOption.PERSISTENCE), null);

        Query query = Builder.create().addKindFieldClause(MinimalTestServiceState.class).build();
        QueryTask q = QueryTask.Builder.createDirectTask()
                .setResultLimit(numberOfServices / 4)
                .setQuery(query).build();

        List<String> resultLinks = new ArrayList<>();
        query(q, (r, e) -> {
            if (e != null) {
                this.host.failIteration(e);
            } else if (!r.hasResult()) {
                this.host.completeIteration();
            } else {
                resultLinks.add(r.getDocumentSelfLink());
            }
        });
        assertEquals(20, resultLinks.size());
        assertNotNull(resultLinks.get(0));

    }

    @Test
    public void testQueryNotDirect() throws Throwable {
        int numberOfServices = 20;
        this.services = this.host.doThroughputServiceStart(numberOfServices,
                MinimalTestService.class,
                this.host.buildMinimalTestState(), EnumSet.of(ServiceOption.PERSISTENCE), null);

        Query query = Builder.create().addKindFieldClause(MinimalTestServiceState.class).build();
        QueryTask q = QueryTask.Builder.create()
                .setQuery(query).build();

        List<String> resultLinks = new ArrayList<>();
        query(q, (r, e) -> {
            if (e != null) {
                this.host.failIteration(e);
            } else if (!r.hasResult()) {
                this.host.completeIteration();
            } else {
                resultLinks.add(r.getDocumentSelfLink());
            }
        });
        assertEquals(20, resultLinks.size());
        assertNotNull(resultLinks.get(0));
    }

    @Test
    public void testQueryExpanded() throws Throwable {
        int numberOfServices = 20;
        this.services = this.host.doThroughputServiceStart(numberOfServices,
                MinimalTestService.class,
                this.host.buildMinimalTestState(), EnumSet.of(ServiceOption.PERSISTENCE), null);

        Query query = Builder.create().addKindFieldClause(MinimalTestServiceState.class).build();
        QueryTask q = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.EXPAND_CONTENT)
                .setQuery(query).build();

        List<MinimalTestServiceState> results = new ArrayList<>();
        query(q, (r, e) -> {
            if (e != null) {
                this.host.failIteration(e);
            } else if (!r.hasResult()) {
                this.host.completeIteration();
            } else {
                results.add(r.getResult());
            }
        });
        assertEquals(20, results.size());
        assertNotNull(results.get(0));
    }

    @Test
    public void testCountQuery() throws Throwable {
        int numberOfServices = 20;
        this.services = this.host.doThroughputServiceStart(numberOfServices,
                MinimalTestService.class,
                this.host.buildMinimalTestState(), EnumSet.of(ServiceOption.PERSISTENCE), null);

        Query query = Builder.create().addKindFieldClause(MinimalTestServiceState.class).build();
        QueryTask q = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.COUNT)
                .setQuery(query).build();

        List<Long> results = new ArrayList<>();
        query(q, (r, e) -> {
            if (e != null) {
                this.host.failIteration(e);
            } else if (!r.hasResult()) {
                this.host.completeIteration();
            } else {
                results.add(r.getCount());
            }
        });

        assertEquals(1, results.size());
        assertEquals(20, results.get(0).intValue());
    }

    @Test
    public void testFailIfQueryTaskOrResultCompletionNotSet() throws Throwable {
        try {
            this.queryHelper.setQueryTask(null);
            fail("IllegalArgumentException expected when argument null.");
        } catch (IllegalArgumentException e) {
            //expected
        }

        try {
            this.queryHelper.setResultHandler(null);
            fail("IllegalArgumentException expected when argument null.");
        } catch (IllegalArgumentException e) {
            //expected
        }

        try {
            this.queryHelper.sendWith(null);
            fail("IllegalArgumentException expected when argument null.");
        } catch (IllegalArgumentException e) {
            //expected
        }

        this.queryHelper = QueryTaskClientHelper.create(MinimalTestServiceState.class);
        try {
            this.queryHelper
                    .setResultHandler((r, e) -> {
                        this.host.completeIteration();
                    })
                    .sendWith(host);
            fail("IllegalArgumentException expected when argument null.");
        } catch (IllegalArgumentException e) {
            //expected
        }

        this.queryHelper = QueryTaskClientHelper.create(MinimalTestServiceState.class);
        QueryTask q = QueryTask.Builder.createDirectTask().build();
        try {
            this.queryHelper
                    .setQueryTask(q)
                    .sendWith(host);
            fail("IllegalArgumentException expected when argument null.");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    private MinimalTestServiceState doPost(MinimalTestServiceState minimalTestState)
            throws Throwable {
        List<Service> currentServices = this.host.doThroughputServiceStart(1,
                MinimalTestService.class,
                minimalTestState, EnumSet.of(ServiceOption.PERSISTENCE), null);
        this.services.addAll(currentServices);

        MinimalTestServiceState[] result = new MinimalTestServiceState[] { null };
        this.host.testStart(1);
        this.host.sendRequest(Operation.createGet(currentServices.get(0).getUri())
                .setReferer(this.host.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    result[0] = o.getBody(MinimalTestServiceState.class);
                    this.host.completeIteration();
                }));
        this.host.testWait();
        return result[0];
    }

    private MinimalTestServiceState doPatch(MinimalTestServiceState minimalTestState)
            throws Throwable {
        this.host.testStart(1);
        MinimalTestServiceState[] result = new MinimalTestServiceState[] { null };
        this.host.sendRequest(Operation
                .createPatch(UriUtils.buildUri(this.host, minimalTestState.documentSelfLink))
                .setBody(minimalTestState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    result[0] = o.getBody(MinimalTestServiceState.class);
                    this.host.completeIteration();
                })
                .setReferer(this.host.getUri()));
        this.host.testWait();
        return result[0];
    }

    private void delete(MinimalTestServiceState minimalTestState) throws Throwable {
        URI uri = UriUtils.buildUri(this.host, minimalTestState.documentSelfLink);
        if (this.services != null) {
            Iterator<Service> iter = this.services.iterator();
            while (iter.hasNext()) {
                Service next = iter.next();
                if (next.getUri().equals(uri)) {
                    iter.remove();
                }
            }
        }

        delete(uri);
    }

    private void delete(URI uri) throws Throwable {
        this.host.testStart(1);
        this.host.sendRequest(Operation
                .createDelete(uri)
                .setBody(new ServiceDocument())
                .setCompletion(this.host.getCompletion())
                .setReferer(this.host.getUri()));
        this.host.testWait();
    }

    private List<MinimalTestServiceState> queryDocument(String documentSelfLink) throws Throwable {
        this.host.testStart(1);
        this.queryHelper.setDocumentLink(documentSelfLink)
                .setResultHandler(handler())
                .sendWith(host);
        this.host.testWait();
        return this.minimalTestStates;
    }

    private List<MinimalTestServiceState> queryDocumentUpdatedSince(
            long documentSinceUpdateTimeMicros, String documentSelfLink) throws Throwable {
        this.host.testStart(1);
        this.queryHelper.setUpdatedDocumentSince(documentSinceUpdateTimeMicros, documentSelfLink)
                .setResultHandler(handler())
                .sendWith(host);
        this.host.testWait();
        return this.minimalTestStates;
    }

    private List<MinimalTestServiceState> queryUpdatedSince(long timeInMicros) throws Throwable {
        this.host.testStart(1);
        this.queryHelper
                .setUpdatedSince(timeInMicros)
                .setResultHandler(handler())
                .sendWith(host);
        this.host.testWait();
        return this.minimalTestStates;
    }

    private void query(QueryTask queryTask, ResultHandler<MinimalTestServiceState> resultHandler)
            throws Throwable {
        this.host.testStart(1);
        this.queryHelper
                .setQueryTask(queryTask)
                .setResultHandler(resultHandler)
                .sendWith(host);
        this.host.testWait();
    }

    private ResultHandler<MinimalTestServiceState> handler() {
        this.minimalTestStates.clear();
        return (r, e) -> {
            if (e != null) {
                this.host.failIteration(e);
            } else if (r.hasResult()) {
                this.minimalTestStates.add(r.getResult());
            } else {
                this.host.completeIteration();
            }
        };
    }
}
