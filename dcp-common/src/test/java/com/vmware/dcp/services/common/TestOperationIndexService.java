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

package com.vmware.dcp.services.common;

import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.CommandLineArgumentParser;
import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.common.test.VerificationHost;

public class TestOperationIndexService {

    public VerificationHost host;

    /**
     * Command line argument specifying request count
     */
    public int updateCount = 100;

    @Before
    public void setUp() throws Exception {
        CommandLineArgumentParser.parseFromProperties(this);

        this.host = VerificationHost.create(0, null);
        CommandLineArgumentParser.parseFromProperties(this.host);

        try {
            this.host.start();
            // Start the tracing service
            this.host.toggleOperationTracing(this.host.getUri(), true);
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            this.host.toggleOperationTracing(this.host.getUri(), false);
        } catch (Throwable e) {
            throw new Exception(e);
        }
        this.host.tearDown();
    }

    @Test
    public void testRestart() throws Throwable {
        this.host.toggleOperationTracing(this.host.getUri(), false);
        this.host.toggleOperationTracing(this.host.getUri(), true);
    }

    @Test
    public void testPost() throws Throwable {
        this.host.testStart(this.updateCount);

        HashMap<String, Object> stateCountMap = new HashMap<>();

        for (int i = 0; i < this.updateCount; i++) {
            ExampleService.ExampleServiceState state = new ExampleService.ExampleServiceState();
            state.counter = (long) i;
            state.name = String.format("0x%08x", i);

            stateCountMap.put(state.name, 0);

            Operation op = Operation
                    .createPost(UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK))
                    .setBody(state)
                    .setCompletion(this.host.getCompletion())
                    .setReferer(this.host.getReferer());

            this.host.sendRequest(op);
        }

        this.host.testWait();
        this.host.logThroughput();

        // Now query for the documents
        QueryTask q = new QueryTask();
        q.querySpec = new QueryTask.QuerySpecification();
        q.querySpec.options = EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);
        q.taskInfo.isDirect = true;

        q.querySpec.query.setTermPropertyName("path");
        q.querySpec.query.setTermMatchValue(ExampleFactoryService.SELF_LINK);
        q.indexLink = ServiceUriPaths.CORE_OPERATION_INDEX;

        // We need to poll even when testWait tells us the POST is done.
        final boolean[] foundAllExpectedResults = { false };

        Operation queryOp = Operation
                .createPost(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_QUERY_TASKS))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    QueryTask query = o.getBody(QueryTask.class);

                    if (query.results == null ||
                            query.results.documentLinks == null ||
                            query.results.documentLinks.size() != this.updateCount * 2) {
                        // didn't return all results.  Try again.
                        this.host.completeIteration();
                        return;
                    }

                    foundAllExpectedResults[0] = true;
                    for (Object d : query.results.documents.values()) {
                        Operation.SerializedOperation sop = Utils.fromJson(d,
                                Operation.SerializedOperation.class);
                        if (!sop.documentKind.equals(Operation.SerializedOperation.KIND)) {
                            this.host.failIteration(new IllegalStateException("kind not equal"));
                            return;
                        }

                        ExampleService.ExampleServiceState state = Utils.fromJson(
                                sop.jsonBody, ExampleService.ExampleServiceState.class);

                        int curCount = (int) stateCountMap.get(state.name);
                        if (curCount != (int) stateCountMap.replace(state.name,
                                curCount + 1)) {
                            this.host
                                    .failIteration(new IllegalStateException("curCount not equal"));
                            return;
                        }
                    }

                    this.host.completeIteration();
                });

        while (new Date().before(this.host.getTestExpiration())) {
            this.host.testStart(1);
            this.host.send(queryOp.setBody(Utils.clone(q)));
            this.host.testWait();
            if (foundAllExpectedResults[0]) {
                break;
            }
            Thread.sleep(250);
        }

        if (new Date().after(this.host.getTestExpiration())) {
            new TimeoutException();
        }

        // Each operation sent by the test should be indexed twice (once in sendRequest, and
        // once in handleRequest).  Verify that for each state we POSTED, the query returned 2 entries.
        for (Object v : stateCountMap.values()) {
            assertTrue((int) v == 2);
        }

        int c = 100;
        // Generate some traffic to black-listed URIs to check the operations don't get indexed.
        this.host.testStart(c);
        for (int i = 0; i < c; i++) {
            this.host.sendRequest(Operation
                    .createGet(UriUtils.buildUri(this.host, ServiceUriPaths.DEFAULT_NODE_GROUP))
                    .setReferer(this.host.getUri())
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        // Verify the blacklist by querying for everything in the op index.
        q.querySpec.query.setTermMatchType(QueryTask.QueryTerm.MatchType.WILDCARD);
        q.querySpec.query.setTermMatchValue("*");
        queryOp.setBody(q)
                .setCompletion((o, e) -> {
                    try {
                        if (e != null) {
                            throw e;
                        }

                        QueryTask query = o.getBody(QueryTask.class);
                        if (query.results == null) {
                            throw new IllegalStateException("no results");
                        }

                        // we have at least updateCount * 2 worth of documents
                        if (query.results.documentLinks.size() < this.updateCount * 2) {
                            throw new IllegalStateException("expected more operations");
                        }

                        // Check that there are no greater than the above + some fudge factor.  We don't want
                        // too many documents in the index (thereby verifying the blacklist is working as intended).
                        // Use 10% of updateCount as the fudge factor.  Anything greater than that just sounds unreasonable.
                        if (query.results.documentLinks.size() > (this.updateCount * 2 + (this.updateCount / 10))) {
                            throw new IllegalStateException("too many operations found");
                        }

                        this.host.completeIteration();
                    } catch (Throwable e1) {
                        this.host.failIteration(e1);
                    }
                });

        this.host.testStart(1);
        this.host.send(queryOp);
        this.host.testWait();

    }
}
