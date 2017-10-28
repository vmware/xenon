/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.TestResults;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;

public class TestInterleavedQueries extends BasicReusableHostTestCase {

    @Rule
    public TestResults testResults = new TestResults();

    public int operationTimeoutMillis;
    public int errorThreshold;

    public TestInterleavedQueries() {
        // These values picked for IDE configuration: short test,
        // default heap size of 256m

        this.requestCount = 100;
        this.operationTimeoutMillis = 350;
        this.errorThreshold = 5;
    }

    /**
     * How many interleaved queries does it take for system to become unresponsive?
     *
     * @throws Throwable
     */
    @Test
    public void testStress() throws Throwable {
        // assume there are 50 documentKinds each having 20 fields
        final int fieldCount = 50 * 20;
        List<String> keys = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            keys.add("key-" + UUID.randomUUID());
        }

        // create some initial documents
        for (int i = 0; i < 500; i++) {
            ExampleServiceState doc = createBigState(keys);
            Operation postDocument = Operation.createPost(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                    .setBody(doc);

            this.host.getTestRequestSender().sendAndWait(postDocument);
        }

        long start = System.nanoTime();

        int i;
        int errors = this.errorThreshold;
        for (i = 0; i < this.requestCount; i++) {
            this.host.log("Processing request %s", i);
            ExampleServiceState doc = createBigState(keys);
            Operation postDocument = Operation.createPost(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                    .setExpiration(Utils.fromNowMicrosUtc(this.operationTimeoutMillis * 1000))
                    .setBody(doc);

            try {
                this.host.getTestRequestSender().sendAndWait(postDocument);
            } catch (Throwable e) {
                errors--;
                if (errors == 0) {
                    this.host.log("System become unresponsive after %d requests ", i);
                    break;
                }
            }

            QueryTask task = makeQueryTask();

            // don't expire tasks anytime soon: expiring them before the test is over will skew the test
            task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(TimeUnit.MINUTES.toMicros(30));

            Operation postTask = Operation.createPost(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_QUERY_TASKS))
                    .setBody(task);
            this.host.getTestRequestSender().sendAndWait(postTask);
        }

        writeResults(start, i, this.errorThreshold);
    }

    private QueryTask makeQueryTask() {
        return QueryTask.Builder.createDirectTask()
                .setQuery(QueryTask.Query.Builder.create()
                        .addKindFieldClause(ExampleServiceState.class)
                        .addFieldClause(ExampleServiceState.FIELD_NAME_NAME, "a", MatchType.WILDCARD,
                                Occurance.MUST_NOT_OCCUR)
                        .addFieldClause(ExampleServiceState.FIELD_NAME_NAME, "c", MatchType.WILDCARD,
                                Occurance.MUST_NOT_OCCUR)
                        .addFieldClause(ExampleServiceState.FIELD_NAME_NAME, "5", MatchType.WILDCARD,
                                Occurance.MUST_NOT_OCCUR)
                        .build())
                .setResultLimit(50)
                .build();
    }

    /**
     * What's the throughput when processing N request under interleaved queries load?
     *
     * @throws Throwable
     */
    @Test
    public void testThroughput() throws Throwable {
        // assume there are 50 documentKinds each having 20 fields
        final int fieldCount = 50 * 20;
        List<String> keys = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            keys.add("key-" + UUID.randomUUID());
        }

        // create some initial documents
        for (int i = 0; i < 500; i++) {
            ExampleServiceState doc = createBigState(keys);
            Operation postDocument = Operation.createPost(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                    .setBody(doc);

            this.host.getTestRequestSender().sendAndWait(postDocument);
        }

        long start = System.nanoTime();

        int errors = 0;
        int i;
        for (i = 0; i < this.requestCount; i++) {
            this.host.log("Processing request %s", i);
            ExampleServiceState doc = createBigState(keys);
            Operation postDocument = Operation.createPost(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                    .setExpiration(Utils.fromNowMicrosUtc(this.operationTimeoutMillis * 1000))
                    .setBody(doc);

            try {
                this.host.getTestRequestSender().sendAndWait(postDocument);
            } catch (Throwable e) {
                errors++;
                this.host.log("Operation timed out, retrying");
                i--;
                continue;
            }

            QueryTask task = makeQueryTask();

            // don't expire tasks anytime soon: expiring them before the test is over will skew the test
            task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(TimeUnit.MINUTES.toMicros(30));

            Operation postTask = Operation.createPost(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_QUERY_TASKS))
                    .setBody(task);
            this.host.getTestRequestSender().sendAndWait(postTask);
        }

        writeResults(start, i, errors);
    }

    private void writeResults(long start, int opCount, int errors) {
        long duration = System.nanoTime() - start;
        double tput = opCount / TimeUnit.NANOSECONDS.toSeconds(duration);

        this.testResults.getReport().lastValue("ops", opCount);
        this.testResults.getReport().lastValue("time", duration / 1000_000_000.0);
        this.testResults.getReport().lastValue("errors", errors);
        this.testResults.getReport().lastValue(TestResults.KEY_THROUGHPUT, tput);

        this.host.log("throughput: %f ops/s, count = %d", tput, opCount);
    }

    private ExampleServiceState createBigState(List<String> keys) {
        ExampleServiceState res = new ExampleServiceState();
        res.keyValues = new HashMap<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // populate some number of fields
        for (int i = 0; i < 20; i++) {
            res.keyValues.put(keys.get(random.nextInt(keys.size())), "" + new Date());
        }
        res.counter = System.currentTimeMillis();
        res.name = UUID.randomUUID().toString();
        res.required = "random value";
        return res;
    }

}
