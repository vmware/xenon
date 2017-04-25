/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.instrumentation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class QueryResponseTimeTest {

    public static final Long[] DEFAULT_DOC_COUNTS() {
        return new Long[] { 1000L, 10000L, 100000L };
    }

    private static final String DOC_COUNT_PROPERTY = "queryResponseTimeTest.docCounts";
    private static VerificationHost HOST;
    private static Long[] docCounts;

    @BeforeClass
    public static void beforeClass() {
        docCounts = safeParseDocCountsFromProperties();
        if (docCounts == null) {
            docCounts = DEFAULT_DOC_COUNTS();
        }
    }

    public void setup() throws Throwable {
        HOST = VerificationHost.create(0);
        HOST.start();
    }

    public void teardown() {
        HOST.tearDown();
    }

    @Test
    public void test() throws Throwable {

        Logger logger = Logger.getAnonymousLogger();
        logger.info(String.format("Running %s with following doc counts: %s", 
                this.getClass().getName(), Arrays.toString(docCounts)));

        for (long count : docCounts) {
            setup();

            long docGenMillis = generateDocs(count);
            long countQMillis = verifyCount(count); // ensure all writes have been flushed
            long queryMillis = performQueryTask(count);

            HOST.log("Doc count: %d, gen took %d ms, count query took %d ms, query took %d ms",
                    count, docGenMillis, countQMillis, queryMillis);

            teardown();
        }
    }

    private static Long[] safeParseDocCountsFromProperties() {
        Map<String, String> clargs = CommandLineArgumentParser.parsePairsFromProperties();
        String docCountPropertyValue = clargs.get(DOC_COUNT_PROPERTY);
        if (docCountPropertyValue != null && docCountPropertyValue.length() > 0) {
            try {
                return Arrays.stream(docCountPropertyValue.split(","))
                        .map(s -> Long.valueOf(s).longValue())
                        .toArray(Long[]::new);
            } catch (Exception e) {
                HOST.log(Level.WARNING, "Error parsing doc counts: %s. Using defaults...",
                        docCountPropertyValue);
            }
        }
        return null;
    }

    private long performQueryTask(long total) {
        long docIndex = (long) (Math.random() * total);

        Timer timer = Timer.start();
        QueryTask.Query.Builder b = QueryTask.Query.Builder.create();
        b.addKindFieldClause(ExampleServiceState.class);
        b.addFieldClause("name", "foo-" + docIndex);
        Query query = b.build();

        QueryTask qTask = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.INCLUDE_ALL_VERSIONS).addOption(QueryOption.EXPAND_CONTENT)
                .setQuery(query).build();

        Operation queryOp = Operation
                .createPost(UriUtils.buildUri(HOST, ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(qTask);
        QueryTask queryTaskResult = HOST.getTestRequestSender().sendAndWait(queryOp,
                QueryTask.class);

        Assert.assertEquals(Long.valueOf(1L), queryTaskResult.results.documentCount);

        return timer.stop();
    }

    private long generateDocs(long total) {
        Timer timer = Timer.start();

        int batch = 10000;

        List<Operation> ops = new ArrayList<>();
        for (long i = 0; i < total; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            ops.add(Operation.createPost(UriUtils.buildUri(HOST, ExampleService.FACTORY_LINK))
                    .setBody(state));

            if (i != 0 && i % batch == 0) {
                Timer looptimer = Timer.start();
                HOST.getTestRequestSender().sendAndWait(ops);
                long docCreateElapsedMillis = looptimer.stop();
                HOST.log("populating data: i=%,d, took=%d", i, docCreateElapsedMillis);
                ops.clear();
            }
        }

        // send remaining
        long lastStart = System.currentTimeMillis();
        HOST.getTestRequestSender().sendAndWait(ops);
        long lastEnd = System.currentTimeMillis();
        HOST.log("populating remaining: took=%d", lastEnd - lastStart);

        return timer.stop();
    }

    private long verifyCount(long expectedDocCount) {
        Timer timer = Timer.start();

        Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask qTask = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.COUNT)
                .setQuery(query).build();

        Operation queryOp = Operation
                .createPost(UriUtils.buildUri(HOST, ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(qTask);

        QueryTask qTaskResult = HOST.getTestRequestSender().sendAndWait(queryOp, QueryTask.class);

        Assert.assertEquals(expectedDocCount, qTaskResult.results.documentCount.longValue());

        return timer.stop();
    }

    static class Timer {
        private long startTime;

        private Timer() {
            startTime = System.currentTimeMillis();
        }

        public static Timer start() {
            Timer timer = new Timer();
            return timer;
        }

        public long stop() {
            long endTime = System.currentTimeMillis();
            return endTime - startTime;
        }
    }
}
