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

package ${groupId};

import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Utilities that are useful to testing Xenon
 */
@SuppressWarnings({"WeakerAccess", "Duplicates"})
public class TestUtils {
    private TestUtils() {}

    /**
     * Repeatedly ask a query until the results reach a certain count.
     *
     * @param h - Xenon Service host, used to form URLs and get logger
     * @param query - usually built with QueryTask.Builder.createDirectTask()
     * @param count - result count desired.
     * @param timeout - millisconds to wait before giving up.
     */
    static void waitUntilQueryResultsCountEquals(ServiceHost h, Query query, int count, long timeout) {
        TestRequestSender sender = new TestRequestSender(h);
        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(query)
                .addOption(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT)
                .build();
        TestContext.waitFor(Duration.ofMillis(timeout), () -> {
            Operation post = Operation.createPost(h, ServiceUriPaths.CORE_QUERY_TASKS).setBody(queryTask);
            QueryTask result = sender.sendAndWait(post, QueryTask.class);
            // h.log(Level.INFO, "%d/%d", result.results.documentCount, count);
            return result.results.documentCount == count;
        }, () -> {
                fail("wait timed out");
                return "wait timed out";
            });
    }

    /**
     *
     * @param host - used to construct URL base and also get the logger
     * @param stateClass - document class to be queried
     * @param count - number of documents expected
     * @param timeout - time to wait before giving up
     */
    static void checkServiceCount(ServiceHost host,
                                  Class<? extends ServiceDocument> stateClass,
                                  int count,
                                  long timeout) {
        host.log(Level.INFO, "Checking service count for " + stateClass.toString());
        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(stateClass)
                .build();
        TestUtils.waitUntilQueryResultsCountEquals(host, query, count, timeout);
    }
}
