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

package example.group;

import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Utilities that are useful to testing Xenon
 */
public class TestUtils {
    private TestUtils() {}

    /**
     * Create and wait for specified query until the results reach a certain count.
     *
     * @param h - Xenon Service host, used to form URLs and get logger
     * @param query - usually built with QueryTask.Builder.createDirectTask()
     * @param count - result count desired.
     */
    static void waitUntilQueryResultsCountEquals(ServiceHost host, Query query, int count) {
        TestRequestSender sender = new TestRequestSender(host);
        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(query)
                .build();
        // setting expectedResultCount in the query specification to keep query
        // from completing until result count is satisfied
        queryTask.querySpec.expectedResultCount = (long)count;
        Operation post = Operation.createPost(host, ServiceUriPaths.CORE_QUERY_TASKS).setBody(queryTask);
        sender.sendAndWait(post, QueryTask.class);
    }

    /**
     * Creates a query task that will not complete unless the result count
     * of the query matches the supplied value.
     * Note that a GET to the factory, and counting the results would be
     * equivalent to this query, but would have requird a polling loop
     * and the use of TestContext.waitFor()
     *
     * @param host - used to construct URL base and also get the logger
     * @param stateClass - document class to be queried
     * @param count - number of documents expected
     */
    static void waitForServiceCountEquals(ServiceHost host,
                                    Class<? extends ServiceDocument> stateClass,
                                    int count) {
        host.log(Level.INFO, "Checking service count for " + stateClass.toString());
        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(stateClass)
                .build();
        waitUntilQueryResultsCountEquals(host, query, count);
    }
}
