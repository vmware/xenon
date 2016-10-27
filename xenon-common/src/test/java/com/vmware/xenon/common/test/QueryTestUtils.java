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

package com.vmware.xenon.common.test;

import java.net.URI;
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Query and index manipulation utility methods.
 * Note: For test code only!
 */
public final class QueryTestUtils {
    private QueryTestUtils() {

    }

    public static void logVersionInfoForService(TestRequestSender s, URI serviceUri,
            long targetVersion) {
        URI indexUri = UriUtils.buildUri(serviceUri, ServiceUriPaths.CORE_DOCUMENT_INDEX);
        indexUri = UriUtils.extendUriWithQuery(indexUri,
                ServiceDocument.FIELD_NAME_SELF_LINK, serviceUri.getPath(),
                ServiceDocument.FIELD_NAME_VERSION, Long.toString(targetVersion));
        Operation rop = s.sendAndWait(Operation.createGet(indexUri));
        Logger.getAnonymousLogger()
                .info("GET " + indexUri + " " + Utils.toJsonHtml(rop.getBodyRaw()));

        Query qs = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, serviceUri.getPath())
                .build();
        QueryTask q = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.INCLUDE_ALL_VERSIONS)
                .addOption(QueryOption.EXPAND_CONTENT)
                .setQuery(qs)
                .build();
        URI queryUri = UriUtils.buildUri(serviceUri, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        Operation queryOp = s.sendAndWait(Operation.createPost(queryUri).setBody(q));
        Logger.getAnonymousLogger().info(Utils.toJsonHtml(queryOp.getBodyRaw()));
    }

    public static URI startAndSubscribeToContinuousQuery(TestRequestSender s, ServiceHost host,
            QueryTask qt, Consumer<Operation> notify) {
        qt.querySpec.options.add(QueryOption.CONTINUOUS);
        qt.querySpec.options.add(QueryOption.EXPAND_CONTENT);

        Operation post = Operation.createPost(
                UriUtils.buildUri(host.getUri(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS))
                .setBody(qt);
        post = s.sendAndWait(post);
        QueryTask activeTask = post.getBody(QueryTask.class);
        URI queryTaskURI = UriUtils.buildUri(host.getUri(), activeTask.documentSelfLink);

        // Wait for query task to have data
        TestContext.waitFor(s.getTimeout(), () -> {
            QueryTask queryTaskResponse = s.sendGetAndWait(queryTaskURI.toString(),
                    QueryTask.class);
            if (queryTaskResponse.results != null
                    && queryTaskResponse.results.documentLinks != null
                    && !queryTaskResponse.results.documentLinks.isEmpty()) {
                qt.documentSelfLink = queryTaskResponse.documentSelfLink;
                qt.results = queryTaskResponse.results;
                return true;
            }
            return false;
        });

        // Subscribe to the query
        TestContext ctx = new TestContext(1, s.getTimeout());
        Operation subscribe = Operation.createPost(queryTaskURI)
                .setReferer(host.getUri())
                .setCompletion(ctx.getCompletion());

        URI notificationURI = host.startSubscriptionService(subscribe, notify,
                ServiceSubscriber.create(false));
        ctx.await();
        return notificationURI;
    }

    public static void stopContinuousQuerySubscription(TestRequestSender s, ServiceHost host,
            URI notificationTarget, QueryTask qt) {
        TestContext ctx = new TestContext(1, s.getTimeout());
        Operation delete = Operation
                .createDelete(UriUtils.buildUri(notificationTarget, qt.documentSelfLink))
                .setReferer(host.getUri())
                .setCompletion(ctx.getCompletion());
        host.stopSubscriptionService(delete, notificationTarget);
        ctx.await();
    }

}
