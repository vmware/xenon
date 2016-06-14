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

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Generic {@link ServiceDocument} query client to create and execute common {@link QueryTask}s
 * related to common functionality for all {@link ServiceDocument} types.
 *
 * Main functionality is about changes to given documents (this is useful when we have subscription,
 * notifications and so on), simplified way to get a given document if not sure it exists or not
 * (the regular way will block if the document is not created) and just a query accepting QuerySpec
 * but abstract all the code dealing with retrieving the query including direct vs indirect,
 * expanded or not expanded, paginated or not paginated. Usually, all of those variation introduce a
 * lot of repeating code that this class is trying to abstract.
 *
 * The result is kind of iterator completion that can process the results in an unified way without
 * need to know if paginated, direct or expanded.
 *
  * Example usage:
 *
 *<pre>
 *
 *  Query query = Builder.create()
 *             .addKindFieldClause(MinimalTestServiceState.class)
 *             .build();
 *  QueryTask q = QueryTask.Builder.createDirectTask()
 *                .addOption(QueryOption.EXPAND_CONTENT)
 *                .setQuery(query).build();
 *
 *  List<MinimalTestServiceState> results = new ArrayList<>();
 *  QueryTaskClientHelper
 *         .create(MinimalTestServiceState.class)
 *         .setQueryTask(queryTask)
 *         .setResultHandler((r, e) -> {
 *                    if (e != null) {
 *                        //fail or log exception.
 *                    } else if (r.hasResult()) {
 *                       //add to result array
 *                       //or process every document
 *                       this.results.add(r.getResult());
 *                    } else {
 *                      //end of query. either complete operation
 *                      // or call another function.
 *                    })
 *         .sendWith(host);
 * </pre>
 */
public class QueryTaskClientHelper<T extends ServiceDocument> {
    public static final long QUERY_RETRIEVAL_RETRY_INTERVAL_MILLIS = Long.getLong(
            "xenon.querytaskclienthelper.query.retry.interval.millis", 300);
    public static final long DEFAULT_EXPIRATION_TIME_IN_MICROS = Long.getLong(
            "xenon.querytaskclienthelper.query.documents.default.expiration.millis",
            TimeUnit.SECONDS.toMicros(120));
    public static final Integer DEFAULT_QUERY_RESULT_LIMIT = Integer.getInteger(
            "xenon.querytaskclienthelper.query.documents.default.resultLimit", 50);

    private final Class<T> type;
    private ServiceHost host;
    private QueryTask queryTask;
    private ResultHandler<T> resultHandler;
    private URI baseUri;

    private QueryTaskClientHelper(Class<T> type) {
        this.type = type;
    }

    public static <T extends ServiceDocument> QueryTaskClientHelper<T> create(Class<T> type) {
        return new QueryTaskClientHelper<T>(type);
    }

    /**
     * Query for a document based on {@link ServiceDocument#documentSelfLink}. This is the same
     * operation of GET <code>documentSelfLink</code>. It is especially needed when a
     * {@link ServiceDocument} might not exist since using <code>GET</code> directly will timeout if
     * {@link ServiceDocument} doesn't exist (if {@link Operation#PRAGMA_HEADER} not used).
     *
     * @param documentSelfLink
     *            {@link ServiceDocument#documentSelfLink} of the document to be retrieved.
     */
    public QueryTaskClientHelper<T> setDocumentLink(String documentSelfLink) {
        setUpdatedDocumentSince(-1, documentSelfLink);
        return this;
    }

    /**
     * Query an expanded document extending {@link ServiceDocument}s that is updated or deleted
     * since given time in the past. The result will return if there is any update or delete
     * operation from provided documentSinceUpdateTimeMicros.
     *
     * This query could be used to find
     * out if there were any updates since the time it is provided as parameter.
     *
     * @param documentSinceUpdateTimeMicros
     *            Indicating a time since the document was last updated matching the property
     *            {@link ServiceDocument#documentUpdateTimeMicros}.
     */
    public QueryTaskClientHelper<T> setUpdatedDocumentSince(long documentSinceUpdateTimeMicros,
            String documentSelfLink) {
        assertNotNull(documentSelfLink, "documentSelfLink");

        Query query = Builder.create().addKindFieldClause(this.type)
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, documentSelfLink)
                .build();
        QueryTask q = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.EXPAND_CONTENT)
                .setQuery(query).build();

        if (documentSinceUpdateTimeMicros != -1) {
            q.querySpec.options.add(QueryOption.INCLUDE_DELETED);
            q.querySpec.query
                    .addBooleanClause(createUpdatedSinceTimeRange(documentSinceUpdateTimeMicros));
        }

        setQueryTask(q);
        return this;
    }

    /**
     * Query for a list of expanded documents extending {@link ServiceDocument}s that
     * are updated or deleted since given time in the past. The result will include
     * both updated and deleted documents.
     *
     * @param documentSinceUpdateTimeMicros
     *            Indicating a time since the document was last updated matching the property
     *            {@link ServiceDocument#documentUpdateTimeMicros}.
     */
    public QueryTaskClientHelper<T> setUpdatedSince(long documentSinceUpdateTimeMicros) {
        long nowMicrosUtc = Utils.getNowMicrosUtc();
        if (nowMicrosUtc < documentSinceUpdateTimeMicros) {
            throw new IllegalArgumentException(
                    "'documentSinceUpdateTimeMicros' must be in the past.");
        }

        Query query = Builder.create().addKindFieldClause(this.type)
                .addClause(createUpdatedSinceTimeRange(documentSinceUpdateTimeMicros)).build();
        QueryTask q = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.EXPAND_CONTENT)
                .addOption(QueryOption.INCLUDE_DELETED)
                .setResultLimit(DEFAULT_QUERY_RESULT_LIMIT)
                .setQuery(query).build();

        setQueryTask(q);
        return this;
    }

    /**
     * Set the ServiceHost, which will be used to send the request to the Query task service.
     *
     * @param serviceHost
     *
     * @return QueryTaskClientHelper
     */
    public QueryTaskClientHelper<T> sendWith(ServiceHost serviceHost) {
        assertNotNull(serviceHost, "'serviceHost' must not be null.");
        this.host = serviceHost;
        if (this.baseUri == null) {
            this.baseUri = serviceHost.getUri();
        }
        sendQueryRequest();
        return this;
    }

    /**
     * Set queryTask to be used for the query request.
     *
     * @param queryTask
     *
     * @return QueryTaskClientHelper
     */
    public QueryTaskClientHelper<T> setQueryTask(QueryTask queryTask) {
        assertNotNull(queryTask, "'queryTask' must not be null.");
        if (queryTask.documentExpirationTimeMicros == 0) {
            queryTask.documentExpirationTimeMicros = getDefaultQueryExpiration();
        }
        this.queryTask = queryTask;
        return this;
    }

    /**
     * Set the base URI to be used when generating {@link Operation}s.
     *
     * @param baseUri
     *
     * @return QueryTaskClientHelper
     */
    public QueryTaskClientHelper<T> setBaseUri(URI baseUri) {
        assertNotNull(baseUri, "'baseUri' must not be null.");
        this.baseUri = baseUri;
        return this;
    }

    /**
     * Set ResultHandler to be used during completion handling of every element. Either the list of
     * ServiceDocuments will be passed as parameter or exception in case of errors.
     *
     * @param resultHandler
     *            The resultHandler to be called. Either the list of ServiceDocuments will be passed
     *            as parameter or exception in case of errors.
     *
     * @return QueryTaskClientHelper
     */
    public QueryTaskClientHelper<T> setResultHandler(ResultHandler<T> resultHandler) {
        assertNotNull(resultHandler, "'resultHandler' must not be null.");
        this.resultHandler = resultHandler;
        return this;
    }

    public static long getDefaultQueryExpiration() {
        return Utils.getNowMicrosUtc() + DEFAULT_EXPIRATION_TIME_IN_MICROS;
    }

    private void sendQueryRequest() {
        assertNotNull(this.queryTask, "'queryTask' must be set first.");
        assertNotNull(this.resultHandler, "'resultHandler' must be set first.");

        this.host.sendRequest(Operation
                .createPost(UriUtils.extendUri(this.baseUri, ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(this.queryTask)
                .setReferer(this.host.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.resultHandler.handle(noResult(), e);
                        return;
                    }
                    QueryTask qrt = o.getBody(QueryTask.class);
                    processQuery(qrt, this.resultHandler);
                }));
    }

    private void processQuery(QueryTask q, ResultHandler<T> handler) {
        if (TaskState.isFailed(q.taskInfo)) {
            handler.handle(noResult(), new IllegalStateException(q.taskInfo.failure.message));
            return;
        }

        if (q.taskInfo.isDirect || TaskState.isFinished(q.taskInfo)) {
            processQueryResult(q, handler);
            return;
        }

        this.host.sendRequest(Operation
                .createGet(UriUtils.extendUri(this.baseUri, q.documentSelfLink))
                .setReferer(this.host.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        handler.handle(noResult(), e);
                        return;
                    }

                    QueryTask rsp = o.getBody(QueryTask.class);

                    if (!TaskState.isFinished(rsp.taskInfo)) {
                        this.host.log(Level.FINE, "Resource query not complete yet, retrying...");
                        this.host.schedule(() -> processQuery(rsp, handler),
                                QUERY_RETRIEVAL_RETRY_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
                        return;
                    }

                    processQueryResult(rsp, handler);
                }));
    }

    private void processQueryResult(QueryTask rsp, ResultHandler<T> handler) {
        if (rsp.querySpec.resultLimit != null && rsp.querySpec.resultLimit != Integer.MAX_VALUE) {
            // pagination results:
            getNextPageLinks(rsp.results.nextPageLink, rsp.querySpec.resultLimit, handler);
            return;
        }

        processResults(rsp, handler);
    }

    private void processResults(QueryTask rsp, ResultHandler<T> handler) {
        List<String> links = rsp.results.documentLinks;
        if (isExpandQuery(rsp)) {
            if (rsp.results.documents.isEmpty()) {
                handler.handle(noResult(), null);
            } else {
                long count = links.size();
                for (String documentLink : links) {
                    handler.handle(result(rsp.results.documents.get(documentLink), count), null);
                }
                // close the query;
                handler.handle(noResult(), null);
            }
        } else if (isCountQuery(rsp)) {
            handler.handle(countResult(rsp.results.documentCount), null);
            // close the query;
            handler.handle(noResult(), null);
        } else {
            if (links == null || links.isEmpty()) {
                handler.handle(noResult(), null);
            } else {
                for (String selfLink : links) {
                    handler.handle(resultLink(selfLink, links.size()), null);
                }
                // close the query;
                handler.handle(noResult(), null);
            }
        }
    }

    private void getNextPageLinks(String nextPageLink, int resultLimit, ResultHandler<T> handler) {
        try {
            if (nextPageLink == null) {
                handler.handle(noResult(), null);
                return;
            }

            this.host.sendRequest(Operation
                    .createGet(UriUtils.extendUri(this.baseUri, nextPageLink))
                    .setReferer(this.host.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            handler.handle(noResult(), e);
                            return;
                        }
                        try {
                            QueryTask page = o.getBody(QueryTask.class);

                            if (isExpandQuery(page)) {
                                Collection<Object> values = page.results.documents.values();
                                for (Object json : values) {
                                    handler.handle(result(json, values.size()), null);
                                }
                            } else if (isCountQuery(page)) {
                                handler.handle(countResult(page.results.documentCount), null);
                            } else {
                                List<String> links = page.results.documentLinks;
                                for (String link : links) {
                                    handler.handle(resultLink(link, links.size()), null);
                                }
                            }
                            getNextPageLinks(page.results.nextPageLink, resultLimit, handler);
                        } catch (Throwable ex) {
                            handler.handle(noResult(), ex);
                        }
                    }));
        } catch (Throwable ex) {
            handler.handle(noResult(), ex);
        }
    }

    private boolean isExpandQuery(QueryTask q) {
        return q.querySpec.options != null
                && q.querySpec.options.contains(QueryOption.EXPAND_CONTENT);
    }

    private boolean isCountQuery(QueryTask q) {
        return q.querySpec.options != null && q.querySpec.options.contains(QueryOption.COUNT);
    }

    private Query createUpdatedSinceTimeRange(long timeInMicros) {
        long limitToNowInMicros = Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(10);
        NumericRange<Long> range = NumericRange.createLongRange(timeInMicros, limitToNowInMicros,
                true, false);
        range.precisionStep = 64; // 4 and 8 doesn't work. 16 works but set 64 to be certain.
        QueryTask.Query latestSinceCondition = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS)
                .setNumericRange(range)
                .setTermMatchType(MatchType.TERM);
        latestSinceCondition.occurance = Occurance.MUST_OCCUR;
        return latestSinceCondition;
    }

    private static void assertNotNull(Object value, String propertyName) {
        if (value == null) {
            throw new IllegalArgumentException("'" + propertyName + "' is required");
        }
    }

    public QueryElementResult<T> result(Object json, long count) {
        QueryElementResult<T> r = new QueryElementResult<>();
        r.result = Utils.fromJson(json, this.type);
        r.documentSelfLink = r.result.documentSelfLink;
        r.count = count;
        return r;
    }

    public static <S extends ServiceDocument> QueryElementResult<S> resultLink(
            String selfLink, long count) {

        QueryElementResult<S> r = new QueryElementResult<>();
        r.documentSelfLink = selfLink;
        r.count = count;
        return r;
    }

    public static <S extends ServiceDocument> QueryElementResult<S> countResult(
            long count) {
        QueryElementResult<S> r = new QueryElementResult<>();
        r.count = count;
        return r;
    }

    public static <S extends ServiceDocument> QueryElementResult<S> noResult() {
        return new QueryElementResult<>();
    }

    public static class QueryElementResult<T extends ServiceDocument> {
        private T result;
        private String documentSelfLink;
        private long count;

        public boolean hasResult() {
            return this.result != null || this.documentSelfLink != null || this.count > 0;
        }

        public T getResult() {
            return this.result;
        }

        public String getDocumentSelfLink() {
            return this.documentSelfLink;
        }

        public long getCount() {
            return this.count;
        }
    }

    @FunctionalInterface
    public static interface ResultHandler<T extends ServiceDocument> {
        public void handle(QueryElementResult<T> queryElementResult, Throwable failure);
    }
}
