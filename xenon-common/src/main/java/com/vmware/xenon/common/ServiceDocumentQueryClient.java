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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
 */
public class ServiceDocumentQueryClient<T extends ServiceDocument> {
    public static final long QUERY_RETRIVAL_RETRY_INTERVAL_MILLIS = Long.getLong(
            "xenon.query.retry.interval.millis", 500);
    public static final long DEFAULT_EXPIRATION_TIME_IN_MICROS = Long.getLong(
            "xenon.query.documents.default.expiration.millis", TimeUnit.SECONDS.toMicros(120));
    public static final Integer DEFAULT_QUERY_RESULT_LIMIT = Integer.getInteger(
            "xenon.query.documents.default.resultLimit", 50);

    private final Class<T> type;
    private final ServiceHost host;

    public ServiceDocumentQueryClient(ServiceHost host, Class<T> type) {
        this.host = host;
        this.type = type;
    }

    public static long getDefaultQueryExpiration() {
        return Utils.getNowMicrosUtc() + DEFAULT_EXPIRATION_TIME_IN_MICROS;
    }

    /**
     * Query for a document based on {@link ServiceDocument#documentSelfLink}. This is the same
     * operation of GET <code>documentSelfLink</code>. It is especially needed when a
     * {@link ServiceDocument} might not exist since using <code>GET</code> directly will timeout if
     * {@link ServiceDocument} doesn't exist (if {@link Operation#PRAGMA_HEADER} not used).
     *
     * @param documentSelfLink
     *            {@link ServiceDocument#documentSelfLink} of the document to be retrieved.
     * @param completionHandler
     *            The completion handler to be called when the result is retrieved either with the
     *            document or exception in case of error. The {@link ServiceDocument} might be null
     *            if doesn't exists.
     */
    public void queryDocument(String documentSelfLink,
            Consumer<ServiceDocumentQueryElementResult<T>> completionHandler) {
        queryUpdatedDocumentSince(-1, documentSelfLink, completionHandler);
    }

    /**
     * Query an expanded document extending {@link ServiceDocument}s that is updated or deleted
     * since given time in the past. The result will return if there is any update or delete
     * operation from provided documentSinceUpdateTimeMicros. The deleted documents will be marked
     * as with {@link ServiceDocument#documentSignature} =
     * {@link ServiceDocument#isDeletedTemplate(ServiceDocument)}. This query could be used to find
     * out if there were any updates since the time it is provided as parameter.
     *
     * @param documentSinceUpdateTimeMicros
     *            Indicating a time since the document was last updated matching the property
     *            {@link ServiceDocument#documentUpdateTimeMicros}.
     * @param completionHandler
     *            The completion handler to be called. Either the ServiceDocuments will be passed as
     *            parameter if there are changes to it or exception in case of errors.
     */
    public void queryUpdatedDocumentSince(long documentSinceUpdateTimeMicros,
            String documentSelfLink,
            Consumer<ServiceDocumentQueryElementResult<T>> completionHandler) {
        assertNotNull(documentSelfLink, "documentSelfLink");
        assertNotNull(completionHandler, "completionHandler");

        Query query = Builder.create().addKindFieldClause(this.type)
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, documentSelfLink)
                .build();
        QueryTask q = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.EXPAND_CONTENT)
                .setQuery(query).build();

        q.documentExpirationTimeMicros = getDefaultQueryExpiration();

        if (documentSinceUpdateTimeMicros != -1) {
            q.querySpec.options.add(QueryOption.INCLUDE_DELETED);
            q.querySpec.query
                    .addBooleanClause(createUpdatedSinceTimeRange(documentSinceUpdateTimeMicros));
        }

        this.host.sendRequest(Operation
                .createPost(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(q)
                .setReferer(this.host.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        completionHandler.accept(error(e));
                        return;
                    }
                    try {
                        QueryTask qtr = o.getBody(QueryTask.class);
                        if (qtr.results.documents == null
                                || qtr.results.documents.isEmpty()) {
                            completionHandler.accept(noResult());
                            return;
                        } else {
                            Collection<Object> values = qtr.results.documents.values();
                            completionHandler.accept(result(values.iterator().next(),
                                    values.size()));
                        }
                    } catch (Throwable ex) {
                        completionHandler.accept(error(ex));
                    }
                }));
    }

    /**
     * Query for a list of expanded documents extending {@link ServiceDocument}s that are updated or
     * deleted since given time in the past. The result will include both updated and deleted
     * documents. The deleted documents will be marked as with
     * {@link ServiceDocument#documentSignature} =
     * {@link ServiceDocument#isDeletedTemplate(ServiceDocument)}.
     *
     * @param documentSinceUpdateTimeMicros
     *            Indicating a time since the document was last updated matching the property
     *            {@link ServiceDocument#documentUpdateTimeMicros}.
     * @param completionHandler
     *            The completion handler to be called. Either the list of ServiceDocuments will be
     *            passed as parameter or exception in case of errors.
     */
    public void queryUpdatedSince(long documentSinceUpdateTimeMicros,
            Consumer<ServiceDocumentQueryElementResult<T>> completionHandler) {
        assertNotNull(completionHandler, "completionHandler");
        long nowMicrosUtc = Utils.getNowMicrosUtc();
        if (nowMicrosUtc < documentSinceUpdateTimeMicros) {
            throw new IllegalStateException("'documentSinceUpdateTimeMicros' must be in the past.");
        }

        Query query = Builder.create().addKindFieldClause(this.type)
                .addClause(createUpdatedSinceTimeRange(documentSinceUpdateTimeMicros)).build();
        QueryTask q = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.EXPAND_CONTENT)
                .addOption(QueryOption.INCLUDE_DELETED)
                .setResultLimit(DEFAULT_QUERY_RESULT_LIMIT)
                .setQuery(query).build();

        q.documentExpirationTimeMicros = getDefaultQueryExpiration();

        query(q, completionHandler);
    }

    /**
     * Generic Query helper method. The result could be {@link ServiceDocument}s if the query is
     * defined as expanded or just String document links.
     *
     * @param q
     *            Fully defined {@link QueryTask}
     * @param completionHandler
     *            The completion handler to be called. Either the list of ServiceDocuments will be
     *            passed as parameter or exception in case of errors.
     */
    public void query(QueryTask q, Consumer<ServiceDocumentQueryElementResult<T>> completionHandler) {
        if (q.documentExpirationTimeMicros == 0) {
            q.documentExpirationTimeMicros = getDefaultQueryExpiration();
        }
        this.host.sendRequest(Operation
                .createPost(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(q)
                .setReferer(this.host.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        completionHandler.accept(error(e));
                        return;
                    }
                    QueryTask qrt = o.getBody(QueryTask.class);
                    processQuery(qrt, completionHandler);
                }));
    }

    private void processQuery(QueryTask q,
            Consumer<ServiceDocumentQueryElementResult<T>> handler) {
        if (TaskState.isFailed(q.taskInfo)) {
            handler.accept(error(new IllegalStateException(q.taskInfo.failure.message)));
            return;
        }

        if (q.taskInfo.isDirect || TaskState.isFinished(q.taskInfo)) {
            processQueryResult(q, handler);
            return;
        }

        this.host.sendRequest(Operation.createGet(UriUtils.buildUri(this.host, q.documentSelfLink))
                .setReferer(this.host.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        handler.accept(error(e));
                        return;
                    }

                    QueryTask rsp = o.getBody(QueryTask.class);

                    if (!TaskState.isFinished(rsp.taskInfo)) {
                        this.host.log(Level.FINE, "Resource query not complete yet, retrying...");
                        this.host.schedule(() -> processQuery(rsp, handler),
                                QUERY_RETRIVAL_RETRY_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
                        return;
                    }

                    processQueryResult(rsp, handler);
                }));
    }

    private void processQueryResult(QueryTask rsp,
            Consumer<ServiceDocumentQueryElementResult<T>> handler) {
        if (rsp.querySpec.resultLimit != null && rsp.querySpec.resultLimit != Integer.MAX_VALUE) {
            // pagination results:
            getNextPageLinks(rsp.results.nextPageLink, rsp.querySpec.resultLimit, handler);
            return;
        }

        processResults(rsp, handler);
    }

    private void processResults(QueryTask rsp,
            Consumer<ServiceDocumentQueryElementResult<T>> handler) {
        List<String> links = rsp.results.documentLinks;
        if (isExpandQuery(rsp)) {
            if (rsp.results.documents.isEmpty()) {
                handler.accept(noResult());
            } else {
                long count = links.size();
                for (String documentLink : links) {
                    handler.accept(result(rsp.results.documents.get(documentLink), count));
                }
                // close the query;
                handler.accept(noResult());
            }
        } else if (isCountQuery(rsp)) {
            handler.accept(countResult(rsp.results.documentCount));
        } else {
            if (links == null || links.isEmpty()) {
                handler.accept(noResult());
            } else {
                for (String selfLink : links) {
                    handler.accept(resultLink(selfLink, links.size()));
                }
                // close the query;
                handler.accept(noResult());
            }
        }
    }

    private void getNextPageLinks(String nextPageLink, int resultLimit,
            Consumer<ServiceDocumentQueryElementResult<T>> handler) {
        try {
            if (nextPageLink == null) {
                handler.accept(noResult());
                return;
            }

            this.host.sendRequest(Operation
                    .createGet(UriUtils.buildUri(this.host, nextPageLink))
                    .setReferer(this.host.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            handler.accept(error(e));
                            return;
                        }
                        try {
                            QueryTask page = o.getBody(QueryTask.class);

                            if (isExpandQuery(page)) {
                                Collection<Object> values = page.results.documents.values();
                                for (Object json : values) {
                                    handler.accept(result(json, values.size()));
                                }
                            } else if (isCountQuery(page)) {
                                handler.accept(countResult(page.results.documentCount));
                            } else {
                                List<String> links = page.results.documentLinks;
                                for (String link : links) {
                                    handler.accept(resultLink(link, links.size()));
                                }
                            }

                            getNextPageLinks(page.results.nextPageLink, resultLimit, handler);
                        } catch (Throwable ex) {
                            handler.accept(error(ex));
                        }
                    }));
        } catch (Throwable ex) {
            handler.accept(error(ex));
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

    public static <S extends ServiceDocument> ServiceDocumentQueryElementResult<S> error(
            Throwable exception) {

        ServiceDocumentQueryElementResult<S> r = new ServiceDocumentQueryElementResult<>();
        r.exception = exception;
        return r;
    }

    public ServiceDocumentQueryElementResult<T> result(Object json, long count) {
        ServiceDocumentQueryElementResult<T> r = new ServiceDocumentQueryElementResult<>();
        r.result = Utils.fromJson(json, this.type);
        r.documentSelfLink = r.result.documentSelfLink;
        r.count = count;
        return r;
    }

    public static <S extends ServiceDocument> ServiceDocumentQueryElementResult<S> result(
            S document, long count) {

        ServiceDocumentQueryElementResult<S> r = new ServiceDocumentQueryElementResult<>();
        r.result = document;
        r.documentSelfLink = r.result.documentSelfLink;
        return r;
    }

    public static <S extends ServiceDocument> ServiceDocumentQueryElementResult<S> resultLink(
            String selfLink, long count) {

        ServiceDocumentQueryElementResult<S> r = new ServiceDocumentQueryElementResult<>();
        r.documentSelfLink = selfLink;
        r.count = count;
        return r;
    }

    public static <S extends ServiceDocument> ServiceDocumentQueryElementResult<S> countResult(
            long count) {
        ServiceDocumentQueryElementResult<S> r = new ServiceDocumentQueryElementResult<>();
        r.count = count;
        return r;
    }

    public static <S extends ServiceDocument> ServiceDocumentQueryElementResult<S> noResult() {
        return new ServiceDocumentQueryElementResult<>();
    }

    public static class ServiceDocumentQueryElementResult<T extends ServiceDocument> {
        private Throwable exception;
        private T result;
        private String documentSelfLink;
        private long count;

        public boolean hasException() {
            return this.exception != null;
        }

        public boolean hasResult() {
            return this.result != null || this.documentSelfLink != null || this.count > 0;
        }

        public Throwable getException() {
            return this.exception;
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

        public void throwRunTimeException() {
            if (this.exception instanceof RuntimeException) {
                throw (RuntimeException) this.exception;
            }
            throw new RuntimeException(this.exception);
        }
    }
}
