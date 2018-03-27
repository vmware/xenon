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

package com.vmware.xenon.services.graphdb;


import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.NamedThreadFactory;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.QueryFilterUtils;
import com.vmware.xenon.common.RoundRobinOperationQueue;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceStatUtils;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.opentracing.TracingExecutor;
import com.vmware.xenon.services.common.GuestUserService;
import com.vmware.xenon.services.common.QueryFilter;
import com.vmware.xenon.services.common.QueryFilter.QueryFilterException;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryRuntimeContext;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.SystemUserService;
import com.vmware.xenon.services.common.UpdateIndexRequest;

public class GremlinDocumentIndexService extends StatelessService {

    public static final String SELF_LINK = ServiceUriPaths.CORE + "/gremlin-index";

    public static final String PROPERTY_NAME_QUERY_THREAD_COUNT = Utils.PROPERTY_NAME_PREFIX
            + GremlinDocumentIndexService.class.getSimpleName()
            + ".QUERY_THREAD_COUNT";
    public static final int QUERY_THREAD_COUNT = Integer.getInteger(
            PROPERTY_NAME_QUERY_THREAD_COUNT,
            Utils.DEFAULT_THREAD_COUNT * 2);

    public static final String PROPERTY_NAME_UPDATE_THREAD_COUNT = Utils.PROPERTY_NAME_PREFIX
            + GremlinDocumentIndexService.class.getSimpleName()
            + ".UPDATE_THREAD_COUNT";
    public static final int UPDATE_THREAD_COUNT = Integer.getInteger(
            PROPERTY_NAME_UPDATE_THREAD_COUNT,
            Utils.DEFAULT_THREAD_COUNT / 2);

    public static final String PROPERTY_NAME_QUERY_QUEUE_DEPTH = Utils.PROPERTY_NAME_PREFIX
            + "GremlinDocumentIndexService.queryQueueDepth";
    public static final int QUERY_QUEUE_DEPTH = Integer.getInteger(
            PROPERTY_NAME_QUERY_QUEUE_DEPTH,
            10 * Service.OPERATION_QUEUE_DEFAULT_LIMIT
    );

    public static final String PROPERTY_NAME_UPDATE_QUEUE_DEPTH = Utils.PROPERTY_NAME_PREFIX
            + "GremlinDocumentIndexService.updateQueueDepth";
    public static final int UPDATE_QUEUE_DEPTH = Integer.getInteger(
            PROPERTY_NAME_UPDATE_QUEUE_DEPTH,
            10 * Service.OPERATION_QUEUE_DEFAULT_LIMIT
    );

    public static final int DEFAULT_QUERY_RESULT_LIMIT = 10000;

    public static final int DEFAULT_QUERY_PAGE_RESULT_LIMIT = 10000;

    public static final int DEFAULT_EXPIRED_DOCUMENT_SEARCH_THRESHOLD = 10000;

    private GremlinServiceDocumentDao dao;

    private static int expiredDocumentSearchThreshold = 1000;

    private static int queryResultLimit = DEFAULT_QUERY_RESULT_LIMIT;

    private static int queryPageResultLimit = DEFAULT_QUERY_PAGE_RESULT_LIMIT;

    private final Runnable queryTaskHandler = this::handleQueryRequest;

    private final Runnable updateRequestHandler = this::handleUpdateRequest;

    public static void setImplicitQueryResultLimit(int limit) {
        queryResultLimit = limit;
    }

    public static int getImplicitQueryResultLimit() {
        return queryResultLimit;
    }

    public static void setImplicitQueryProcessingPageSize(int limit) {
        queryPageResultLimit = limit;
    }

    public static int getImplicitQueryProcessingPageSize() {
        return queryPageResultLimit;
    }

    public static void setExpiredDocumentSearchThreshold(int count) {
        expiredDocumentSearchThreshold = count;
    }

    public static int getExpiredDocumentSearchThreshold() {
        return expiredDocumentSearchThreshold;
    }

    static final String PROPERTY_NAME_BINARY_SERIALIZED_STATE = "binarySerializedState";

    public static final String STAT_NAME_ACTIVE_QUERY_FILTERS = "activeQueryFilterCount";

    public static final String STAT_NAME_ACTIVE_PAGINATED_QUERIES = "activePaginatedQueryCount";

    public static final String STAT_NAME_GROUP_QUERY_COUNT = "groupQueryCount";

    public static final String STAT_NAME_QUERY_DURATION_MICROS = "queryDurationMicros";

    public static final String STAT_NAME_GROUP_QUERY_DURATION_MICROS = "groupQueryDurationMicros";

    public static final String STAT_NAME_QUERY_SINGLE_DURATION_MICROS = "querySingleDurationMicros";

    public static final String STAT_NAME_RESULT_PROCESSING_DURATION_MICROS = "resultProcessingDurationMicros";

    public static final String STAT_NAME_FORCED_UPDATE_DOCUMENT_DELETE_COUNT = "singleVersionDocumentDeleteCount";

    public static final String STAT_NAME_FIELD_COUNT_PER_DOCUMENT = "fieldCountPerDocument";

    public static final String STAT_NAME_INDEXING_DURATION_MICROS = "indexingDurationMicros";

    public static final String STAT_NAME_SEARCHER_UPDATE_COUNT = "indexSearcherUpdateCount";

    public static final String STAT_NAME_SERVICE_DELETE_COUNT = "serviceDeleteCount";

    public static final String STAT_NAME_DOCUMENT_EXPIRATION_COUNT = "expiredDocumentCount";

    public static final String STAT_NAME_DOCUMENT_KIND_QUERY_COUNT_FORMAT = "documentKindQueryCount-%s";

    public static final String STAT_NAME_NON_DOCUMENT_KIND_QUERY_COUNT = "nonDocumentKindQueryCount";

    public static final String STAT_NAME_SINGLE_QUERY_BY_FACTORY_COUNT_FORMAT = "singleQueryByFactoryCount-%s";

    public static final String STAT_NAME_PREFIX_UPDATE_QUEUE_DEPTH = "updateQueueDepth";

    public static final String STAT_NAME_FORMAT_UPDATE_QUEUE_DEPTH = STAT_NAME_PREFIX_UPDATE_QUEUE_DEPTH + "-%s";

    public static final String STAT_NAME_PREFIX_QUERY_QUEUE_DEPTH = "queryQueueDepth";

    public static final String STAT_NAME_FORMAT_QUERY_QUEUE_DEPTH = STAT_NAME_PREFIX_QUERY_QUEUE_DEPTH + "-%s";

    private static final EnumSet<AggregationType> AGGREGATION_TYPE_AVG_MAX =
            EnumSet.of(AggregationType.AVG, AggregationType.MAX);

    private static final EnumSet<AggregationType> AGGREGATION_TYPE_SUM = EnumSet.of(AggregationType.SUM);

    /**
     * Synchronization object used to coordinate index writer update
     */
    protected final Semaphore writerSync = new Semaphore(
            UPDATE_THREAD_COUNT + QUERY_THREAD_COUNT);

    protected Map<String, QueryTask> activeQueries = new ConcurrentHashMap<>();

    ExecutorService privateIndexingExecutor;

    ExecutorService privateQueryExecutor;

    private final RoundRobinOperationQueue queryQueue = new RoundRobinOperationQueue(
            "index-service-query",
            Integer.getInteger(PROPERTY_NAME_QUERY_QUEUE_DEPTH, Service.OPERATION_QUEUE_DEFAULT_LIMIT));

    private final RoundRobinOperationQueue updateQueue = new RoundRobinOperationQueue(
            "index-service-update",
            Integer.getInteger(PROPERTY_NAME_UPDATE_QUEUE_DEPTH, 10 * Service.OPERATION_QUEUE_DEFAULT_LIMIT));

    private URI uri;

    public static class DeleteQueryRuntimeContextRequest extends ServiceDocument {
        public QueryRuntimeContext context;
        static final String KIND = Utils.buildKind(DeleteQueryRuntimeContextRequest.class);
    }

    /**
     * Special GET request/response body to retrieve index service related info.
     *
     * Internal usage only mainly for backup/restore.
     */
    public static class InternalDocumentIndexInfo {
        public GremlinDocumentIndexService indexService;
        public Semaphore writerSync;
    }

    public static class MaintenanceRequest {
        static final String KIND = Utils.buildKind(MaintenanceRequest.class);
    }

    public GremlinDocumentIndexService(String connectionString) {
        super(ServiceDocument.class);
        super.toggleOption(ServiceOption.CORE, true);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        this.dao = new GremlinServiceDocumentDao(connectionString);
    }

    @Override
    public void handleStart(final Operation post) {
        super.setMaintenanceIntervalMicros(getHost().getMaintenanceIntervalMicros() * 5);
        // index service getUri() will be invoked on every load and save call for every operation,
        // so its worth caching (plus we only have a very small number of index services
        this.uri = super.getUri();

        ExecutorService es = new ThreadPoolExecutor(QUERY_THREAD_COUNT, QUERY_THREAD_COUNT,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(QUERY_QUEUE_DEPTH),
                new NamedThreadFactory(getUri() + "/queries"));
        this.privateQueryExecutor = TracingExecutor.create(es, this.getHost().getTracer());

        es = new ThreadPoolExecutor(UPDATE_THREAD_COUNT, UPDATE_THREAD_COUNT,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(UPDATE_QUEUE_DEPTH),
                new NamedThreadFactory(getUri() + "/updates"));
        this.privateIndexingExecutor = TracingExecutor.create(es, this.getHost().getTracer());

        post.complete();
    }

    private void adjustTimeSeriesStat(String name, EnumSet<AggregationType> type, double delta) {
        if (!this.hasOption(ServiceOption.INSTRUMENTATION)) {
            return;
        }

        ServiceStat dayStat = ServiceStatUtils.getOrCreateDailyTimeSeriesStat(this, name, type);
        this.adjustStat(dayStat, delta);

        ServiceStat hourStat = ServiceStatUtils.getOrCreateHourlyTimeSeriesStat(this, name, type);
        this.adjustStat(hourStat, delta);
    }

    private void setTimeSeriesHistogramStat(String name, EnumSet<AggregationType> type, double v) {
        if (!this.hasOption(ServiceOption.INSTRUMENTATION)) {
            return;
        }
        ServiceStat dayStat = ServiceStatUtils.getOrCreateDailyTimeSeriesHistogramStat(this, name, type);
        this.setStat(dayStat, v);

        ServiceStat hourStat = ServiceStatUtils.getOrCreateHourlyTimeSeriesHistogramStat(this, name, type);
        this.setStat(hourStat, v);
    }

    private void handleDeleteRuntimeContext(Operation op) throws Exception {
        op.complete();
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handleRequest(Operation op) {
        Action a = op.getAction();
        if (a == Action.PUT) {
            Operation.failActionNotSupported(op);
            return;
        }

        if (a == Action.PATCH && op.isRemote()) {
            // PATCH is reserved for in-process QueryTaskService
            Operation.failActionNotSupported(op);
            return;
        }

        try {
            if (a == Action.GET || a == Action.PATCH) {
                if (offerQueryOperation(op)) {
                    this.privateQueryExecutor.submit(this.queryTaskHandler);
                }
            } else {
                if (offerUpdateOperation(op)) {
                    this.privateIndexingExecutor.submit(this.updateRequestHandler);
                }
            }
        } catch (RejectedExecutionException e) {
            op.fail(e);
        }
    }

    private void handleQueryRequest() {
        Operation op = pollQueryOperation();
        if (op == null) {
            return;
        }

        if (op.getExpirationMicrosUtc() > 0 && op.getExpirationMicrosUtc() < Utils.getSystemNowMicrosUtc()) {
            op.fail(new RejectedExecutionException("Operation has expired"));
            return;
        }

        OperationContext originalContext = OperationContext.getOperationContext();
        try {
            this.writerSync.acquire();
            OperationContext.setFrom(op);

            switch (op.getAction()) {
            case GET:
                // handle special GET request. Internal call only. Currently from backup/restore services.
                if (!op.isRemote() && op.hasBody() && op.getBodyRaw() instanceof InternalDocumentIndexInfo) {
                    Operation.failActionNotSupported(op);
                } else {
                    handleGetImpl(op);
                }
                break;
            case PATCH:
                ServiceDocument sd = (ServiceDocument) op.getBodyRaw();
                if (sd.documentKind != null) {
                    if (sd.documentKind.equals(QueryTask.KIND)) {
                        QueryTask task = (QueryTask) sd;
                        handleQueryTaskPatch(op, task);
                        break;
                    }
                    if (sd.documentKind.equals(DeleteQueryRuntimeContextRequest.KIND)) {
                        handleDeleteRuntimeContext(op);
                        break;
                    }
                }
                Operation.failActionNotSupported(op);
                break;
            default:
                break;
            }
        } catch (Exception e) {
            op.fail(e);
        } finally {
            OperationContext.setFrom(originalContext);
            this.writerSync.release();
        }
    }

    private void handleUpdateRequest() {
        Operation op = pollUpdateOperation();
        if (op == null) {
            return;
        }

        OperationContext originalContext = OperationContext.getOperationContext();
        try {
            this.writerSync.acquire();
            OperationContext.setFrom(op);

            switch (op.getAction()) {
            case DELETE:
                handleDeleteImpl(op);
                break;
            case POST:
                Object o = op.getBodyRaw();
                if (o != null) {
                    if (o instanceof UpdateIndexRequest) {
                        updateIndex(op);
                        break;
                    }
                    if (o instanceof MaintenanceRequest) {
                        handleMaintenanceImpl(op);
                        break;
                    }
                }
                Operation.failActionNotSupported(op);
                break;
            default:
                break;
            }
        } catch (Exception e) {
            op.fail(e);
        } finally {
            OperationContext.setFrom(originalContext);
            this.writerSync.release();
        }
    }

    private void handleQueryTaskPatch(Operation op, QueryTask task) throws Exception {
        QueryTask.QuerySpecification qs = task.querySpec;

        if (qs.options.contains(QueryOption.CONTINUOUS)) {
            if (handleContinuousQueryTaskPatch(op, task, qs)) {
                return;
            }
            // intentional fall through for tasks just starting and need to execute a query
        }

        if (qs.options.contains(QueryOption.GROUP_BY)) {
            handleGroupByQueryTaskPatch(op, task);
            return;
        }

        ServiceDocumentQueryResult rsp = this.dao.queryDocuments(op, task);
        if (rsp == null) {
            rsp = new ServiceDocumentQueryResult();
            rsp.queryTimeMicros = 0L;
            rsp.documentOwner = getHost().getId();

        }
        op.setBodyNoCloning(rsp).complete();
    }

    private boolean handleContinuousQueryTaskPatch(Operation op, QueryTask task,
            QueryTask.QuerySpecification qs) throws QueryFilterException {
        switch (task.taskInfo.stage) {
        case CREATED:
            logWarning("Task %s is in invalid state: %s", task.taskInfo.stage);
            op.fail(new IllegalStateException("Stage not supported"));
            return true;
        case STARTED:
            QueryTask clonedTask = new QueryTask();
            clonedTask.documentSelfLink = task.documentSelfLink;
            clonedTask.querySpec = task.querySpec;
            clonedTask.querySpec.context.filter = QueryFilter.create(qs.query);
            clonedTask.querySpec.context.subjectLink = getSubject(op);
            this.activeQueries.put(task.documentSelfLink, clonedTask);
            adjustTimeSeriesStat(STAT_NAME_ACTIVE_QUERY_FILTERS, AGGREGATION_TYPE_SUM,
                    1);
            logInfo("Activated continuous query task: %s", task.documentSelfLink);
            break;
        case CANCELLED:
        case FAILED:
        case FINISHED:
            if (this.activeQueries.remove(task.documentSelfLink) != null) {
                adjustTimeSeriesStat(STAT_NAME_ACTIVE_QUERY_FILTERS, AGGREGATION_TYPE_SUM,
                        -1);
            }
            op.complete();
            return true;
        default:
            break;
        }
        return false;
    }

    public void handleGetImpl(Operation get) throws Exception {
        String selfLink = null;
        Long version = null;
        ServiceOption serviceOption = ServiceOption.NONE;

        EnumSet<QueryOption> options = EnumSet.noneOf(QueryOption.class);
        if (get.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK)) {
            // fast path for checking if a service exists, and loading its latest state
            serviceOption = ServiceOption.PERSISTENCE;
            // the GET operation URI is set to the service we want to load, not the self link
            // of the index service. This is only possible when the operation was directly
            // dispatched from the local host, on the index service
            selfLink = get.getUri().getPath();
            options.add(QueryOption.INCLUDE_DELETED);
        } else {
            // REST API for loading service state, given a set of URI query parameters
            Map<String, String> params = UriUtils.parseUriQueryParams(get.getUri());
            String cap = params.get(UriUtils.URI_PARAM_CAPABILITY);

            if (cap != null) {
                serviceOption = ServiceOption.valueOf(cap);
            }

            if (serviceOption == ServiceOption.IMMUTABLE) {
                options.add(QueryOption.INCLUDE_ALL_VERSIONS);
                serviceOption = ServiceOption.PERSISTENCE;
            }

            if (params.containsKey(UriUtils.URI_PARAM_INCLUDE_DELETED)) {
                options.add(QueryOption.INCLUDE_DELETED);
            }

            if (params.containsKey(ServiceDocument.FIELD_NAME_VERSION)) {
                version = Long.parseLong(params.get(ServiceDocument.FIELD_NAME_VERSION));
            }

            selfLink = params.get(ServiceDocument.FIELD_NAME_SELF_LINK);
            String fieldToExpand = params.get(UriUtils.URI_PARAM_ODATA_EXPAND);
            if (fieldToExpand == null) {
                fieldToExpand = params.get(UriUtils.URI_PARAM_ODATA_EXPAND_NO_DOLLAR_SIGN);
            }
            if (fieldToExpand != null
                    && fieldToExpand
                    .equals(ServiceDocumentQueryResult.FIELD_NAME_DOCUMENT_LINKS)) {
                options.add(QueryOption.EXPAND_CONTENT);
            }
        }

        if (selfLink == null) {
            get.fail(new IllegalArgumentException(
                    ServiceDocument.FIELD_NAME_SELF_LINK + " query parameter is required"));
            return;
        }

        if (!selfLink.endsWith(UriUtils.URI_WILDCARD_CHAR)) {

            // Enforce auth check for the returning document for remote GET requests.
            // This is mainly for the direct client requests to the index-service such as
            // "/core/document-index?documentSelfLink=...".
            // Some other core services also perform remote GET (e.g.: NodeSelectorSynchronizationService),
            // but they populate appropriate auth context such as system-user.
            // For non-wildcard selfLink request, auth check is performed as part of queryIndex().
            if (get.isRemote() && getHost().isAuthorizationEnabled()) {
                get.nestCompletion((op, ex) -> {
                    if (ex != null) {
                        get.fail(ex);
                        return;
                    }

                    if (get.getAuthorizationContext().isSystemUser() || !op.hasBody()) {
                        // when there is no matching document, we cannot evaluate the auth, thus simply complete.
                        get.complete();
                        return;
                    }

                    // evaluate whether the matched document is authorized for the user
                    QueryFilter queryFilter = get.getAuthorizationContext().getResourceQueryFilter(Action.GET);
                    if (queryFilter == null) {
                        // do not match anything
                        queryFilter = QueryFilter.FALSE;
                    }
                    // This completion handler is called right after it retrieved the document from lucene and
                    // deserialized it to its state type.
                    // Since calling "op.getBody(ServiceDocument.class)" changes(down cast) the actual document object
                    // to an instance of ServiceDocument, it will lose the additional data which might be required in
                    // authorization filters; Therefore, here uses "op.getBodyRaw()" and just cast to ServiceDocument
                    // which doesn't convert the document object.
                    ServiceDocument doc = (ServiceDocument) op.getBodyRaw();
                    if (!QueryFilterUtils.evaluate(queryFilter, doc, getHost())) {
                        get.fail(Operation.STATUS_CODE_FORBIDDEN);
                        return;
                    }
                    get.complete();
                });
            }

            // Most basic query is retrieving latest document at latest version for a specific link
            queryIndexSingle(selfLink, get, version);
            return;
        }

        // Self link prefix query, returns all self links with the same prefix. A GET on a
        // factory translates to this query.
        selfLink = selfLink.substring(0, selfLink.length() - 1);
        ServiceDocumentQueryResult rsp = this.dao.queryBySelfLinkPrefix(get, selfLink, options);
        if (rsp != null) {
            rsp.documentOwner = getHost().getId();
            get.setBodyNoCloning(rsp).complete();
            return;
        }

        if (serviceOption == ServiceOption.PERSISTENCE) {
            // specific index requested but no results, return empty response
            rsp = new ServiceDocumentQueryResult();
            rsp.documentLinks = new ArrayList<>();
            rsp.documentOwner = getHost().getId();
            get.setBodyNoCloning(rsp).complete();
            return;
        }

        // no results in the index, search the service host started services
        queryServiceHost(selfLink + UriUtils.URI_WILDCARD_CHAR, options, get);
    }

    /**
     * retrieves the next available operation given the fairness scheme
     */
    private Operation pollQueryOperation() {
        return this.queryQueue.poll();
    }

    private Operation pollUpdateOperation() {
        return this.updateQueue.poll();
    }

    /**
     * Queues operation in a multi-queue that uses the subject as the key per queue
     */
    private boolean offerQueryOperation(Operation op) {
        String subject = getSubject(op);
        return this.queryQueue.offer(subject, op);
    }

    private boolean offerUpdateOperation(Operation op) {
        String subject = getSubject(op);
        return this.updateQueue.offer(subject, op);
    }

    private String getSubject(Operation op) {

        if (op.getAuthorizationContext() != null
                && op.getAuthorizationContext().isSystemUser()) {
            return SystemUserService.SELF_LINK;
        }

        if (getHost().isAuthorizationEnabled()) {
            return op.getAuthorizationContext().getClaims().getSubject();
        }

        return GuestUserService.SELF_LINK;
    }

    private void queryIndexSingle(String selfLink, Operation op, Long version)
            throws Exception {
        long startNanos = System.nanoTime();
        ServiceDocument doc = this.dao.loadDocument(selfLink, null);
        long durationNanos = System.nanoTime() - startNanos;
        setTimeSeriesHistogramStat(STAT_NAME_QUERY_SINGLE_DURATION_MICROS,
                AGGREGATION_TYPE_AVG_MAX, TimeUnit.NANOSECONDS.toMicros(durationNanos));

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            String factoryLink = UriUtils.getParentPath(selfLink);
            if (factoryLink != null) {
                String statKey = String.format(STAT_NAME_SINGLE_QUERY_BY_FACTORY_COUNT_FORMAT, factoryLink);
                adjustStat(statKey, 1);
            }
        }
        if (doc == null) {
            op.complete();
            return;
        }

        boolean hasExpired = false;

        Long expiration = doc.documentExpirationTimeMicros;
        if (expiration != null) {
            hasExpired = expiration <= Utils.getSystemNowMicrosUtc();
        }

        if (hasExpired) {
            op.complete();
            return;
        }

        op.setBodyNoCloning(doc).complete();
    }

    private void queryServiceHost(String selfLink, EnumSet<QueryOption> options, Operation op) {
        if (options.contains(QueryOption.EXPAND_CONTENT)) {
            // the index writers had no results, ask the host a simple prefix query
            // for the services, and do a manual expand
            op.nestCompletion(o -> {
                expandLinks(o, op);
            });
        }
        getHost().queryServiceUris(selfLink, op);
    }

    private void handleGroupByQueryTaskPatch(Operation op, QueryTask task) throws IOException {
        op.setBodyNoCloning(null).complete();
    }

    private void expandLinks(Operation o, Operation get) {
        ServiceDocumentQueryResult r = o.getBody(ServiceDocumentQueryResult.class);
        if (r.documentLinks == null || r.documentLinks.isEmpty()) {
            get.setBodyNoCloning(r).complete();
            return;
        }

        r.documents = new HashMap<>();

        AtomicInteger i = new AtomicInteger(r.documentLinks.size());
        CompletionHandler c = (op, e) -> {
            try {
                if (e != null) {
                    logWarning("failure expanding %s: %s", op.getUri().getPath(), e.getMessage());
                    return;
                }
                synchronized (r.documents) {
                    r.documents.put(op.getUri().getPath(), op.getBodyRaw());
                }
            } finally {
                if (i.decrementAndGet() == 0) {
                    get.setBodyNoCloning(r).complete();
                }
            }
        };

        for (String selfLink : r.documentLinks) {
            sendRequest(Operation.createGet(this, selfLink)
                    .setCompletion(c));
        }
    }

    public void handleDeleteImpl(Operation delete) throws Exception {
        setProcessingStage(ProcessingStage.STOPPED);

        this.privateIndexingExecutor.shutdown();
        this.privateQueryExecutor.shutdown();
        this.getHost().stopService(this);
        delete.complete();
    }

    protected void updateIndex(Operation updateOp) throws Exception {
        UpdateIndexRequest r = updateOp.getBody(UpdateIndexRequest.class);
        ServiceDocument s = r.document;
        ServiceDocumentDescription desc = r.description;

        if (updateOp.isRemote()) {
            updateOp.fail(new IllegalStateException("Remote requests not allowed"));
            return;
        }

        if (s == null) {
            updateOp.fail(new IllegalArgumentException("document is required"));
            return;
        }

        String link = s.documentSelfLink;
        if (link == null) {
            updateOp.fail(new IllegalArgumentException(
                    "documentSelfLink is required"));
            return;
        }

        if (s.documentUpdateAction == null) {
            updateOp.fail(new IllegalArgumentException(
                    "documentUpdateAction is required"));
            return;
        }

        if (desc == null) {
            updateOp.fail(new IllegalArgumentException("description is required"));
            return;
        }

        s.documentDescription = null;

        this.dao.saveDocument(s, desc);

        updateOp.setBodyNoCloning(null).complete();
        applyActiveQueries(updateOp, s, desc);
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public void handleMaintenance(Operation post) {
        Operation maintenanceOp = Operation
                .createPost(this.getUri())
                .setBodyNoCloning(new MaintenanceRequest())
                .setCompletion((o, ex) -> {
                    if (ex != null) {
                        post.fail(ex);
                        return;
                    }
                    post.complete();
                });

        setAuthorizationContext(maintenanceOp, getSystemAuthorizationContext());
        handleRequest(maintenanceOp);
    }

    private void handleMaintenanceImpl(Operation op) throws Exception {
        try {
            this.dao.applyDocumentExpiration(expiredDocumentSearchThreshold);
            op.complete();
        } catch (Exception e) {
            if (this.getHost().isStopping()) {
                op.fail(new CancellationException("Host is stopping"));
                return;
            }
            op.fail(e);
        }
    }

    private void applyActiveQueries(Operation op, ServiceDocument latestState,
            ServiceDocumentDescription desc) {
        if (this.activeQueries.isEmpty()) {
            return;
        }

        if (op.getAction() == Action.DELETE) {
            // This code path is reached for document expiration, but the last update action for
            // expired documents is usually a PATCH or PUT. Dummy up a document body with a last
            // update action of DELETE for the purpose of providing notifications.
            latestState = Utils.clone(latestState);
            latestState.documentUpdateAction = Action.DELETE.name();
        }

        // set current context from the operation so all active query task notifications carry the
        // same context as the operation that updated the index
        OperationContext.setFrom(op);

        // TODO Optimize. We currently traverse each query independently. We can collapse the queries
        // and evaluate clauses keeping track which clauses applied, then skip any queries accordingly.

        for (Entry<String, QueryTask> taskEntry : this.activeQueries.entrySet()) {
            if (getHost().isStopping()) {
                continue;
            }

            QueryTask activeTask = taskEntry.getValue();
            QueryFilter filter = activeTask.querySpec.context.filter;
            if (desc == null) {
                if (!QueryFilterUtils.evaluate(filter, latestState, getHost())) {
                    continue;
                }
            } else {
                if (!filter.evaluate(latestState, desc)) {
                    continue;
                }
            }

            QueryTask patchBody = new QueryTask();
            patchBody.taskInfo.stage = TaskStage.STARTED;
            patchBody.querySpec = null;
            patchBody.results = new ServiceDocumentQueryResult();
            patchBody.results.documentLinks.add(latestState.documentSelfLink);
            if (activeTask.querySpec.options.contains(QueryOption.EXPAND_CONTENT) ||
                    activeTask.querySpec.options.contains(QueryOption.COUNT)) {
                patchBody.results.documents = new HashMap<>();
                patchBody.results.documents.put(latestState.documentSelfLink, latestState);
            }

            // Send PATCH to continuous query task with document that passed the query filter.
            // Any subscribers will get notified with the body containing just this document
            Operation patchOperation = Operation.createPatch(this, activeTask.documentSelfLink)
                    .setBodyNoCloning(
                            patchBody);
            // Set the authorization context to the user who created the continous query.
            OperationContext currentContext = OperationContext.getOperationContext();
            if (activeTask.querySpec.context.subjectLink != null) {
                setAuthorizationContext(patchOperation,
                        getAuthorizationContextForSubject(
                                activeTask.querySpec.context.subjectLink));
            }
            sendRequest(patchOperation);
            OperationContext.restoreOperationContext(currentContext);
        }
    }

}
