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

package com.vmware.xenon.services.common;

import java.lang.reflect.Field;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import static com.vmware.xenon.services.common.PostgresQueryConverter.escapeSqlLike;
import static com.vmware.xenon.services.common.PostgresQueryConverter.escapeSqlString;

import com.vmware.xenon.common.NamedThreadFactory;
import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.QueryFilterUtils;
import com.vmware.xenon.common.ReflectionUtils;
import com.vmware.xenon.common.RoundRobinOperationQueue;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.DocumentIndexingOption;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceStatUtils;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.config.XenonConfiguration;
import com.vmware.xenon.common.opentracing.TracingExecutor;
import com.vmware.xenon.common.serialization.GsonSerializers;
import com.vmware.xenon.services.common.QueryFilter.QueryFilterException;
import com.vmware.xenon.services.common.QueryPageService.PostgresQueryPage;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryRuntimeContext;

public class PostgresDocumentIndexService extends StatelessService {

    public static final String SELF_LINK = ServiceUriPaths.CORE_DOCUMENT_INDEX;

    public static final int QUERY_THREAD_COUNT = XenonConfiguration.integer(
            PostgresDocumentIndexService.class,
            "QUERY_THREAD_COUNT",
            Utils.DEFAULT_THREAD_COUNT * 2
    );

    public static final int UPDATE_THREAD_COUNT = XenonConfiguration.integer(
            PostgresDocumentIndexService.class,
            "UPDATE_THREAD_COUNT",
            Utils.DEFAULT_THREAD_COUNT / 2
    );

    public static final int QUERY_QUEUE_DEPTH = XenonConfiguration.integer(
            PostgresDocumentIndexService.class,
            "queryQueueDepth",
            10 * Service.OPERATION_QUEUE_DEFAULT_LIMIT
    );

    public static final int UPDATE_QUEUE_DEPTH = XenonConfiguration.integer(
            PostgresDocumentIndexService.class,
            "updateQueueDepth",
            10 * Service.OPERATION_QUEUE_DEFAULT_LIMIT
    );

    public static final int DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH = 10000;

    public static final int MIN_QUERY_RESULT_LIMIT = 1000;

    public static final int DEFAULT_QUERY_RESULT_LIMIT = 10000;

    public static final int DEFAULT_QUERY_PAGE_RESULT_LIMIT = 10000;

    public static final int DEFAULT_EXPIRED_DOCUMENT_SEARCH_THRESHOLD = 10000;

    public static final int DEFAULT_METADATA_UPDATE_MAX_QUEUE_DEPTH = 10000;

    public static final long DEFAULT_PAGINATED_SEARCHER_EXPIRATION_DELAY = TimeUnit.SECONDS.toMicros(1);

    private static final int QUERY_EXECUTOR_WORK_QUEUE_CAPACITY = XenonConfiguration.integer(
            PostgresDocumentIndexService.class,
            "queryExecutorWorkQueueCapacity",
            QUERY_QUEUE_DEPTH
    );

    private static final int UPDATE_EXECUTOR_WORK_QUEUE_CAPACITY = XenonConfiguration.integer(
            PostgresDocumentIndexService.class,
            "updateExecutorWorkQueueCapacity",
            UPDATE_QUEUE_DEPTH
    );

    // private static final String DOCUMENTS_WITHOUT_RESULTS = "DocumentsWithoutResults";

    private static int expiredDocumentSearchThreshold = 1000;

    private static int indexFileCountThresholdForWriterRefresh = DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH;

    private static int versionRetentionBulkCleanupThreshold = 10000;

    private static int versionRetentionServiceThreshold = 100;

    private static int queryResultLimit = DEFAULT_QUERY_RESULT_LIMIT;

    private static int queryPageResultLimit = DEFAULT_QUERY_PAGE_RESULT_LIMIT;

    private static long searcherRefreshIntervalMicros = 0;

    private static int metadataUpdateMaxQueueDepth = DEFAULT_METADATA_UPDATE_MAX_QUEUE_DEPTH;

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

    public static void setIndexFileCountThresholdForWriterRefresh(int count) {
        indexFileCountThresholdForWriterRefresh = count;
    }

    public static int getIndexFileCountThresholdForWriterRefresh() {
        return indexFileCountThresholdForWriterRefresh;
    }

    public static void setExpiredDocumentSearchThreshold(int count) {
        expiredDocumentSearchThreshold = count;
    }

    public static int getExpiredDocumentSearchThreshold() {
        return expiredDocumentSearchThreshold;
    }

    public static void setVersionRetentionBulkCleanupThreshold(int count) {
        versionRetentionBulkCleanupThreshold = count;
    }

    public static int getVersionRetentionBulkCleanupThreshold() {
        return versionRetentionBulkCleanupThreshold;
    }

    public static void setVersionRetentionServiceThreshold(int count) {
        versionRetentionServiceThreshold = count;
    }

    public static int getVersionRetentionServiceThreshold() {
        return versionRetentionServiceThreshold;
    }

    public static long getSearcherRefreshIntervalMicros() {
        return searcherRefreshIntervalMicros;
    }

    public static void setSearcherRefreshIntervalMicros(long interval) {
        searcherRefreshIntervalMicros = interval;
    }

    public static void setMetadataUpdateMaxQueueDepth(int depth) {
        metadataUpdateMaxQueueDepth = depth;
    }

    public static int getMetadataUpdateMaxQueueDepth() {
        return metadataUpdateMaxQueueDepth;
    }

    static final String LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE = "binarySerializedState";

    static final String LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE = "jsonSerializedState";

    public static final String STAT_NAME_ACTIVE_QUERY_FILTERS = "activeQueryFilterCount";

    public static final String STAT_NAME_ACTIVE_PAGINATED_QUERIES = "activePaginatedQueryCount";

    public static final String STAT_NAME_COMMIT_COUNT = "commitCount";

    public static final String STAT_NAME_INDEX_LOAD_RETRY_COUNT = "indexLoadRetryCount";

    public static final String STAT_NAME_COMMIT_DURATION_MICROS = "commitDurationMicros";

    public static final String STAT_NAME_GROUP_QUERY_COUNT = "groupQueryCount";

    public static final String STAT_NAME_QUERY_DURATION_MICROS = "queryDurationMicros";

    public static final String STAT_NAME_GROUP_QUERY_DURATION_MICROS = "groupQueryDurationMicros";

    public static final String STAT_NAME_QUERY_SINGLE_DURATION_MICROS = "querySingleDurationMicros";

    public static final String STAT_NAME_QUERY_ALL_VERSIONS_DURATION_MICROS = "queryAllVersionsDurationMicros";

    public static final String STAT_NAME_RESULT_PROCESSING_DURATION_MICROS = "resultProcessingDurationMicros";

    public static final String STAT_NAME_INDEXED_FIELD_COUNT = "indexedFieldCount";

    public static final String STAT_NAME_INDEXED_DOCUMENT_COUNT = "indexedDocumentCount";

    public static final String STAT_NAME_FORCED_UPDATE_DOCUMENT_DELETE_COUNT = "singleVersionDocumentDeleteCount";

    public static final String STAT_NAME_FIELD_COUNT_PER_DOCUMENT = "fieldCountPerDocument";

    public static final String STAT_NAME_INDEXING_DURATION_MICROS = "indexingDurationMicros";

    public static final String STAT_NAME_SEARCHER_UPDATE_COUNT = "indexSearcherUpdateCount";

    public static final String STAT_NAME_SEARCHER_REUSE_BY_DOCUMENT_KIND_COUNT = "indexSearcherReuseByDocumentKindCount";

    public static final String STAT_NAME_PAGINATED_SEARCHER_UPDATE_COUNT = "paginatedIndexSearcherUpdateCount";

    public static final String STAT_NAME_PAGINATED_SEARCHER_FORCE_DELETION_COUNT = "paginatedIndexSearcherForceDeletionCount";

    public static final String STAT_NAME_WRITER_ALREADY_CLOSED_EXCEPTION_COUNT = "indexWriterAlreadyClosedFailureCount";

    public static final String STAT_NAME_READER_ALREADY_CLOSED_EXCEPTION_COUNT = "indexReaderAlreadyClosedFailureCount";

    public static final String STAT_NAME_SERVICE_DELETE_COUNT = "serviceDeleteCount";

    public static final String STAT_NAME_DOCUMENT_EXPIRATION_COUNT = "expiredDocumentCount";

    public static final String STAT_NAME_DOCUMENT_EXPIRATION_FORCED_MAINTENANCE_COUNT = "expiredDocumentForcedMaintenanceCount";

    public static final String STAT_NAME_METADATA_INDEXING_UPDATE_COUNT = "metadataIndexingUpdateCount";

    public static final String STAT_NAME_VERSION_CACHE_LOOKUP_COUNT = "versionCacheLookupCount";

    public static final String STAT_NAME_VERSION_CACHE_MISS_COUNT = "versionCacheMissCount";

    public static final String STAT_NAME_VERSION_CACHE_ENTRY_COUNT = "versionCacheEntryCount";

    public static final String STAT_NAME_MAINTENANCE_SEARCHER_REFRESH_DURATION_MICROS =
            "maintenanceSearcherRefreshDurationMicros";

    public static final String STAT_NAME_MAINTENANCE_DOCUMENT_EXPIRATION_DURATION_MICROS =
            "maintenanceDocumentExpirationDurationMicros";

    public static final String STAT_NAME_MAINTENANCE_VERSION_RETENTION_DURATION_MICROS =
            "maintenanceVersionRetentionDurationMicros";

    public static final String STAT_NAME_MAINTENANCE_METADATA_INDEXING_DURATION_MICROS =
            "maintenanceMetadataIndexingDurationMicros";

    public static final String STAT_NAME_DOCUMENT_KIND_QUERY_COUNT_FORMAT = "documentKindQueryCount-%s";

    public static final String STAT_NAME_NON_DOCUMENT_KIND_QUERY_COUNT = "nonDocumentKindQueryCount";

    public static final String STAT_NAME_SINGLE_QUERY_BY_FACTORY_COUNT_FORMAT = "singleQueryByFactoryCount-%s";

    public static final String STAT_NAME_PREFIX_UPDATE_QUEUE_DEPTH = "updateQueueDepth";

    public static final String STAT_NAME_FORMAT_UPDATE_QUEUE_DEPTH = STAT_NAME_PREFIX_UPDATE_QUEUE_DEPTH + "-%s";

    public static final String STAT_NAME_PREFIX_QUERY_QUEUE_DEPTH = "queryQueueDepth";

    public static final String STAT_NAME_FORMAT_QUERY_QUEUE_DEPTH = STAT_NAME_PREFIX_QUERY_QUEUE_DEPTH + "-%s";

    private static final String STAT_NAME_MAINTENANCE_MEMORY_LIMIT_DURATION_MICROS =
            "maintenanceMemoryLimitDurationMicros";

    private static final String STAT_NAME_MAINTENANCE_FILE_LIMIT_REFRESH_DURATION_MICROS =
            "maintenanceFileLimitRefreshDurationMicros";

    static final String STAT_NAME_VERSION_RETENTION_SERVICE_COUNT = "versionRetentionServiceCount";

    static final String STAT_NAME_ITERATIONS_PER_QUERY = "iterationsPerQuery";

    private static final EnumSet<AggregationType> AGGREGATION_TYPE_AVG_MAX =
            EnumSet.of(AggregationType.AVG, AggregationType.MAX);

    private static final EnumSet<AggregationType> AGGREGATION_TYPE_SUM = EnumSet.of(AggregationType.SUM);

    /**
     * Synchronization object used to coordinate index searcher refresh
     */
    protected final Object searchSync = new Object();

    /**
     * Synchronization object used to coordinate document metadata updates.
     */
    private final Object metadataUpdateSync = new Object();

    /**
     * Synchronization object used to coordinate index writer update
     */
    protected final Semaphore writerSync = new Semaphore(
            UPDATE_THREAD_COUNT + QUERY_THREAD_COUNT);

    private ThreadLocal<PostgresIndexDocumentHelper> indexDocumentHelper = ThreadLocal
            .withInitial(PostgresIndexDocumentHelper::new);

    protected Map<String, QueryTask> activeQueries = new ConcurrentHashMap<>();

    /**
     * Time when memory pressure removed {@link #updatesPerLink} entries.
     */
    // private long serviceRemovalDetectedTimeMicros;

    private final Map<String, DocumentUpdateInfo> updatesPerLink = new HashMap<>();
    private final Map<String, Long> liveVersionsPerLink = new HashMap<>();
    // private final Map<String, Long> immutableParentLinks = new HashMap<>();
    // private final Map<String, Long> documentKindUpdateInfo = new HashMap<>();

    private final SortedSet<MetadataUpdateInfo> metadataUpdates =
            new TreeSet<>(Comparator.comparingLong((info) -> info.updateTimeMicros));
    private final Map<String, MetadataUpdateInfo> metadataUpdatesPerLink = new HashMap<>();

    // memory pressure threshold in bytes
    long updateMapMemoryLimit;

    ExecutorService privateIndexingExecutor;

    ExecutorService privateQueryExecutor;

    private Set<String> fieldsToLoadIndexingIdLookup;
    private Set<String> fieldToLoadVersionLookup;
    private Set<String> fieldsToLoadNoExpand;
    private Set<String> fieldsToLoadWithExpand;

    private final RoundRobinOperationQueue queryQueue = new RoundRobinOperationQueue(
            "index-service-query", QUERY_QUEUE_DEPTH);

    private final RoundRobinOperationQueue updateQueue = new RoundRobinOperationQueue(
            "index-service-update", UPDATE_QUEUE_DEPTH);

    private URI uri;

    private Properties connectionProperties;
    private String connectionUrl;

    public static class MetadataUpdateInfo {
        public String selfLink;
        public String kind;
        public long updateTimeMicros;
    }

    public static class DocumentUpdateInfo {
        public long updateTimeMicros;
        public long version;
    }

    public static class DeleteQueryRuntimeContextRequest extends ServiceDocument {
        public QueryRuntimeContext context;
        static final String KIND = Utils.buildKind(DeleteQueryRuntimeContextRequest.class);
    }

    /**
     * Special GET request/response body to retrieve lucene related info.
     *
     * Internal usage only mainly for backup/restore.
     */
    public static class InternalDocumentIndexInfo {
        public PostgresDocumentIndexService luceneIndexService;
        public Semaphore writerSync;
    }

    public static class MaintenanceRequest {
        static final String KIND = Utils.buildKind(MaintenanceRequest.class);
    }

    public PostgresDocumentIndexService(String url, Properties props) {
        super(ServiceDocument.class);
        super.toggleOption(ServiceOption.CORE, true);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);

        this.connectionUrl = url;
        this.connectionProperties = props;
    }

    Connection getConnection() {
        try {
            return DriverManager.getConnection(this.connectionUrl, this.connectionProperties);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleStart(final Operation post) {
        super.setMaintenanceIntervalMicros(getHost().getMaintenanceIntervalMicros() * 5);
        // index service getUri() will be invoked on every load and save call for every operation,
        // so its worth caching (plus we only have a very small number of index services
        this.uri = super.getUri();

        ExecutorService es = new ThreadPoolExecutor(QUERY_THREAD_COUNT, QUERY_THREAD_COUNT,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(QUERY_EXECUTOR_WORK_QUEUE_CAPACITY),
                new NamedThreadFactory(getUri() + "/queries"));
        this.privateQueryExecutor = TracingExecutor.create(es, this.getHost().getTracer());

        es = new ThreadPoolExecutor(QUERY_THREAD_COUNT, QUERY_THREAD_COUNT,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(UPDATE_EXECUTOR_WORK_QUEUE_CAPACITY),
                new NamedThreadFactory(getUri() + "/updates"));
        this.privateIndexingExecutor = TracingExecutor.create(es, this.getHost().getTracer());

        initializeInstance();

        post.complete();
    }

    private void initializeInstance() {
        this.liveVersionsPerLink.clear();
        this.updatesPerLink.clear();

        this.fieldsToLoadIndexingIdLookup = new HashSet<>();
        this.fieldsToLoadIndexingIdLookup.add(ServiceDocument.FIELD_NAME_VERSION);
        this.fieldsToLoadIndexingIdLookup.add(ServiceDocument.FIELD_NAME_UPDATE_ACTION);
        this.fieldsToLoadIndexingIdLookup.add(PostgresIndexDocumentHelper.FIELD_NAME_INDEXING_ID);
        this.fieldsToLoadIndexingIdLookup.add(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS);
        this.fieldsToLoadIndexingIdLookup.add(PostgresIndexDocumentHelper.FIELD_NAME_INDEXING_METADATA_VALUE_TOMBSTONE_TIME);

        this.fieldToLoadVersionLookup = new HashSet<>();
        this.fieldToLoadVersionLookup.add(ServiceDocument.FIELD_NAME_VERSION);
        this.fieldToLoadVersionLookup.add(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS);

        this.fieldsToLoadNoExpand = new HashSet<>();
        this.fieldsToLoadNoExpand.add(ServiceDocument.FIELD_NAME_SELF_LINK);
        this.fieldsToLoadNoExpand.add(ServiceDocument.FIELD_NAME_VERSION);
        this.fieldsToLoadNoExpand.add(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS);
        this.fieldsToLoadNoExpand.add(ServiceDocument.FIELD_NAME_UPDATE_ACTION);

        this.fieldsToLoadWithExpand = new HashSet<>(this.fieldsToLoadNoExpand);
        this.fieldsToLoadWithExpand.add(ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS);
        this.fieldsToLoadWithExpand.add(LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE);
    }


    private void setTimeSeriesStat(String name, EnumSet<AggregationType> type, double v) {
        if (!this.hasOption(ServiceOption.INSTRUMENTATION)) {
            return;
        }
        ServiceStat dayStat = ServiceStatUtils.getOrCreateDailyTimeSeriesStat(this, name, type);
        this.setStat(dayStat, v);

        ServiceStat hourStat = ServiceStatUtils.getOrCreateHourlyTimeSeriesStat(this, name, type);
        this.setStat(hourStat, v);
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

    private String getQueryStatName(QueryTask.Query query) {
        if (query.term != null) {
            if (query.term.propertyName.equals(ServiceDocument.FIELD_NAME_KIND)) {
                return String.format(STAT_NAME_DOCUMENT_KIND_QUERY_COUNT_FORMAT, query.term.matchValue);
            }
            return STAT_NAME_NON_DOCUMENT_KIND_QUERY_COUNT;
        }

        StringBuilder kindSb = new StringBuilder();
        for (QueryTask.Query clause : query.booleanClauses) {
            if (clause.term == null || clause.term.propertyName == null || clause.term.matchValue == null) {
                continue;
            }
            if (clause.term.propertyName.equals(ServiceDocument.FIELD_NAME_KIND)) {
                if (kindSb.length() > 0) {
                    kindSb.append(", ");
                }
                kindSb.append(clause.term.matchValue);
            }
        }

        if (kindSb.length() > 0) {
            return String.format(STAT_NAME_DOCUMENT_KIND_QUERY_COUNT_FORMAT, kindSb.toString());
        }

        return STAT_NAME_NON_DOCUMENT_KIND_QUERY_COUNT;
    }

    private void handleDeleteRuntimeContext(Operation op) throws Exception {
        op.complete();

        adjustTimeSeriesStat(STAT_NAME_PAGINATED_SEARCHER_FORCE_DELETION_COUNT, AGGREGATION_TYPE_SUM, 1);
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
        OperationContext originalContext = OperationContext.getOperationContext();
        Operation op = pollQueryOperation();
        try {
            this.writerSync.acquire();
            while (op != null) {
                if (op.getExpirationMicrosUtc() > 0 && op.getExpirationMicrosUtc() < Utils.getSystemNowMicrosUtc()) {
                    op.fail(new RejectedExecutionException("Operation has expired"));
                    return;
                }

                OperationContext.setFrom(op);
                switch (op.getAction()) {
                case GET:
                    // handle special GET request. Internal call only. Currently from backup/restore services.
                    if (!op.isRemote() && op.hasBody() && op.getBodyRaw() instanceof InternalDocumentIndexInfo) {
                        InternalDocumentIndexInfo response = new InternalDocumentIndexInfo();
                        response.luceneIndexService = this;
                        response.writerSync = this.writerSync;
                        op.setBodyNoCloning(response).complete();
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
                op = pollQueryOperation();
            }
        } catch (Exception e) {
            checkFailureAndRecover(e);
            if (op != null) {
                op.fail(e);
            }
        } finally {
            OperationContext.setFrom(originalContext);
            this.writerSync.release();
        }
    }

    private void handleUpdateRequest() {
        OperationContext originalContext = OperationContext.getOperationContext();
        Operation op = pollUpdateOperation();
        try {
            this.writerSync.acquire();
            while (op != null) {
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
                op = pollUpdateOperation();
            }
        } catch (Exception e) {
            checkFailureAndRecover(e);
            if (op != null) {
                op.fail(e);
            }
        } finally {
            OperationContext.setFrom(originalContext);
            this.writerSync.release();
        }
    }

    private void handleQueryTaskPatch(Operation op, QueryTask task) throws Exception {
        QuerySpecification qs = task.querySpec;

        String luceneQuery = (String) qs.context.nativeQuery;
        String luceneSort = (String) qs.context.nativeSort;

        if (luceneSort == null && task.querySpec.options != null
                && task.querySpec.options.contains(QueryOption.SORT)) {
            luceneSort = PostgresQueryConverter.convertToPostgresSort(task.querySpec, false);
            task.querySpec.context.nativeSort = luceneSort;
        }

        if (luceneQuery == null) {
            luceneQuery = PostgresQueryConverter.convert(task.querySpec.query, qs.context);
            /*if (qs.options.contains(QueryOption.TIME_SNAPSHOT)) {
                Query latestDocumentClause = LongPoint.newRangeQuery(
                        ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS, 0,
                        qs.timeSnapshotBoundaryMicros);
                luceneQuery = new BooleanQuery.Builder()
                        .add(latestDocumentClause, Occur.MUST)
                        .add(luceneQuery, Occur.FILTER).build();
            }*/
            qs.context.nativeQuery = luceneQuery;
        }


        if (qs.options.contains(QueryOption.CONTINUOUS) ||
                qs.options.contains(QueryOption.CONTINUOUS_STOP_MATCH)) {
            if (handleContinuousQueryTaskPatch(op, task, qs)) {
                return;
            }
            // intentional fall through for tasks just starting and need to execute a query
        }

        if (qs.options.contains(QueryOption.GROUP_BY)) {
            handleGroupByQueryTaskPatch(op, task);
            return;
        }

        PostgresQueryPage lucenePage = (PostgresQueryPage) qs.context.nativePage;
        ServiceDocumentQueryResult rsp = new ServiceDocumentQueryResult();

        /*
        if (s == null && qs.resultLimit != null && qs.resultLimit > 0
                && qs.resultLimit != Integer.MAX_VALUE
                && !qs.options.contains(QueryOption.TOP_RESULTS)) {
            // this is a paginated query. If this is the start of the query, create a dedicated searcher
            // for this query and all its pages. It will be expired when the query task itself expires.
            // Since expiration of QueryPageService and index-searcher uses different mechanism, to guarantee
            // that index-searcher still exists when QueryPageService expired, add some delay for searcher
            // expiration time.
            Set<String> documentKind = qs.context.kindScope;
            long expiration = task.documentExpirationTimeMicros + DEFAULT_PAGINATED_SEARCHER_EXPIRATION_DELAY;
            s = createOrUpdatePaginatedQuerySearcher(expiration, this.writer, documentKind, qs.options);
        }*/

        if (!queryIndex(op, null, qs.options, luceneQuery, lucenePage,
                qs.resultLimit,
                task.documentExpirationTimeMicros, task.indexLink, task.nodeSelectorLink, rsp, qs)) {
            op.setBodyNoCloning(rsp).complete();
        }
    }

    private boolean handleContinuousQueryTaskPatch(Operation op, QueryTask task,
            QuerySpecification qs) throws QueryFilterException {
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
        int resultLimit = Integer.MAX_VALUE;
        selfLink = selfLink.substring(0, selfLink.length() - 1);
        String tq = String.format("documentselflink LIKE '%s%%' ESCAPE '\\'", escapeSqlLike(selfLink));

        ServiceDocumentQueryResult rsp = new ServiceDocumentQueryResult();
        rsp.documentLinks = new ArrayList<>();
        if (queryIndex(get, selfLink, options, tq, null, resultLimit, 0, null, null, rsp,
                null)) {
            return;
        }

        if (serviceOption == ServiceOption.PERSISTENCE) {
            // specific index requested but no results, return empty response
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

    private boolean queryIndex(
            Operation op,
            String selfLinkPrefix,
            EnumSet<QueryOption> options,
            String tq,
            PostgresQueryPage page,
            int count,
            long expiration,
            String indexLink,
            String nodeSelectorPath,
            ServiceDocumentQueryResult rsp,
            QuerySpecification qs) throws Exception {
        if (options == null) {
            options = EnumSet.noneOf(QueryOption.class);
        }

        if (options.contains(QueryOption.EXPAND_CONTENT)
                || options.contains(QueryOption.EXPAND_BINARY_CONTENT)
                || options.contains(QueryOption.EXPAND_SELECTED_FIELDS)) {
            rsp.documents = new HashMap<>();
        }

        if (options.contains(QueryOption.COUNT)) {
            rsp.documentCount = 0L;
        } else {
            rsp.documentLinks = new ArrayList<>();
        }

        Set<String> kindScope = null;

        if (qs != null && qs.context != null) {
            kindScope = qs.context.kindScope;
        }

        long queryStartTimeMicros = Utils.getNowMicrosUtc();
        tq = updateQuery(op, qs, tq, queryStartTimeMicros, options);
        if (tq == null) {
            return false;
        }

        if (qs != null && qs.query != null && this.hasOption(ServiceOption.INSTRUMENTATION)) {
            String queryStat = getQueryStatName(qs.query);
            this.adjustStat(queryStat, 1);
        }

        ServiceDocumentQueryResult result;
        if (options.contains(QueryOption.COUNT)) {
            result = queryIndexCount(options, tq, rsp, qs, queryStartTimeMicros, nodeSelectorPath);
        } else {
            result = queryIndexPaginated(op, options, tq, page, count, expiration, indexLink, nodeSelectorPath,
                    rsp, qs, queryStartTimeMicros);
        }

        result.documentOwner = getHost().getId();
        if (!options.contains(QueryOption.COUNT) && result.documentLinks.isEmpty()) {
            return false;
        }
        op.setBodyNoCloning(result).complete();
        return true;
    }

    private void queryIndexSingle(String selfLink, Operation op, Long version)
            throws Exception {
        try {
            ServiceDocument sd = getDocumentAtVersion(selfLink, version);
            if (sd == null) {
                op.complete();
                return;
            }
            op.setBodyNoCloning(sd).complete();
        } catch (CancellationException e) {
            op.fail(e);
        }
    }

    private ServiceDocument getDocumentAtVersion(String selfLink, Long version)
            throws Exception {
        long startNanos = System.nanoTime();
        try (Connection conn = getConnection();
              Statement stmt = conn.createStatement();
              ResultSet rs = queryIndexForVersion(stmt, selfLink, version, null)) {
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
            if (!rs.next()) {
                return null;
            }

            PostgresDocumentStoredFieldVisitor visitor = new PostgresDocumentStoredFieldVisitor();
            loadDoc(visitor, rs, this.fieldsToLoadWithExpand);

            boolean hasExpired = false;

            Long expiration = visitor.documentExpirationTimeMicros;
            if (expiration != null && expiration != 0) {
                hasExpired = expiration <= Utils.getSystemNowMicrosUtc();
            }

            if (hasExpired) {
                return null;
            }
            return getStateFromLuceneDocument(visitor, selfLink);
        }
    }

    /**
     * Find the document given a self link and version number.
     *
     * This function is used for two purposes; find given version to...
     * 1) load state if the service state is not yet cached
     * 2) filter query results to only include the given version
     *
     * In case (1), authorization is applied in the service host (either against
     * the cached state or freshly loaded state).
     * In case (2), authorization should NOT be applied because the original query
     * already included the resource group query per the authorization context.
     * Query results will be filtered given the REAL latest version, not the latest
     * version subject to the resource group query. This means older versions of
     * a document will NOT appear in the query result if the user is not authorized
     * to see the newer version.
     *
     * If given version is null then function returns the latest version.
     * And if given version is not found then no document is returned.
     */
    private ResultSet queryIndexForVersion(Statement stmt, String selfLink, Long version, Long documentsUpdatedBeforeInMicros)
          throws SQLException {
        String condition = String.format("documentselflink = '%s'", escapeSqlString(selfLink));

        // when QueryOption.TIME_SNAPSHOT  is enabled (documentsUpdatedBeforeInMicros i.e. QuerySpecification.timeSnapshotBoundaryMicros is present)
        // perform query to find a document with link updated before supplied time.
        if (documentsUpdatedBeforeInMicros != null) {
            condition = String.format("%s AND documentupdatetimemicros BETWEEN '0' AND '%s'", condition, documentsUpdatedBeforeInMicros);
        } else if (version != null) {
            condition = String.format("%s AND documentversion = %s", condition, version);
        }

        String query = String.format("SELECT * FROM docs WHERE %s ORDER BY documentversion DESC LIMIT 1", condition);
        //logInfo("SQL query: %s", query);
        return stmt.executeQuery(query);
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

    /**
     * This routine modifies a user-specified query to include clauses which
     * apply the resource group query specified by the operation's authorization
     * context and which exclude expired documents.
     *
     * If the operation was executed by the system user, no resource group query
     * is applied.
     *
     * If no query needs to be executed return null
     *
     * @return Augmented query.
     */
    private String updateQuery(Operation op, QuerySpecification qs, String tq, long now,
            EnumSet<QueryOption> queryOptions) {
        return tq;
            /*
        Query expirationClause = LongPoint.newRangeQuery(
                ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS, 1, now);
        BooleanQuery.Builder builder = new BooleanQuery.Builder()
                .add(expirationClause, Occur.MUST_NOT)
                .add(tq, Occur.FILTER);

        if (queryOptions.contains(QueryOption.INDEXED_METADATA)) {
            if (!queryOptions.contains(QueryOption.INCLUDE_ALL_VERSIONS)
                    && !queryOptions.contains(QueryOption.TIME_SNAPSHOT)) {
                Query currentClause = NumericDocValuesField.newSlowExactQuery(
                        PostgresIndexDocumentHelper.FIELD_NAME_INDEXING_METADATA_VALUE_TOMBSTONE_TIME,
                        PostgresIndexDocumentHelper.ACTIVE_DOCUMENT_TOMBSTONE_TIME);
                builder.add(currentClause, Occur.MUST);
            }
            // There is a bug in lucene where sort and numeric doc values don't play well
            // apply the optimization to limit the resultset only when there is no sort specified
            if ((qs != null && qs.sortTerm == null) &&
                    queryOptions.contains(QueryOption.TIME_SNAPSHOT)) {
                Query tombstoneClause = NumericDocValuesField.newSlowRangeQuery(
                        PostgresIndexDocumentHelper.FIELD_NAME_INDEXING_METADATA_VALUE_TOMBSTONE_TIME,
                        qs.timeSnapshotBoundaryMicros, PostgresIndexDocumentHelper.ACTIVE_DOCUMENT_TOMBSTONE_TIME);
                builder.add(tombstoneClause, Occur.MUST);
            }
        }
        if (!getHost().isAuthorizationEnabled()) {
            return builder.build();
        }

        AuthorizationContext ctx = op.getAuthorizationContext();
        if (ctx == null) {
            // Don't allow operation if no authorization context and auth is enabled
            return null;
        }

        // Allow unconditionally if this is the system user
        if (ctx.isSystemUser()) {
            return builder.build();
        }

        // If the resource query in the authorization context is unspecified,
        // use a Lucene query that doesn't return any documents so that every
        // result will be empty.
        QueryTask.Query resourceQuery = ctx.getResourceQuery(Action.GET);
        Condition rq = null;
        if (resourceQuery == null) {
            rq = DSL.condition("false");
        } else {
            rq = PostgresQueryConverter.convert(resourceQuery, null);
        }

        builder.add(rq, Occur.FILTER);
        return builder.build();
        */
    }

    private void handleGroupByQueryTaskPatch(Operation op, QueryTask task) {
        op.setBodyNoCloning(null).complete();
    /*
        QuerySpecification qs = task.querySpec;
        IndexSearcher s = (IndexSearcher) qs.context.nativeSearcher;
        SolrQueryPage page = (SolrQueryPage) qs.context.nativePage;
        Condition tq = (Condition) qs.context.nativeQuery;
        Sort sort = (Sort) qs.context.nativeSort;
        if (sort == null && qs.sortTerm != null) {
            sort = PostgresQueryConverter.convertToLuceneSort(qs, false);
        }

        Sort groupSort = null;
        if (qs.groupSortTerm != null) {
            groupSort = PostgresQueryConverter.convertToLuceneSort(qs, true);
        }

        GroupingSearch groupingSearch;
        if (qs.groupByTerm.propertyType == ServiceDocumentDescription.TypeName.LONG ||
                qs.groupByTerm.propertyType == ServiceDocumentDescription.TypeName.DOUBLE) {
            groupingSearch = new GroupingSearch(qs.groupByTerm.propertyName + GROUP_BY_PROPERTY_NAME_SUFFIX);
        } else {
            groupingSearch = new GroupingSearch(
                    PostgresIndexDocumentHelper.createSortFieldPropertyName(qs.groupByTerm.propertyName));
        }

        groupingSearch.setGroupSort(groupSort);
        groupingSearch.setSortWithinGroup(sort);

        adjustTimeSeriesStat(STAT_NAME_GROUP_QUERY_COUNT, AGGREGATION_TYPE_SUM, 1);

        int groupOffset = page != null ? page.groupOffset : 0;
        int groupLimit = qs.groupResultLimit != null ? qs.groupResultLimit : 10000;

        Set<String> kindScope = qs.context.kindScope;

        if (s == null && qs.groupResultLimit != null) {
            // Since expiration of QueryPageService and index-searcher uses different mechanism, to guarantee
            // that index-searcher still exists when QueryPageService expired, add some delay for searcher
            // expiration time.
            long expiration = task.documentExpirationTimeMicros + DEFAULT_PAGINATED_SEARCHER_EXPIRATION_DELAY;
            s = createOrUpdatePaginatedQuerySearcher(expiration, this.writer, kindScope, qs.options);
        }

        if (s == null) {
            s = createOrRefreshSearcher(null, kindScope, Integer.MAX_VALUE, this.writer,
                    qs.options.contains(QueryOption.DO_NOT_REFRESH));
        }

        ServiceDocumentQueryResult rsp = new ServiceDocumentQueryResult();
        rsp.nextPageLinksPerGroup = new TreeMap<>();

        // perform the actual search
        long startNanos = System.nanoTime();
        TopGroups<?> groups = groupingSearch.search(s, tq, groupOffset, groupLimit);
        long durationNanos = System.nanoTime() - startNanos;
        setTimeSeriesHistogramStat(STAT_NAME_GROUP_QUERY_DURATION_MICROS, AGGREGATION_TYPE_AVG_MAX,
                TimeUnit.NANOSECONDS.toMicros(durationNanos));

        // generate page links for each grouped result
        for (GroupDocs<?> groupDocs : groups.groups) {
            if (groupDocs.totalHits == 0) {
                continue;
            }
            QueryTask.Query perGroupQuery = Utils.clone(qs.query);

            String groupValue;

            // groupValue can be ANY OF ( GROUPS, null )
            // The "null" group signifies documents that do not have the property.
            if (groupDocs.groupValue != null) {
                groupValue = ((BytesRef) groupDocs.groupValue).utf8ToString();
            } else {
                groupValue = DOCUMENTS_WITHOUT_RESULTS;
            }

            // we need to modify the query to include a top level clause that restricts scope
            // to documents with the groupBy field and value
            QueryTask.Query clause = new QueryTask.Query()
                    .setTermPropertyName(qs.groupByTerm.propertyName)
                    .setTermMatchType(MatchType.TERM);
            clause.occurance = QueryTask.Query.Occurance.MUST_OCCUR;

            if (qs.groupByTerm.propertyType == ServiceDocumentDescription.TypeName.LONG
                    && groupDocs.groupValue != null) {
                clause.setNumericRange(QueryTask.NumericRange.createEqualRange(Long.parseLong(groupValue)));
            } else if (qs.groupByTerm.propertyType == ServiceDocumentDescription.TypeName.DOUBLE
                    && groupDocs.groupValue != null) {
                clause.setNumericRange(QueryTask.NumericRange.createEqualRange(Double.parseDouble(groupValue)));
            } else {
                clause.setTermMatchValue(groupValue);
            }

            if (perGroupQuery.booleanClauses == null) {
                QueryTask.Query topLevelClause = perGroupQuery;
                perGroupQuery.addBooleanClause(topLevelClause);
            }

            perGroupQuery.addBooleanClause(clause);
            Condition lucenePerGroupQuery = PostgresQueryConverter.convert(perGroupQuery, qs.context);

            // for each group generate a query page link
            String pageLink = createNextPage(op, s, qs, lucenePerGroupQuery, sort,
                    null, 0, null,
                    task.documentExpirationTimeMicros, task.indexLink, task.nodeSelectorLink, false);

            rsp.nextPageLinksPerGroup.put(groupValue, pageLink);
        }

        if (qs.groupResultLimit != null && groups.groups.length >= groupLimit) {
            // check if we need to generate a next page for the next set of group results
            if (groups.totalHitCount > groups.totalGroupedHitCount) {
                rsp.nextPageLink = createNextPage(op, s, qs, tq, sort,
                        null, 0, groupLimit + groupOffset,
                        task.documentExpirationTimeMicros, task.indexLink, task.nodeSelectorLink, page != null);
            }
        }

        op.setBodyNoCloning(rsp).complete();
        */
    }

    private ServiceDocumentQueryResult queryIndexCount(
            EnumSet<QueryOption> queryOptions,
            String termQuery,
            ServiceDocumentQueryResult response,
            QuerySpecification querySpec,
            long queryStartTimeMicros,
            String nodeSelectorPath)
            throws Exception {

        if (queryOptions.contains(QueryOption.INCLUDE_ALL_VERSIONS)) {
            // Special handling for queries which include all versions in order to avoid allocating
            // a large, unnecessary ScoreDocs array.

            String query = "SELECT COUNT(*) FROM docs WHERE " + termQuery;
            logInfo("SQL count: %s", query);
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(query)) {
                    rs.next();
                    long count = rs.getLong(1);
                    logInfo("SQL count: %s : %s", query, count);
                    response.documentCount = count;
                }
            }
            long queryTimeMicros = Utils.getNowMicrosUtc() - queryStartTimeMicros;
            response.queryTimeMicros = queryTimeMicros;
            setTimeSeriesHistogramStat(STAT_NAME_QUERY_ALL_VERSIONS_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX, queryTimeMicros);
            return response;
        }

        response.queryTimeMicros = 0L;
        ResultSet after = null;
        long start = queryStartTimeMicros;
        int resultLimit = MIN_QUERY_RESULT_LIMIT;

        String query = "SELECT * FROM docs WHERE " + termQuery;
        logInfo("SQL query: %s", query);
        try (Connection conn = getConnection();
              Statement stmt = conn.createStatement();
              ResultSet rs = stmt.executeQuery(query)) {
            do {
                long queryEndTimeMicros = Utils.getNowMicrosUtc();
                long luceneQueryDurationMicros = queryEndTimeMicros - start;
                response.queryTimeMicros = queryEndTimeMicros - queryStartTimeMicros;

                if (!rs.next()) {
                    break;
                }

                setTimeSeriesHistogramStat(STAT_NAME_QUERY_ALL_VERSIONS_DURATION_MICROS,
                        AGGREGATION_TYPE_AVG_MAX, luceneQueryDurationMicros);

                after = processQueryResults(querySpec, queryOptions, resultLimit,
                      response,
                      rs, start, nodeSelectorPath, false);

                long now = Utils.getNowMicrosUtc();
                setTimeSeriesHistogramStat(STAT_NAME_RESULT_PROCESSING_DURATION_MICROS,
                        AGGREGATION_TYPE_AVG_MAX, now - queryEndTimeMicros);

                start = now;
                // grow the result limit
                resultLimit = Math.min(resultLimit * 2, queryResultLimit);
            } while (true);
        }

        response.documentLinks.clear();
        return response;
    }

    private ServiceDocumentQueryResult queryIndexPaginated(Operation op,
            EnumSet<QueryOption> options,
            String tq,
            PostgresQueryPage page,
            int count,
            long expiration,
            String indexLink,
            String nodeSelectorPath,
            ServiceDocumentQueryResult rsp,
            QuerySpecification qs,
            long queryStartTimeMicros) throws Exception {
        ResultSet after = null;
        boolean hasExplicitLimit = count != Integer.MAX_VALUE;
        boolean isPaginatedQuery = hasExplicitLimit
                && !options.contains(QueryOption.TOP_RESULTS);
        boolean hasPage = page != null;
        boolean shouldProcessResults = true;
        boolean useDirectSearch = options.contains(QueryOption.TOP_RESULTS)
                && options.contains(QueryOption.INCLUDE_ALL_VERSIONS);
        int resultLimit = count;
        int hitCount;

        if (isPaginatedQuery && !hasPage) {
            // QueryTask.resultLimit was set, but we don't have a page param yet, which means this
            // is the initial POST to create the queryTask. Since the initial query results will be
            // discarded in this case, just set the limit to 1 and do not process results.
            resultLimit = 1;
            hitCount = 1;
            shouldProcessResults = false;
            rsp.documentCount = 1L;
        } else if (!hasExplicitLimit) {
            // The query does not have an explicit result limit set. We still specify an implicit
            // limit in order to avoid out of memory conditions, since Lucene will use the limit in
            // order to allocate a results array; however, if the number of hits returned by Lucene
            // is higher than the default limit, we will fail the query later.
            hitCount = queryResultLimit;
        } else if (!options.contains(QueryOption.INCLUDE_ALL_VERSIONS)) {
            // The query has an explicit result limit set, but the value is specified in terms of
            // the number of desired results in the QueryTask.
            // Assume twice as much data fill be fetched to account for the discrepancy.
            // The do/while loop below will correct this estimate at every iteration
            hitCount = Math.min(2 * resultLimit, queryPageResultLimit);
        } else {
            hitCount = resultLimit;
        }

        if (hasPage) {
            // For example, via GET of QueryTask.nextPageLink
            after = page.after;
            rsp.prevPageLink = page.previousPageLink;
        }

        String sort = "documentversion DESC";
        if (qs != null && qs.sortTerm != null) {
            // see if query is part of a task and already has a cached sort
            if (qs.context != null) {
                sort = (String) qs.context.nativeSort;
            }

            if (sort == null) {
                sort = PostgresQueryConverter.convertToPostgresSort(qs, false);
            }
        }

        int queryCount = 0;
        rsp.queryTimeMicros = 0L;
        long start = queryStartTimeMicros;
        int offset = (qs == null || qs.offset == null) ? 0 : qs.offset;

        String query = String.format("SELECT * FROM docs WHERE %s ORDER BY %s", tq, sort);

        logInfo("SQL query: %s", query);
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                // Turn use of the cursor on.
                st.setFetchSize(50);
                try (ResultSet rs = st.executeQuery(query)) {
                    do {
                        if (!rs.next()) {
                            break;
                        }
                        queryCount++;
                        long end = Utils.getNowMicrosUtc();

                        //                long totalHits = rs..size();
                        //                if (!hasExplicitLimit && !hasPage && !isPaginatedQuery
                        //                      && totalHits > hitCount) {
                        //                    throw new IllegalStateException(
                        //                          "Query returned large number of results, please specify a resultLimit. Results:"
                        //                                + totalHits + ", QuerySpec: " + Utils.toJson(qs));
                        //                }

                        //hits = results.size();

                        long queryTime = end - start;

                        rsp.documentCount = 0L;
                        rsp.queryTimeMicros += queryTime;
                        ResultSet bottom = null;
                        if (shouldProcessResults) {
                            start = end;
                            bottom = processQueryResults(qs, options, count, rsp, rs,
                                  queryStartTimeMicros, nodeSelectorPath, true);
                            end = Utils.getNowMicrosUtc();

                            // remove docs for offset
                            int size = rsp.documentLinks.size();
                            if (size < offset) {
                                rsp.documentLinks.clear();
                                rsp.documentCount = 0L;
                                if (rsp.documents != null) {
                                    rsp.documents.clear();
                                }
                                offset -= size;
                            } else {
                                List<String> links = rsp.documentLinks.subList(0, offset);
                                if (rsp.documents != null) {
                                    links.forEach(rsp.documents::remove);
                                }
                                rsp.documentCount -= links.size();
                                links.clear();
                                offset = 0;
                            }

                            if (hasOption(ServiceOption.INSTRUMENTATION)) {
                                String statName = options.contains(QueryOption.INCLUDE_ALL_VERSIONS)
                                        ? STAT_NAME_QUERY_ALL_VERSIONS_DURATION_MICROS
                                        : STAT_NAME_QUERY_DURATION_MICROS;
                                setTimeSeriesHistogramStat(statName, AGGREGATION_TYPE_AVG_MAX, queryTime);
                                setTimeSeriesHistogramStat(
                                        STAT_NAME_RESULT_PROCESSING_DURATION_MICROS,
                                        AGGREGATION_TYPE_AVG_MAX, end - start);
                            }
                        }

                        if (count == Integer.MAX_VALUE || useDirectSearch) {
                            // single pass
                            break;
                        }

                        //                if (rs.isEmpty()) {
                        //                    break;
                        //                }

                        if (isPaginatedQuery) {
                            if (!hasPage) {
                                bottom = null;
                            }

                            if (!hasPage || rsp.documentLinks.size() >= count
                                /*|| rs.getRow().size() < resultLimit*/) {
                                // query had less results then per page limit or page is full of results

                                boolean createNextPageLink = true;
                                if (hasPage) {
                                    int numOfHits = hitCount + offset;
                                    createNextPageLink = checkNextPageHasEntry(bottom, options,
                                          tq, sort, numOfHits, qs, queryStartTimeMicros,
                                          nodeSelectorPath);
                                }

                                if (createNextPageLink) {
                                    rsp.nextPageLink = createNextPage(op, qs, tq, sort, bottom,
                                          offset, null, expiration, indexLink, nodeSelectorPath,
                                          hasPage);
                                }
                                break;
                            }
                        }

                        after = bottom;
                        resultLimit = count - rsp.documentLinks.size();

                        // on the next iteration get twice as much data as in this iteration
                        // but never get more than queryResultLimit at once.
                        hitCount = Math.min(queryPageResultLimit, 2 * hitCount);
                    } while (resultLimit > 0);
                }
            }
        }

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            ServiceStat st = ServiceStatUtils.getOrCreateHistogramStat(this, STAT_NAME_ITERATIONS_PER_QUERY);
            setStat(st, queryCount);
        }

        // TODO: hack
        if (rsp.documentCount == null) {
            rsp.documentCount = (long) rsp.documentLinks.size();
        }
        return rsp;
    }

    /**
     * Checks next page exists or not.
     *
     * If there is a valid entry in searchAfter result, this returns true.
     * If searchAfter result is empty or entries are all invalid(expired, etc), this returns false.
     *
     * For example, let's say there are 5 docs. doc=1,2,5 are valid and doc=3,4 are expired(invalid).
     *
     * When limit=2, the first page shows doc=1,2. In this logic, searchAfter will first fetch
     * doc=3,4 but they are invalid(filtered out in `processQueryResults`).
     * Next iteration will hit doc=5 and it is a valid entry. Therefore, it returns true.
     *
     * If doc=1,2 are valid and doc=3,4,5 are invalid, then searchAfter will hit doc=3,4 and
     * doc=5. However, all entries are invalid. This returns false indicating there is no next page.
     */
    private boolean checkNextPageHasEntry(ResultSet after,
            EnumSet<QueryOption> options,
            String tq,
            String sort,
            int count,
            QuerySpecification qs,
            long queryStartTimeMicros,
            String nodeSelectorPath) throws Exception {

        boolean hasValidNextPageEntry = false;

        // Iterate searchAfter until it finds a *valid* entry.
        // If loop reaches to the end and no valid entries found, then current page is the last page.
        while (after != null) {
            // fetch next page
            Object nextPageResults = null;
            /*
            if (sort == null) {
                nextPageResults = s.searchAfter(after, tq, count);
            } else {
                nextPageResults = s.searchAfter(after, tq, count, sort, false, false);
            }*/
            if (nextPageResults == null) {
                break;
            }

            ResultSet docs = null;
            /*
            // TODO: doesn't work
            ScoreDoc[] hits = nextPageResults.scoreDocs;
            if (hits.length == 0) {
                // reached to the end
                break;
            }*/

            ServiceDocumentQueryResult rspForNextPage = new ServiceDocumentQueryResult();
            rspForNextPage.documents = new HashMap<>();
            // use resultLimit=1 as even one found result means there has to be a next page
            after = processQueryResults(qs, options, 1, rspForNextPage, docs,
                    queryStartTimeMicros, nodeSelectorPath, false);

            if (rspForNextPage.documentCount > 0) {
                hasValidNextPageEntry = true;
                break;
            }
        }

        return hasValidNextPageEntry;
    }

    /**
     * Starts a {@code QueryPageService} to track a partial search result set, associated with a
     * index searcher and search pointers. The page can be used for both grouped queries or
     * document queries
     */
    private String createNextPage(Operation op, QuerySpecification qs,
            String tq,
            String sort,
            ResultSet after,
            int offset,
            Integer groupOffset,
            long expiration,
            String indexLink,
            String nodeSelectorPath,
            boolean hasPage) {

        String nextPageId = Utils.getNowMicrosUtc() + "";
        URI u = UriUtils.buildUri(getHost(), UriUtils.buildUriPath(ServiceUriPaths.CORE_QUERY_PAGE,
                nextPageId));

        // the page link must point to this node, since the index searcher and results have been
        // computed locally. Transform the link to a query page forwarder link, which will
        // transparently forward requests to the current node.

        URI forwarderUri = UriUtils.buildForwardToQueryPageUri(u, getHost().getId());
        String nextLink = forwarderUri.getPath() + UriUtils.URI_QUERY_CHAR
                + forwarderUri.getQuery();

        // Compute previous page link. When FORWARD_ONLY option is specified, do not create previous page link.
        String prevLinkForNewPage = null;
        boolean isForwardOnly = qs.options.contains(QueryOption.FORWARD_ONLY);
        if (!isForwardOnly) {
            URI forwarderUriOfPrevLinkForNewPage = UriUtils.buildForwardToQueryPageUri(op.getReferer(),
                    getHost().getId());
            prevLinkForNewPage = forwarderUriOfPrevLinkForNewPage.getPath()
                    + UriUtils.URI_QUERY_CHAR + forwarderUriOfPrevLinkForNewPage.getQuery();
        }

        // Requests to core/query-page are forwarded to document-index (this service) and
        // referrer of that forwarded request is set to original query-page request.
        // This method is called when query-page wants to create new page for a paginated query.
        // If a new page is going to be created then it is safe to use query-page link
        // from referrer as previous page link of this new page being created.
        PostgresQueryPage page = null;
        if (after != null || groupOffset == null) {
            // page for documents
            page = new PostgresQueryPage(hasPage ? prevLinkForNewPage : null, after);
        } else {
            // page for group results
            page = new PostgresQueryPage(hasPage ? prevLinkForNewPage : null, groupOffset);
        }

        QuerySpecification spec = new QuerySpecification();
        qs.copyTo(spec);

        if (groupOffset == null) {
            spec.options.remove(QueryOption.GROUP_BY);
        }

        spec.offset = offset;
        spec.context.nativeQuery = tq;
        spec.context.nativePage = page;
        spec.context.nativeSearcher = null;
        spec.context.nativeSort = sort;

        ServiceDocument body = new ServiceDocument();
        body.documentSelfLink = u.getPath();
        body.documentExpirationTimeMicros = expiration;

        AuthorizationContext ctx = op.getAuthorizationContext();
        if (ctx != null) {
            body.documentAuthPrincipalLink = ctx.getClaims().getSubject();
        }

        Operation startPost = Operation
                .createPost(u)
                .setBody(body)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logWarning("Unable to start next page service: %s", e.toString());
                    }
                });

        if (ctx != null) {
            setAuthorizationContext(startPost, ctx);
        }

        getHost().startService(startPost, new QueryPageService(spec, indexLink, nodeSelectorPath));
        return nextLink;
    }

    private ResultSet processQueryResults(QuerySpecification qs, EnumSet<QueryOption> options,
            int resultLimit, ServiceDocumentQueryResult rsp, ResultSet rs,
            long queryStartTimeMicros,
            String nodeSelectorPath,
            boolean populateResponse) throws Exception {

        ResultSet lastDocVisited = null;
        Set<String> fieldsToLoad = this.fieldsToLoadNoExpand;
        if (populateResponse && (options.contains(QueryOption.EXPAND_CONTENT)
                || options.contains(QueryOption.OWNER_SELECTION)
                || options.contains(QueryOption.EXPAND_BINARY_CONTENT)
                || options.contains(QueryOption.EXPAND_SELECTED_FIELDS))) {
            fieldsToLoad = this.fieldsToLoadWithExpand;
        }

        if (populateResponse && options.contains(QueryOption.SELECT_LINKS)) {
            fieldsToLoad = new HashSet<>(fieldsToLoad);
            for (QueryTask.QueryTerm link : qs.linkTerms) {
                fieldsToLoad.add(link.propertyName);
            }
        }

        // Keep duplicates out
        Set<String> uniques = new LinkedHashSet<>(rsp.documentLinks);
        final boolean hasCountOption = options.contains(QueryOption.COUNT);
        boolean hasIncludeAllVersionsOption = options.contains(QueryOption.INCLUDE_ALL_VERSIONS);
        Set<String> linkWhiteList = null;
        long documentsUpdatedBefore = -1;

        // will contain the links for which post processing should to be skipped
        // added to support TIME_SNAPSHOT, can be extended in future to represent qs.context.documentLinkBlackList
        Set<String> linkBlackList = options.contains(QueryOption.TIME_SNAPSHOT)
                ? Collections.emptySet() : null;
        if (qs != null) {
            if (qs.context != null && qs.context.documentLinkWhiteList != null) {
                linkWhiteList = qs.context.documentLinkWhiteList;
            }
            if (qs.timeSnapshotBoundaryMicros != null) {
                documentsUpdatedBefore = qs.timeSnapshotBoundaryMicros;
            }
        }

        //long searcherUpdateTime = getSearcherUpdateTime(queryStartTimeMicros);
        Map<String, Long> latestVersionPerLink = new HashMap<>();

        PostgresDocumentStoredFieldVisitor visitor = new PostgresDocumentStoredFieldVisitor();
        boolean firstTime = true;
        while (true) {
            if (firstTime) {
                firstTime = false;
            } else {
                if (!rs.next()) {
                    break;
                }
            }
            if (!hasCountOption && uniques.size() >= resultLimit) {
                break;
            }

            lastDocVisited = rs;
            loadDoc(visitor, rs, fieldsToLoad);
            String link = visitor.documentSelfLink;
            String originalLink = link;

            // ignore results not in supplied white list
            // and also those are in blacklisted links
            if ((linkWhiteList != null && !linkWhiteList.contains(link))
                    || (linkBlackList != null && linkBlackList.contains(originalLink))) {
                continue;
            }

            long documentVersion = visitor.documentVersion;

            Long latestVersion = latestVersionPerLink.get(originalLink);

            if (hasIncludeAllVersionsOption) {
                // Decorate link with version. If a document is marked deleted, at any version,
                // we will include it in the results
                link = UriUtils.buildPathWithVersion(link, documentVersion);
            } else {
                // We first determine what is the latest document version.
                // We then use the latest version to determine if the current document result is relevant.
                if (latestVersion == null) {
                    latestVersion = getLatestVersion(link, documentVersion,
                            documentsUpdatedBefore);

                    // latestVersion == -1 means there was no document version
                    // in history, adding it to blacklist so as to avoid
                    // processing the documents which were found later
                    if (latestVersion == -1) {
                        linkBlackList.add(originalLink);
                        continue;
                    }
                    latestVersionPerLink.put(originalLink, latestVersion);
                }

                if (documentVersion < latestVersion) {
                    continue;
                }

                boolean isDeleted = Action.DELETE.name()
                        .equals(visitor.documentUpdateAction);

                if (isDeleted && !options.contains(QueryOption.INCLUDE_DELETED)) {
                    // ignore a document if its marked deleted and it has the latest version
                    if (documentVersion >= latestVersion) {
                        uniques.remove(link);
                        if (rsp.documents != null) {
                            rsp.documents.remove(link);
                        }
                        if (rsp.selectedLinksPerDocument != null) {
                            rsp.selectedLinksPerDocument.remove(link);
                        }
                    }
                    continue;
                }
            }

            if (hasCountOption || !populateResponse) {
                // count unique instances of this link
                uniques.add(link);
                continue;
            }

            String json = null;
            ServiceDocument state = null;

            if (options.contains(QueryOption.EXPAND_CONTENT)
                    || options.contains(QueryOption.OWNER_SELECTION)
                    || options.contains(QueryOption.EXPAND_SELECTED_FIELDS)) {
                state = getStateFromLuceneDocument(visitor, originalLink);
                if (state == null) {
                    // support reading JSON serialized state for backwards compatibility
                    augmentDoc(visitor, rs, LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE);
                    json = visitor.jsonSerializedState;
                    if (json == null) {
                        continue;
                    }
                }
            }

            if (options.contains(QueryOption.OWNER_SELECTION)) {
                if (!processQueryResultsForOwnerSelection(json, state, nodeSelectorPath)) {
                    continue;
                }
            }

            if (options.contains(QueryOption.EXPAND_BINARY_CONTENT) && !rsp.documents.containsKey(link)) {
                byte[] binaryData = null; //visitor.binarySerializedState;
                if (binaryData != null) {
                    ByteBuffer buffer = ByteBuffer.wrap(binaryData, 0, binaryData.length);
                    rsp.documents.put(link, buffer);
                } else {
                    logWarning("Binary State not found for %s", link);
                }
            } else if (options.contains(QueryOption.EXPAND_CONTENT) && !rsp.documents.containsKey(link)) {
                if (options.contains(QueryOption.EXPAND_BUILTIN_CONTENT_ONLY)) {
                    ServiceDocument stateClone = new ServiceDocument();
                    state.copyTo(stateClone);
                    rsp.documents.put(link, stateClone);
                } else if (state == null) {
                    rsp.documents.put(link, Utils.fromJson(json, JsonElement.class));
                } else {
                    JsonObject jo = toJsonElement(state);
                    rsp.documents.put(link, jo);
                }
            } else if (options.contains(QueryOption.EXPAND_SELECTED_FIELDS) && !rsp.documents.containsKey(link)) {
                // filter out only the selected fields
                Set<String> selectFields = new TreeSet<>();
                if (qs != null) {
                    qs.selectTerms.forEach(qt -> selectFields.add(qt.propertyName));
                }

                // create an uninitialized copy
                ServiceDocument copy = state.getClass().newInstance();
                for (String selectField : selectFields) {
                    // transfer only needed fields
                    Field field = ReflectionUtils.getField(state.getClass(), selectField);
                    if (field != null) {
                        Object value = field.get(state);
                        if (value != null) {
                            field.set(copy, value);
                        }
                    } else {
                        logFine("Unknown field '%s' passed for EXPAND_SELECTED_FIELDS", selectField);
                    }
                }

                JsonObject jo = toJsonElement(copy);
                // this is called only for primitive-typed fields, the rest are nullified already
                jo.entrySet().removeIf(entry -> !selectFields.contains(entry.getKey()));

                rsp.documents.put(link, jo);
            }

            if (options.contains(QueryOption.SELECT_LINKS)) {
                processQueryResultsForSelectLinks(qs, rsp, visitor, rs, link, state);
            }

            uniques.add(link);
        }

        rsp.documentLinks.clear();
        rsp.documentLinks.addAll(uniques);
        rsp.documentCount = (long) rsp.documentLinks.size();
        return lastDocVisited;
    }

    private JsonObject toJsonElement(ServiceDocument state) {
        return (JsonObject) GsonSerializers.getJsonMapperFor(state.getClass()).toJsonElement(state);
    }

    private void loadDoc(PostgresDocumentStoredFieldVisitor visitor, ResultSet rs, Set<String> fields) throws SQLException {
        visitor.reset();

        String data = rs.getString("data");
        visitor.jsonSerializedState = data;

        JsonObject map = Utils.fromJson(data, JsonObject.class);

        map.entrySet().forEach(e -> {
            if (e.getValue().isJsonPrimitive()) {
                if (e.getValue().getAsJsonPrimitive().isString()) {
                    visitor.stringField(e.getKey(), e.getValue().getAsString());
                } else if (e.getValue().getAsJsonPrimitive().isNumber()) {
                    visitor.longField(e.getKey(), e.getValue().getAsLong());
                }
            }
        });
    }

    private void augmentDoc(PostgresDocumentStoredFieldVisitor visitor, ResultSet rs, String field) throws SQLException {
        visitor.reset();
        loadDoc(visitor, rs, Collections.singleton(field));
    }

    private boolean processQueryResultsForOwnerSelection(String json, ServiceDocument state, String nodeSelectorPath) {
        String documentSelfLink;
        if (state == null) {
            documentSelfLink = Utils.fromJson(json, ServiceDocument.class).documentSelfLink;
        } else {
            documentSelfLink = state.documentSelfLink;
        }
        // when node-selector is not specified via query, use the one for index-service which may be null
        if (nodeSelectorPath == null) {
            nodeSelectorPath = getPeerNodeSelectorPath();
        }
        SelectOwnerResponse ownerResponse = getHost().findOwnerNode(nodeSelectorPath, documentSelfLink);

        // omit the result if the documentOwner is not the same as the local owner
        return ownerResponse != null && ownerResponse.isLocalHostOwner;
    }

    private ServiceDocument processQueryResultsForSelectLinks(
            QuerySpecification qs, ServiceDocumentQueryResult rsp, PostgresDocumentStoredFieldVisitor d, ResultSet doc,
            String link,
            ServiceDocument state) throws Exception {
        if (rsp.selectedLinksPerDocument == null) {
            rsp.selectedLinksPerDocument = new HashMap<>();
            rsp.selectedLinks = new HashSet<>();
        }
        Map<String, String> linksPerDocument = rsp.selectedLinksPerDocument.get(link);
        if (linksPerDocument == null) {
            linksPerDocument = new HashMap<>();
            rsp.selectedLinksPerDocument.put(link, linksPerDocument);
        }

        for (QueryTask.QueryTerm qt : qs.linkTerms) {
            String linkValue = d.getLink(qt.propertyName);
            if (linkValue != null) {
                linksPerDocument.put(qt.propertyName, linkValue);
                rsp.selectedLinks.add(linkValue);
                continue;
            }

            // if there is no stored field with the link term property name, it might be
            // a field with a collection of links. We do not store those in lucene, they are
            // part of the binary serialized state.
            if (state == null) {
                PostgresDocumentStoredFieldVisitor visitor = new PostgresDocumentStoredFieldVisitor();
                loadDoc(visitor, doc, this.fieldsToLoadWithExpand);
                state = getStateFromLuceneDocument(visitor, link);
                if (state == null) {
                    logWarning("Skipping link term %s for %s, can not find serialized state",
                            qt.propertyName, link);
                    continue;
                }
            }

            Field linkCollectionField = ReflectionUtils
                    .getField(state.getClass(), qt.propertyName);
            if (linkCollectionField == null) {
                continue;
            }
            Object fieldValue = linkCollectionField.get(state);
            if (fieldValue == null) {
                continue;
            }
            if (!(fieldValue instanceof Collection<?>)) {
                logWarning("Skipping link term %s for %s, field is not a collection",
                        qt.propertyName, link);
                continue;
            }
            @SuppressWarnings("unchecked")
            Collection<String> linkCollection = (Collection<String>) fieldValue;
            int index = 0;
            for (String item : linkCollection) {
                if (item != null) {
                    linksPerDocument.put(
                            QuerySpecification.buildLinkCollectionItemName(qt.propertyName, index++),
                            item);
                    rsp.selectedLinks.add(item);
                }
            }
        }
        return state;
    }

    private ServiceDocument getStateFromLuceneDocument(PostgresDocumentStoredFieldVisitor doc, String link) {
        String json = doc.jsonSerializedState;
        if (json == null) {
            logWarning("State not found for %s", link);
            return null;
        }

        // TODO: how to find class
        Class<?> clazz;
        String className = doc.documentKind.replace(':', '.');
        while (true) {
            try {
                clazz = Class.forName(className);
                break;
            } catch (ClassNotFoundException e) {
                int i = className.lastIndexOf('.');
                if (i == -1) {
                    throw new RuntimeException(e);
                }

                // Check if inner class, replace last '.' with '$'
                StringBuilder sb = new StringBuilder(className);
                sb.setCharAt(i, '$');
                className = sb.toString();
            }
        }
        ServiceDocument state = (ServiceDocument) Utils.fromJson(json, clazz);

        if (state.documentSelfLink == null) {
            state.documentSelfLink = link;
        }
        if (state.documentKind == null) {
            state.documentKind = Utils.buildKind(state.getClass());
        }
        return state;
    }

    private long getLatestVersion(
            String link, long version, long documentsUpdatedBeforeInMicros) throws SQLException {
        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            adjustStat(STAT_NAME_VERSION_CACHE_LOOKUP_COUNT, 1);
        }

        /*
        synchronized (this.searchSync) {
            DocumentUpdateInfo dui = this.updatesPerLink.get(link);
            if (documentsUpdatedBeforeInMicros == -1 && dui != null && dui.updateTimeMicros <= searcherUpdateTime) {
                return Math.max(version, dui.version);
            }

            if (!this.immutableParentLinks.isEmpty()) {
                String parentLink = UriUtils.getParentPath(link);
                if (this.immutableParentLinks.containsKey(parentLink)) {
                    // all immutable services have just a single, zero, version
                    return 0;
                }
            }
        }
        */

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            adjustStat(STAT_NAME_VERSION_CACHE_MISS_COUNT, 1);
        }

        try (Connection conn = getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = queryIndexForVersion(stmt, link, null,
                documentsUpdatedBeforeInMicros > 0 ? documentsUpdatedBeforeInMicros : null)) {

            boolean isNext = rs.next();
            // Checking if total hits were Zero when QueryOption.TIME_SNAPSHOT is enabled

            if (documentsUpdatedBeforeInMicros != -1 && !isNext) {
                return -1;
            }

            if (!isNext) {
                return version;
            }

            PostgresDocumentStoredFieldVisitor visitor = new PostgresDocumentStoredFieldVisitor();
            loadDoc(visitor, rs, this.fieldToLoadVersionLookup);

            long latestVersion = visitor.documentVersion;
            long updateTime = visitor.documentUpdateTimeMicros;
            // attempt to refresh or create new version cache entry, from the entry in the query results
            // The update method will reject the update if the version is stale
            updateLinkInfoCache(null, link, null, latestVersion, updateTime);
            return latestVersion;
        }
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

        PostgresIndexDocumentHelper indexDocHelper = this.indexDocumentHelper.get();

        indexDocHelper.addSelfLinkField(link);
        if (s.documentKind != null) {
            indexDocHelper.addKindField(s.documentKind);
        }
        indexDocHelper.addUpdateActionField(s.documentUpdateAction);
        //indexDocHelper.addBinaryStateFieldToDocument(s, r.serializedDocument, desc);
        if (s.documentAuthPrincipalLink != null) {
            indexDocHelper.addAuthPrincipalLinkField(s.documentAuthPrincipalLink);
        }
        if (s.documentTransactionId != null) {
            indexDocHelper.addTxIdField(s.documentTransactionId);
        }
        indexDocHelper.addUpdateTimeField(s.documentUpdateTimeMicros);
        if (s.documentExpirationTimeMicros > 0) {
            indexDocHelper.addExpirationTimeField(s.documentExpirationTimeMicros);
        }
        indexDocHelper.addVersionField(s.documentVersion);

        if (desc.documentIndexingOptions.contains(DocumentIndexingOption.INDEX_METADATA)) {
            indexDocHelper.addIndexingIdField(link, s.documentEpoch, s.documentVersion);
            indexDocHelper.addTombstoneTimeField();
        }

        try {
            if (desc.propertyDescriptions == null
                    || desc.propertyDescriptions.isEmpty()) {
                // no additional property type information, so we will add the
                // document with common fields indexed plus the full body
                addDocumentToIndex(updateOp, s, desc);
                return;
            }

            indexDocHelper.addIndexableFieldsToDocument(s, desc);

            addDocumentToIndex(updateOp, s, desc);
        } finally {
            // NOTE: The Document is a thread local managed by the index document helper. Its fields
            // must be cleared *after* its added to the index (above) and *before* its re-used.
            // After the fields are cleared, the document can not be used in this scope
        }
    }

    private void checkDocumentRetentionLimit(ServiceDocument state, ServiceDocumentDescription desc) {
        if (desc.versionRetentionLimit
                == ServiceDocumentDescription.FIELD_VALUE_DISABLED_VERSION_RETENTION) {
            return;
        }

        long limit = Math.max(1L, desc.versionRetentionLimit);
        if (state.documentVersion < limit) {
            return;
        }

        // If the addition of the new document version has not pushed the current document across
        // a retention threshold boundary, then return. A retention boundary is reached when the
        // addition of a new document means that more versions of the document are present in the
        // index than the versionRetentionLimit specified in the service document description.
        long floor = Math.max(1L, desc.versionRetentionFloor);
        if (floor > limit) {
            floor = limit;
        }

        long chunkThreshold = Math.max(1L, limit - floor);
        if (((state.documentVersion - limit) % chunkThreshold) != 0) {
            return;
        }

        String link = state.documentSelfLink;
        long newValue = state.documentVersion - floor;
        synchronized (this.liveVersionsPerLink) {
            Long currentValue = this.liveVersionsPerLink.get(link);
            if (currentValue == null || newValue > currentValue) {
                this.liveVersionsPerLink.put(link, newValue);
            }
        }
    }

    /**
     * Will attempt to re-open index writer to recover from a specific exception. The method
     * assumes the caller has acquired the writer semaphore
     */
    private void checkFailureAndRecover(Exception e) {
        if (getHost().isStopping()) {
            logInfo("Exception after host stop, on index service thread: %s", e.toString());
            return;
        }

        logSevere("Exception on index service thread: %s", Utils.toString(e));
        this.adjustStat(STAT_NAME_WRITER_ALREADY_CLOSED_EXCEPTION_COUNT, 1);
    }


    private void deleteAllDocumentsForSelfLinkForcedPost(ServiceDocument sd)
            throws SQLException {
        String delete = String.format("DELETE FROM docs WHERE documentselflink = '%s'",
                escapeSqlString(sd.documentSelfLink));
        logInfo("SQL delete: %s", delete);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            int response = stmt.executeUpdate(delete);
            logInfo("SQL delete: %s : %s", delete, response);
        }

        // Delete all previous versions from the index. If we do not, we will end up with
        // duplicate version history
        adjustStat(STAT_NAME_FORCED_UPDATE_DOCUMENT_DELETE_COUNT, 1);
        //wr.deleteDocuments(new Term(ServiceDocument.FIELD_NAME_SELF_LINK, sd.documentSelfLink));
        synchronized (this.searchSync) {
            // Clean previous cached entry
            this.updatesPerLink.remove(sd.documentSelfLink);
            long now = Utils.getNowMicrosUtc();
            //this.serviceRemovalDetectedTimeMicros = now;
        }
        updateLinkInfoCache(getHost().buildDocumentDescription(sd.documentSelfLink),
                sd.documentSelfLink, sd.documentKind, 0, Utils.getNowMicrosUtc());
    }

    private void deleteAllDocumentsForSelfLink(Operation postOrDelete, String link,
            ServiceDocument state)
            throws Exception {
        deleteDocumentsFromIndex(postOrDelete,
                state != null ? getHost().buildDocumentDescription(state.documentSelfLink) : null,
                link, state != null ? state.documentKind : null, 0, Long.MAX_VALUE);
        synchronized (this.searchSync) {
            // Remove previous cached entry
            this.updatesPerLink.remove(link);
            long now = Utils.getNowMicrosUtc();
            //this.serviceRemovalDetectedTimeMicros = now;
        }
        adjustTimeSeriesStat(STAT_NAME_SERVICE_DELETE_COUNT, AGGREGATION_TYPE_SUM, 1);
        logFine("%s expired", link);
        if (state == null) {
            return;
        }

        applyActiveQueries(postOrDelete, state, null);
        // remove service, if its running
        sendRequest(Operation.createDelete(this, state.documentSelfLink)
                .setBodyNoCloning(state)
                .disableFailureLogging(true)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE));
    }

    private void deleteDocumentsFromIndex(Operation delete, ServiceDocumentDescription desc, String link, String kind, long oldestVersion,
            long newestVersion) throws Exception {

        deleteDocumentFromIndex(link, oldestVersion, newestVersion);

        // Use time AFTER index was updated to be sure that it can be compared
        // against the time the searcher was updated and have this change
        // be reflected in the new searcher. If the start time would be used,
        // it is possible to race with updating the searcher and NOT have this
        // change be reflected in the searcher.
        updateLinkInfoCache(desc, link, kind, newestVersion, Utils.getNowMicrosUtc());
        delete.complete();
    }

    private void deleteDocumentFromIndex(String link, long oldestVersion, long newestVersion) throws SQLException {
        String delete = String.format("DELETE FROM docs WHERE documentselflink = '%s' AND documentversion BETWEEN '%s' AND '%s'",
                escapeSqlString(link), oldestVersion, newestVersion);
        logInfo("SQL delete: %s", delete);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            int response = stmt.executeUpdate(delete);
            logInfo("SQL delete: %s : %s", delete, response);
        }
    }

    private void addDocumentToIndex(Operation op, ServiceDocument sd,
            ServiceDocumentDescription desc) throws SQLException {
        long startNanos = 0;
        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            startNanos = System.nanoTime();
        }

        if (op.getAction() == Action.POST
                && op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)) {

            // DocumentIndexingOption.INDEX_METADATA instructs the index service to maintain
            // additional metadata attributes in the index, such as whether a particular Lucene
            // document represents the "current" version of a service, or whether the service is
            // deleted.
            //
            // Since these attributes are updated out of band, there is the potential of a race
            // between this update and the metadata attribute updates, which occur during index
            // service maintenance. This race cannot be avoided by the caller, so we use a lock
            // here to force metadata indexing updates to be flushed before deleting the existing
            // documents.
            //
            // Note this may cause significant additional latency under load for this particular
            // combination of options (PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE and INDEX_METADATA). My
            // original preference was to fail operations in this category; however, supporting
            // this scenario is required in order to support migration for the time being, where
            // services with INDEX_METADATA may be deleted and recreated.
            if (desc.documentIndexingOptions.contains(DocumentIndexingOption.INDEX_METADATA)) {
                synchronized (this.metadataUpdateSync) {
                    synchronized (this.metadataUpdates) {
                        this.metadataUpdatesPerLink.remove(sd.documentSelfLink);
                        this.metadataUpdates.removeIf((info) ->
                                info.selfLink.equals(sd.documentSelfLink));
                    }
                }
            }

            deleteAllDocumentsForSelfLinkForcedPost(sd);
        }

        logInfo("SQL insert: link=%s : ver=%s : node=%s", sd.documentSelfLink, sd.documentVersion, getHost().getId());
        try (Connection conn = getConnection();
              PreparedStatement stmt = conn.prepareStatement("INSERT INTO docs (data,documentselflink,documentversion,documentkind,documentexpirationtimemicros,documentupdateaction,documentupdatetimemicros,documenttransactionid,documentauthprincipallink,tenant) VALUES (to_json(?::json),?,?,?,?,?,?,?,?,?)")) {
            stmt.setString(1, Utils.toJson(sd));
            stmt.setString(2, sd.documentSelfLink);
            stmt.setLong(3, sd.documentVersion);
            stmt.setString(4, sd.documentKind);
            stmt.setLong(5, sd.documentExpirationTimeMicros);
            stmt.setString(6, sd.documentUpdateAction);
            stmt.setLong(7, sd.documentUpdateTimeMicros);
            stmt.setString(8, sd.documentTransactionId);
            stmt.setString(9, sd.documentAuthPrincipalLink);
            stmt.setString(10, "");

            stmt.executeUpdate();
        }

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            long durationNanos = System.nanoTime() - startNanos;
            setTimeSeriesStat(STAT_NAME_INDEXED_DOCUMENT_COUNT, AGGREGATION_TYPE_SUM, 1);
            setTimeSeriesHistogramStat(STAT_NAME_INDEXING_DURATION_MICROS, AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(durationNanos));
        }

        // Use time AFTER index was updated to be sure that it can be compared
        // against the time the searcher was updated and have this change
        // be reflected in the new searcher. If the start time would be used,
        // it is possible to race with updating the searcher and NOT have this
        // change be reflected in the searcher.
        long updateTime = Utils.getNowMicrosUtc();
        updateLinkInfoCache(desc, sd.documentSelfLink, sd.documentKind, sd.documentVersion,
                updateTime);
        op.setBody(null).complete();
        checkDocumentRetentionLimit(sd, desc);
        checkDocumentIndexingMetadata(sd, desc, updateTime);
        applyActiveQueries(op, sd, desc);
    }

    private void checkDocumentIndexingMetadata(ServiceDocument sd, ServiceDocumentDescription desc,
            long updateTimeMicros) {

        if (!desc.documentIndexingOptions.contains(DocumentIndexingOption.INDEX_METADATA)) {
            return;
        }

        if (sd.documentVersion == 0) {
            return;
        }

        /*
        synchronized (this.metadataUpdates) {
            MetadataUpdateInfo info = this.metadataUpdatesPerLink.get(sd.documentSelfLink);
            if (info != null) {
                if (info.updateTimeMicros < updateTimeMicros) {
                    this.metadataUpdates.remove(info);
                    info.updateTimeMicros = updateTimeMicros;
                    this.metadataUpdates.add(info);
                }
                return;
            }

            info = new MetadataUpdateInfo();
            info.selfLink = sd.documentSelfLink;
            info.kind = sd.documentKind;
            info.updateTimeMicros = updateTimeMicros;

            this.metadataUpdatesPerLink.put(sd.documentSelfLink, info);
            this.metadataUpdates.add(info);
        }
        */
    }

    private void updateLinkInfoCache(ServiceDocumentDescription desc,
            String link, String kind, long version, long lastAccessTime) {
        /*
        boolean isImmutable = desc != null
                && desc.serviceCapabilities != null
                && desc.serviceCapabilities.contains(ServiceOption.IMMUTABLE);
        synchronized (this.searchSync) {
            if (isImmutable) {
                String parent = UriUtils.getParentPath(link);
                this.immutableParentLinks.compute(parent, (k, time) -> {
                    if (time == null) {
                        time = lastAccessTime;
                    } else {
                        time = Math.max(time, lastAccessTime);
                    }
                    return time;
                });
            } else {
                this.updatesPerLink.compute(link, (k, entry) -> {
                    if (entry == null) {
                        entry = new DocumentUpdateInfo();
                    }
                    if (version >= entry.version) {
                        entry.updateTimeMicros = Math.max(entry.updateTimeMicros, lastAccessTime);
                        entry.version = version;
                    }
                    return entry;
                });
            }

            if (kind != null) {
                this.documentKindUpdateInfo.compute(kind, (k, entry) -> {
                    if (entry == null) {
                        entry = 0L;
                    }
                    entry = Math.max(entry, lastAccessTime);
                    return entry;
                });
            }

            // The index update time may only be increased.
            if (this.writerUpdateTimeMicros < lastAccessTime) {
                this.writerUpdateTimeMicros = lastAccessTime;
            }
        }
        */
    }

    private void updateLinkInfoCacheForMetadataUpdates(long updateTimeMicros,
            Collection<MetadataUpdateInfo> entries) {
        /*
        synchronized (this.searchSync) {
            for (MetadataUpdateInfo info : entries) {
                this.updatesPerLink.compute(info.selfLink, (k, entry) -> {
                    if (entry != null) {
                        entry.updateTimeMicros = Math.max(entry.updateTimeMicros, updateTimeMicros);
                    }
                    return entry;
                });

                this.documentKindUpdateInfo.compute(info.kind, (k, entry) -> {
                    entry = Math.max(entry, updateTimeMicros);
                    return entry;
                });
            }

            if (this.writerUpdateTimeMicros < updateTimeMicros) {
                this.writerUpdateTimeMicros = updateTimeMicros;
            }
        }
        */
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
            long searcherCreationTime = Utils.getNowMicrosUtc();

            synchronized (this.metadataUpdates) {
                this.metadataUpdatesPerLink.clear();
            }

            long startNanos = System.nanoTime();
            long endNanos = System.nanoTime();
            setTimeSeriesHistogramStat(STAT_NAME_MAINTENANCE_SEARCHER_REFRESH_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            long deadline = Utils.getSystemNowMicrosUtc() + getMaintenanceIntervalMicros();

            startNanos = endNanos;
            applyDocumentExpirationPolicy(deadline);
            endNanos = System.nanoTime();
            setTimeSeriesHistogramStat(STAT_NAME_MAINTENANCE_DOCUMENT_EXPIRATION_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            startNanos = endNanos;
            applyDocumentVersionRetentionPolicy(deadline);
            endNanos = System.nanoTime();
            setTimeSeriesHistogramStat(STAT_NAME_MAINTENANCE_VERSION_RETENTION_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            startNanos = endNanos;
            synchronized (this.metadataUpdateSync) {
                applyMetadataIndexingUpdates(searcherCreationTime, deadline);
            }
            endNanos = System.nanoTime();
            setTimeSeriesHistogramStat(STAT_NAME_MAINTENANCE_METADATA_INDEXING_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            startNanos = endNanos;
            applyMemoryLimit();
            endNanos = System.nanoTime();
            setTimeSeriesHistogramStat(STAT_NAME_MAINTENANCE_MEMORY_LIMIT_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            startNanos = endNanos;
            endNanos = System.nanoTime();
            adjustTimeSeriesStat(STAT_NAME_COMMIT_COUNT, AGGREGATION_TYPE_SUM, 1);
            setTimeSeriesHistogramStat(STAT_NAME_COMMIT_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            // Only send notification when something has committed.
            // When there was nothing to commit, sequence number is -1.
            /*
            if (sequenceNumber > -1) {
                CommitInfo commitInfo = new CommitInfo();
                commitInfo.sequenceNumber = sequenceNumber;
                publish(Operation.createPatch(null).setBody(commitInfo));
            }
            */

            startNanos = endNanos;
            endNanos = System.nanoTime();
            setTimeSeriesHistogramStat(STAT_NAME_MAINTENANCE_FILE_LIMIT_REFRESH_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            if (this.hasOption(ServiceOption.INSTRUMENTATION)) {
                logQueueDepthStat(this.updateQueue, STAT_NAME_FORMAT_UPDATE_QUEUE_DEPTH);
                logQueueDepthStat(this.queryQueue, STAT_NAME_FORMAT_QUERY_QUEUE_DEPTH);
            }

            op.complete();
        } catch (Exception e) {
            if (this.getHost().isStopping()) {
                op.fail(new CancellationException("Host is stopping"));
                return;
            }
            logWarning("Attempting recovery due to error: %s", Utils.toString(e));
            op.fail(e);
        }
    }

    private void logQueueDepthStat(RoundRobinOperationQueue queue, String format) {
        Map<String, Integer> sizes = queue.sizesByKey();
        for (Entry<String, Integer> e : sizes.entrySet()) {
            String statName = String.format(format, e.getKey());
            setTimeSeriesStat(statName, AGGREGATION_TYPE_AVG_MAX, e.getValue());
        }
    }

    private void applyMetadataIndexingUpdates(long searcherCreationTime,
            long deadline) {
        Map<String, MetadataUpdateInfo> entries = new HashMap<>();
        synchronized (this.metadataUpdates) {
            Iterator<MetadataUpdateInfo> it = this.metadataUpdates.iterator();
            while (it.hasNext()) {
                MetadataUpdateInfo info = it.next();
                if (info.updateTimeMicros > searcherCreationTime) {
                    break;
                }

                entries.put(info.selfLink, info);
                it.remove();
            }
        }

        if (entries.isEmpty()) {
            return;
        }

        Collection<MetadataUpdateInfo> entriesToProcess = entries.values();
        int queueDepth = entriesToProcess.size();
        Iterator<MetadataUpdateInfo> it = entriesToProcess.iterator();
        int updateCount = 0;
        while (it.hasNext() && --queueDepth > metadataUpdateMaxQueueDepth) {
            updateCount += applyMetadataIndexingUpdate(it.next());
        }

        while (it.hasNext() && Utils.getSystemNowMicrosUtc() < deadline) {
            updateCount += applyMetadataIndexingUpdate(it.next());
        }

        if (it.hasNext()) {
            synchronized (this.metadataUpdates) {
                while (it.hasNext()) {
                    MetadataUpdateInfo info = it.next();
                    it.remove();
                    this.metadataUpdatesPerLink.putIfAbsent(info.selfLink, info);
                    this.metadataUpdates.add(info);
                }
            }
        }

        updateLinkInfoCacheForMetadataUpdates(Utils.getNowMicrosUtc(), entriesToProcess);

        if (updateCount > 0) {
            setTimeSeriesHistogramStat(STAT_NAME_METADATA_INDEXING_UPDATE_COUNT,
                    AGGREGATION_TYPE_SUM, updateCount);
        }
    }

    private long applyMetadataIndexingUpdate(MetadataUpdateInfo info) {
        /*
        String query = String.format("SELECT * FROM docs WHERE documentselflink = '%s'", escapeSqlString(info.selfLink);

        logInfo("SQL query: %s", query);
        try (Connection conn = getConnection();
              Statement stmt = conn.createStatement();
              ResultSet rs = stmt.executeQuery(query)) {

        }

        Query selfLinkClause = new TermQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK,
                info.selfLink));
        Query currentClause = NumericDocValuesField.newSlowExactQuery(
                PostgresIndexDocumentHelper.FIELD_NAME_INDEXING_METADATA_VALUE_TOMBSTONE_TIME,
                PostgresIndexDocumentHelper.ACTIVE_DOCUMENT_TOMBSTONE_TIME);

        Query booleanQuery = new BooleanQuery.Builder()
                .add(selfLinkClause, Occur.MUST)
                .add(currentClause, Occur.MUST)
                .build();

        //
        // In a perfect world, we'd sort the results here and examine the first result to determine
        // whether the document has been deleted. Unfortunately, Lucene 6.5 has a bug where, for
        // queries which specify sorts, NumericDocValuesField query clauses are ignored (these
        // queries are new and experimental in 6.5). As a result, we must traverse the unordered
        // results and track the highest result that we've seen.
        //

        long highestVersion = -1;
        String lastUpdateAction = null;

        final int pageSize = 10000;

        long updateCount = 0;
        ScoreDoc after = null;
        // PostgresDocumentStoredFieldVisitor is a list as we can have multiple entries for the same
        // version because of how synchronization works
        Map<Long, List<PostgresDocumentStoredFieldVisitor>> versionToDocsMap = new HashMap<>();
        while (true) {
            TopDocs results = searcher.searchAfter(after, booleanQuery, pageSize);
            if (results == null || results.scoreDocs == null || results.scoreDocs.length == 0) {
                break;
            }

            for (ScoreDoc scoreDoc : results.scoreDocs) {
                PostgresDocumentStoredFieldVisitor visitor = new PostgresDocumentStoredFieldVisitor();
                loadDoc(visitor, scoreDoc.doc, this.fieldsToLoadIndexingIdLookup);
                List<PostgresDocumentStoredFieldVisitor> versionDocList = versionToDocsMap
                        .computeIfAbsent(visitor.documentVersion, k -> new ArrayList<>());
                versionDocList.add(visitor);

                if (visitor.documentVersion > highestVersion) {
                    highestVersion = visitor.documentVersion;
                    lastUpdateAction = visitor.documentUpdateAction;
                }
            }
            // check to see if the next version is available for all documents returned in the query above
            Set<Long> missingVersions = new HashSet<>();
            for (Long version : versionToDocsMap.keySet()) {
                if (version == highestVersion) {
                    continue;
                }
                if (!versionToDocsMap.containsKey(version + 1)) {
                    missingVersions.add(version + 1);
                }
            }
            // fetch docs for the missing versions
            Query versionClause = LongPoint.newSetQuery(ServiceDocument.FIELD_NAME_VERSION, missingVersions);
            Query missingVersionQuery = new BooleanQuery.Builder()
                    .add(selfLinkClause, Occur.MUST)
                    .add(versionClause, Occur.MUST)
                    .build();
            TopDocs missingVersionResult = searcher.searchAfter(after, missingVersionQuery, pageSize);
            if (missingVersionResult != null && missingVersionResult.scoreDocs != null
                    && missingVersionResult.scoreDocs.length != 0) {
                for (ScoreDoc scoreDoc : missingVersionResult.scoreDocs) {
                    PostgresDocumentStoredFieldVisitor visitor = new PostgresDocumentStoredFieldVisitor();
                    loadDoc(searcher, visitor, scoreDoc.doc, this.fieldsToLoadIndexingIdLookup);
                    List<PostgresDocumentStoredFieldVisitor> versionDocList = versionToDocsMap
                            .computeIfAbsent(visitor.documentVersion, k -> new ArrayList<>());
                    versionDocList.add(visitor);
                }
            }
            // update the metadata for fields as necessary
            for (List<PostgresDocumentStoredFieldVisitor> visitorDocs : versionToDocsMap.values()) {
                for (PostgresDocumentStoredFieldVisitor visitor : visitorDocs) {
                    if ((visitor.documentVersion == highestVersion &&
                            !Action.DELETE.toString().equals(lastUpdateAction)) ||
                            visitor.documentTombstoneTimeMicros != PostgresIndexDocumentHelper.ACTIVE_DOCUMENT_TOMBSTONE_TIME) {
                        continue;
                    }
                    Long nextVersionCreationTime = null;
                    if (visitor.documentVersion == highestVersion) {
                        // pick the update time on the first entry. They should be the same for all docs of the same version
                        PostgresDocumentStoredFieldVisitor firstDoc = versionToDocsMap.get(visitor.documentVersion).get(0);
                        nextVersionCreationTime = firstDoc.documentUpdateTimeMicros;
                    } else {
                        List<PostgresDocumentStoredFieldVisitor> list = versionToDocsMap.get(visitor.documentVersion + 1);
                        if (list != null) {
                            nextVersionCreationTime = list.get(0).documentUpdateTimeMicros;
                        }
                    }
                    if (nextVersionCreationTime != null) {
                        updateTombstoneTime(wr, visitor.documentIndexingId, nextVersionCreationTime);
                        updateCount++;
                    }
                }
            }
            if (results.scoreDocs.length < pageSize) {
                break;
            }

            after = results.scoreDocs[results.scoreDocs.length - 1];
        }
        return updateCount;
        */
        return 0;
    }

    /*
    private void updateTombstoneTime(IndexWriter wr, String indexingId, long documentUpdateTimeMicros) throws IOException {
        Term indexingIdTerm = new Term(PostgresIndexDocumentHelper.FIELD_NAME_INDEXING_ID,
                indexingId);
        wr.updateNumericDocValue(indexingIdTerm,
                PostgresIndexDocumentHelper.FIELD_NAME_INDEXING_METADATA_VALUE_TOMBSTONE_TIME, documentUpdateTimeMicros);
    }
    */

    private void applyDocumentVersionRetentionPolicy(long deadline) throws Exception {
        Map<String, Long> links = new HashMap<>();
        Iterator<Entry<String, Long>> it;

        do {
            int count = 0;
            synchronized (this.liveVersionsPerLink) {
                it = this.liveVersionsPerLink.entrySet().iterator();
                while (it.hasNext() && count < versionRetentionServiceThreshold) {
                    Entry<String, Long> e = it.next();
                    links.put(e.getKey(), e.getValue());
                    it.remove();
                    count++;
                }
            }

            if (links.isEmpty()) {
                break;
            }

            adjustTimeSeriesStat(STAT_NAME_VERSION_RETENTION_SERVICE_COUNT, AGGREGATION_TYPE_SUM,
                    links.size());

            Operation dummyDelete = Operation.createDelete(null);
            for (Entry<String, Long> e : links.entrySet()) {
                deleteDocumentsFromIndex(dummyDelete, null, e.getKey(), null,  0, e.getValue());
            }

            links.clear();

        } while (Utils.getSystemNowMicrosUtc() < deadline);
    }

    private void applyMemoryLimit() {
        if (getHost().isStopping()) {
            return;
        }
        // close any paginated query searchers that have expired
        long now = Utils.getNowMicrosUtc();
        applyMemoryLimitToDocumentUpdateInfo();
    }

    void applyMemoryLimitToDocumentUpdateInfo() {
        long memThresholdBytes = this.updateMapMemoryLimit;
        final int bytesPerLinkEstimate = 64;
        int count = 0;

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            setStat(STAT_NAME_VERSION_CACHE_ENTRY_COUNT, this.updatesPerLink.size());
        }
        // Note: this code will be updated in the future. It currently calls a host
        // method, inside a lock, which is always a bad idea. The getServiceStage()
        // method is lock free, but its still brittle.
        synchronized (this.searchSync) {
            if (this.updatesPerLink.isEmpty()) {
                return;
            }
            if (memThresholdBytes > this.updatesPerLink.size() * bytesPerLinkEstimate) {
                return;
            }
            Iterator<Entry<String, DocumentUpdateInfo>> li = this.updatesPerLink.entrySet()
                    .iterator();
            while (li.hasNext()) {
                Entry<String, DocumentUpdateInfo> e = li.next();
                // remove entries for services no longer attached / started on host
                if (getHost().getServiceStage(e.getKey()) == null) {
                    count++;
                    li.remove();
                }
            }
        }

        if (count == 0) {
            return;
        }
        //this.serviceRemovalDetectedTimeMicros = Utils.getNowMicrosUtc();

        logInfo("Cleared %d document update entries", count);
    }

    private void applyDocumentExpirationPolicy(long deadline) throws Exception {
/*
        Query versionQuery = LongPoint.newRangeQuery(
                ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS, 1L, Utils.getNowMicrosUtc());

        ScoreDoc after = null;
        Operation dummyDelete = null;
        boolean firstQuery = true;
        Map<String, Long> latestVersions = new HashMap<>();

        do {
            TopDocs results = s.searchAfter(after, versionQuery, expiredDocumentSearchThreshold,
                    this.versionSort, false, false);
            if (results.scoreDocs == null || results.scoreDocs.length == 0) {
                return;
            }

            after = results.scoreDocs[results.scoreDocs.length - 1];

            if (firstQuery && results.totalHits > expiredDocumentSearchThreshold) {
                adjustTimeSeriesStat(STAT_NAME_DOCUMENT_EXPIRATION_FORCED_MAINTENANCE_COUNT,
                        AGGREGATION_TYPE_SUM, 1);
            }

            firstQuery = false;

            PostgresDocumentStoredFieldVisitor visitor = new PostgresDocumentStoredFieldVisitor();
            for (ScoreDoc scoreDoc : results.scoreDocs) {
                loadDoc(s, visitor, scoreDoc.doc, this.fieldsToLoadNoExpand);
                String documentSelfLink = visitor.documentSelfLink;
                Long latestVersion = latestVersions.get(documentSelfLink);
                if (latestVersion == null) {
                    long searcherUpdateTime = getSearcherUpdateTime(s, 0);
                    latestVersion = getLatestVersion(s, searcherUpdateTime, documentSelfLink, 0,
                            -1);
                    latestVersions.put(documentSelfLink, latestVersion);
                }

                if (visitor.documentVersion < latestVersion) {
                    continue;
                }

                // update document with one that has all fields, including binary state
                augmentDoc(s, visitor, scoreDoc.doc, LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE);
                ServiceDocument serviceDocument = null;
                try {
                    serviceDocument = getStateFromLuceneDocument(visitor, documentSelfLink);
                } catch (Exception e) {
                    logWarning("Error deserializing state for %s: %s", documentSelfLink,
                            e.getMessage());
                }

                if (dummyDelete == null) {
                    dummyDelete = Operation.createDelete(null);
                }

                deleteAllDocumentsForSelfLink(dummyDelete, documentSelfLink, serviceDocument);

                adjustTimeSeriesStat(STAT_NAME_DOCUMENT_EXPIRATION_COUNT, AGGREGATION_TYPE_SUM, 1);
            }
        } while (Utils.getSystemNowMicrosUtc() < deadline);
        */
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
            boolean notify = false;
            if (activeTask.querySpec.options.contains(QueryOption.CONTINUOUS)) {
                notify = evaluateQuery(desc, filter, latestState);
            }
            if (!notify && activeTask.querySpec.options.contains(QueryOption.CONTINUOUS_STOP_MATCH)) {
                notify = evaluateQuery(desc, filter, getPreviousStateForDoc(activeTask, latestState));
            }
            if (!notify) {
                continue;
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

    private boolean evaluateQuery(ServiceDocumentDescription desc, QueryFilter filter, ServiceDocument serviceState) {
        if (serviceState == null) {
            return false;
        }
        if (desc == null) {
            if (!QueryFilterUtils.evaluate(filter, serviceState, getHost())) {
                return false;
            }
        } else if (!filter.evaluate(serviceState, desc)) {
            return false;
        }
        return true;
    }

    private ServiceDocument getPreviousStateForDoc(QueryTask activeTask, ServiceDocument latestState) {
        boolean hasPreviousVersion = latestState.documentVersion > 0;
        ServiceDocument previousState = null;
        try {
            if (hasPreviousVersion) {
                previousState = getDocumentAtVersion(latestState.documentSelfLink, latestState.documentVersion - 1);
            }
        } catch (Exception e) {
            logWarning("Exception getting previous state: %s", e.getMessage());
        }
        return previousState;
    }

}
