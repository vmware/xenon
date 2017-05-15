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

import static com.vmware.xenon.services.common.LuceneIndexDocumentHelper.GROUP_BY_PROPERTY_NAME_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexUpgrader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.QueryFilterUtils;
import com.vmware.xenon.common.ReflectionUtils;
import com.vmware.xenon.common.RoundRobinOperationQueue;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.MemoryLimitType;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.serialization.GsonSerializers;
import com.vmware.xenon.common.serialization.KryoSerializers;
import com.vmware.xenon.services.common.QueryFilter.QueryFilterException;
import com.vmware.xenon.services.common.QueryPageService.LuceneQueryPage;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryRuntimeContext;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.ServiceHostManagementService.BackupType;

public class LuceneDocumentIndexService extends StatelessService {

    public static final String SELF_LINK = ServiceUriPaths.CORE_DOCUMENT_INDEX;

    public static final String PROPERTY_NAME_QUERY_THREAD_COUNT = Utils.PROPERTY_NAME_PREFIX
            + LuceneDocumentIndexService.class.getSimpleName()
            + ".QUERY_THREAD_COUNT";

    public static final int QUERY_THREAD_COUNT = Integer.getInteger(
            PROPERTY_NAME_QUERY_THREAD_COUNT,
            Utils.DEFAULT_THREAD_COUNT * 2);

    public static final String PROPERTY_NAME_UPDATE_THREAD_COUNT = Utils.PROPERTY_NAME_PREFIX
            + LuceneDocumentIndexService.class.getSimpleName()
            + ".UPDATE_THREAD_COUNT";
    public static final int UPDATE_THREAD_COUNT = Integer.getInteger(
            PROPERTY_NAME_UPDATE_THREAD_COUNT,
            Utils.DEFAULT_THREAD_COUNT / 2);

    public static final String FILE_PATH_LUCENE = "lucene";

    public static final int DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH = 10000;

    public static final int DEFAULT_INDEX_SEARCHER_COUNT_THRESHOLD = 200;

    public static final int DEFAULT_QUERY_RESULT_LIMIT = 10000;

    public static final int DEFAULT_QUERY_PAGE_RESULT_LIMIT = 10000;

    public static final int DEFAULT_EXPIRED_DOCUMENT_SEARCH_THRESHOLD = 10000;

    private static final String DOCUMENTS_WITHOUT_RESULTS = "DocumentsWithoutResults";

    protected String indexDirectory;

    private static int expiredDocumentSearchThreshold = 1000;

    private static int indexFileCountThresholdForWriterRefresh = DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH;

    private static int versionRetentionBulkCleanupThreshold = 10000;

    private static int versionRetentionServiceThreshold = 100;

    private static int queryResultLimit = DEFAULT_QUERY_RESULT_LIMIT;

    private static int queryPageResultLimit = DEFAULT_QUERY_PAGE_RESULT_LIMIT;

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

    public static final String STAT_NAME_VERSION_CACHE_LOOKUP_COUNT = "versionCacheLookupCount";

    public static final String STAT_NAME_VERSION_CACHE_MISS_COUNT = "versionCacheMissCount";

    public static final String STAT_NAME_VERSION_CACHE_ENTRY_COUNT = "versionCacheEntryCount";

    public static final String STAT_NAME_MAINTENANCE_SEARCHER_REFRESH_DURATION_MICROS =
            "maintenanceSearcherRefreshDurationMicros";

    public static final String STAT_NAME_MAINTENANCE_DOCUMENT_EXPIRATION_DURATION_MICROS =
            "maintenanceDocumentExpirationDurationMicros";

    public static final String STAT_NAME_MAINTENANCE_VERSION_RETENTION_DURATION_MICROS =
            "maintenanceVersionRetentionDurationMicros";

    public static final String STAT_NAME_DOCUMENT_KIND_QUERY_COUNT_FORMAT = "documentKindQueryCount-%s";

    public static final String STAT_NAME_NON_DOCUMENT_KIND_QUERY_COUNT = "nonDocumentKindQueryCount";

    public static final String STAT_NAME_SINGLE_QUERY_BY_FACTORY_COUNT_FORMAT = "singleQueryByFactoryCount-%s";

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
    protected Object searchSync;

    /**
     * Synchronization object used to coordinate index writer update
     */
    protected final Semaphore writerSync = new Semaphore(
            UPDATE_THREAD_COUNT + QUERY_THREAD_COUNT);

    /**
     * Map of searchers per thread id. We do not use a ThreadLocal since we need visibility to this map
     * from the maintenance logic
     */
    protected Map<Long, IndexSearcher> searchers = new HashMap<>();

    private ThreadLocal<LuceneIndexDocumentHelper> indexDocumentHelper = new ThreadLocal<LuceneIndexDocumentHelper>() {
        @Override
        public LuceneIndexDocumentHelper initialValue() {
            return new LuceneIndexDocumentHelper();
        }
    };

    /**
     * Searcher refresh time, per searcher (using hash code)
     */
    protected Map<Integer, Long> searcherUpdateTimesMicros = new ConcurrentHashMap<>();

    /**
     * Searchers used for paginated query tasks.
     */
    protected TreeMap<Long, PaginatedSearcherInfo> paginatedSearchersByCreationTime = new TreeMap<>();
    protected TreeMap<Long, List<PaginatedSearcherInfo>> paginatedSearchersByExpirationTime = new TreeMap<>();

    protected IndexWriter writer = null;

    protected Map<String, QueryTask> activeQueries = new ConcurrentHashMap<>();

    private long writerUpdateTimeMicros;

    private long writerCreationTimeMicros;

    /**
     * Time when memory pressure removed {@link #updatesPerLink} entries.
     */
    private long serviceRemovalDetectedTimeMicros;

    private final Map<String, DocumentUpdateInfo> updatesPerLink = new HashMap<>();
    private final Map<String, Long> liveVersionsPerLink = new HashMap<>();
    private final Map<String, Long> immutableParentLinks = new HashMap<>();
    private final Map<String, Long> documentKindUpdateInfo = new HashMap<>();

    // memory pressure threshold in bytes
    long updateMapMemoryLimit;

    private Sort versionSort;

    private ExecutorService privateIndexingExecutor;

    private ExecutorService privateQueryExecutor;

    private Set<String> fieldToLoadVersionLookup;
    private Set<String> fieldsToLoadNoExpand;
    private Set<String> fieldsToLoadWithExpand;

    private RoundRobinOperationQueue queryQueue = RoundRobinOperationQueue.create();
    private RoundRobinOperationQueue updateQueue = RoundRobinOperationQueue.create();

    private URI uri;

    public static class DocumentUpdateInfo {
        public long updateTimeMicros;
        public long version;
    }

    public static class PaginatedSearcherInfo {
        public long creationTimeMicros;
        public long expirationTimeMicros;
        public boolean singleUse;
        public IndexSearcher searcher;
    }

    public static class DeleteQueryRuntimeContextRequest extends ServiceDocument {
        public QueryRuntimeContext context;
        static final String KIND = Utils.buildKind(DeleteQueryRuntimeContextRequest.class);
    }

    /**
     * NOTE: use backup API in ServiceHostManagementService instead of this class.
     **/
    public static class BackupRequest extends ServiceDocument {
        static final String KIND = Utils.buildKind(BackupRequest.class);
    }

    public static class BackupResponse extends ServiceDocument {
        public URI backupFile;
        static final String KIND = Utils.buildKind(BackupResponse.class);
    }

    /**
     * NOTE: use restore API in ServiceHostManagementService instead of this class.
     **/
    public static class RestoreRequest extends ServiceDocument {
        public URI backupFile;
        public Long timeSnapshotBoundaryMicros;
        static final String KIND = Utils.buildKind(RestoreRequest.class);
    }

    public static class MaintenanceRequest {
        static final String KIND = Utils.buildKind(MaintenanceRequest.class);
    }

    public LuceneDocumentIndexService() {
        this(FILE_PATH_LUCENE);
    }

    public LuceneDocumentIndexService(String indexDirectory) {
        super(ServiceDocument.class);
        super.toggleOption(ServiceOption.CORE, true);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        this.indexDirectory = indexDirectory;
    }

    private boolean isDurable() {
        return this.indexDirectory != null;
    }

    @Override
    public void handleStart(final Operation post) {
        super.setMaintenanceIntervalMicros(getHost().getMaintenanceIntervalMicros() * 5);
        // index service getUri() will be invoked on every load and save call for every operation,
        // so its worth caching (plus we only have a very small number of index services
        this.uri = super.getUri();

        this.privateQueryExecutor = Executors.newFixedThreadPool(QUERY_THREAD_COUNT,
                r -> new Thread(r, getUri() + "/queries/" + Utils.getSystemNowMicrosUtc()));
        this.privateIndexingExecutor = Executors.newFixedThreadPool(UPDATE_THREAD_COUNT,
                r -> new Thread(r, getSelfLink() + "/updates/" + Utils.getSystemNowMicrosUtc()));

        initializeInstance();

        if (isDurable()) {
            // create durable index writer
            File directory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory);
            for (int retryCount = 0; retryCount < 2; retryCount++) {
                try {
                    createWriter(directory, true);
                    // we do not actually know if the index is OK, until we try to query
                    doSelfValidationQuery();
                    if (retryCount == 1) {
                        logInfo("Retry to create index writer was successful");
                    }
                    break;
                } catch (Throwable e) {
                    adjustStat(STAT_NAME_INDEX_LOAD_RETRY_COUNT, 1);
                    if (retryCount < 1) {
                        logWarning("Failure creating index writer: %s, will retry",
                                Utils.toString(e));
                        close(this.writer);
                        archiveCorruptIndexFiles(directory);
                        continue;
                    }
                    logWarning("Failure creating index writer: %s", Utils.toString(e));
                    post.fail(e);
                    return;
                }
            }
        } else {
            // create RAM based index writer
            try {
                createWriter(null, false);
            } catch (Throwable e) {
                logSevere(e);
                post.fail(e);
                return;
            }
        }

        initializeStats();

        post.complete();
    }

    private void initializeInstance() {
        this.searchSync = new Object();
        this.liveVersionsPerLink.clear();
        this.updatesPerLink.clear();
        this.searcherUpdateTimesMicros.clear();
        this.paginatedSearchersByCreationTime.clear();
        this.paginatedSearchersByExpirationTime.clear();
        this.versionSort = new Sort(new SortedNumericSortField(ServiceDocument.FIELD_NAME_VERSION,
                SortField.Type.LONG, true));

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

    private void initializeStats() {
        IndexWriter w = this.writer;
        setTimeSeriesStat(STAT_NAME_INDEXED_DOCUMENT_COUNT, AGGREGATION_TYPE_SUM,
                w != null ? w.numDocs() : 0);
        // simple estimate on field count, just so our first bin does not have a completely bogus
        // number
        setTimeSeriesStat(STAT_NAME_INDEXED_FIELD_COUNT, AGGREGATION_TYPE_SUM,
                w != null ? w.numDocs() * 10 : 0);
    }

    private void setTimeSeriesStat(String name, EnumSet<AggregationType> type, double v) {
        if (!this.hasOption(ServiceOption.INSTRUMENTATION)) {
            return;
        }
        ServiceStat dayStat = getTimeSeriesStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_DAY,
                (int) TimeUnit.DAYS.toHours(1), TimeUnit.HOURS.toMillis(1), type);
        this.setStat(dayStat, v);
        ServiceStat hourStat = getTimeSeriesStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR,
                (int) TimeUnit.HOURS.toMinutes(1), TimeUnit.MINUTES.toMillis(1), type);
        this.setStat(hourStat, v);
    }

    private void adjustTimeSeriesStat(String name, EnumSet<AggregationType> type, double delta) {
        if (!this.hasOption(ServiceOption.INSTRUMENTATION)) {
            return;
        }
        ServiceStat dayStat = getTimeSeriesStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_DAY,
                (int) TimeUnit.DAYS.toHours(1), TimeUnit.HOURS.toMillis(1), type);
        this.adjustStat(dayStat, delta);
        ServiceStat hourStat = getTimeSeriesStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR,
                (int) TimeUnit.HOURS.toMinutes(1), TimeUnit.MINUTES.toMillis(1), type);
        this.adjustStat(hourStat, delta);
    }

    private void setTimeSeriesHistogramStat(String name, EnumSet<AggregationType> type, double v) {
        if (!this.hasOption(ServiceOption.INSTRUMENTATION)) {
            return;
        }
        ServiceStat dayStat = getTimeSeriesHistogramStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_DAY,
                (int) TimeUnit.DAYS.toHours(1), TimeUnit.HOURS.toMillis(1), type);
        this.setStat(dayStat, v);
        ServiceStat hourStat = getTimeSeriesHistogramStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR,
                (int) TimeUnit.HOURS.toMinutes(1), TimeUnit.MINUTES.toMillis(1), type);
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

    public IndexWriter createWriter(File directory, boolean doUpgrade) throws Exception {
        Directory luceneDirectory = directory != null ? MMapDirectory.open(directory.toPath())
                : new RAMDirectory();
        return createWriterWithLuceneDirectory(luceneDirectory, doUpgrade);
    }

    IndexWriter createWriterWithLuceneDirectory(Directory dir, boolean doUpgrade) throws Exception {
        Analyzer analyzer = new SimpleAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        Long totalMBs = getHost().getServiceMemoryLimitMB(getSelfLink(), MemoryLimitType.EXACT);
        if (totalMBs != null) {
            long cacheSizeMB = (totalMBs * 99) / 100;
            cacheSizeMB = Math.max(1, cacheSizeMB);
            iwc.setRAMBufferSizeMB(cacheSizeMB);
            // reserve 1% of service memory budget for version cache
            long memoryLimitMB = Math.max(1, totalMBs / 100);
            this.updateMapMemoryLimit = memoryLimitMB * 1024 * 1024;
        }


        // Upgrade the index in place if necessary.
        if (doUpgrade && DirectoryReader.indexExists(dir)) {
            upgradeIndex(dir);
        }

        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        iwc.setIndexDeletionPolicy(new SnapshotDeletionPolicy(
                new KeepOnlyLastCommitDeletionPolicy()));

        IndexWriter w = new IndexWriter(dir, iwc);
        w.commit();

        synchronized (this.searchSync) {
            this.writer = w;
            this.updatesPerLink.clear();
            this.writerUpdateTimeMicros = Utils.getNowMicrosUtc();
            this.writerCreationTimeMicros = this.writerUpdateTimeMicros;
        }
        return this.writer;
    }

    private void upgradeIndex(Directory dir) throws IOException {
        boolean doUpgrade = false;

        String lastSegmentsFile = SegmentInfos.getLastCommitSegmentsFileName(dir.listAll());
        SegmentInfos sis = SegmentInfos.readCommit(dir, lastSegmentsFile);
        for (SegmentCommitInfo commit : sis) {
            if (!commit.info.getVersion().equals(Version.LATEST)) {
                logInfo("Found Index version %s", commit.info.getVersion().toString());
                doUpgrade = true;
                break;
            }
        }

        if (doUpgrade) {
            logInfo("Upgrading index to %s", Version.LATEST.toString());
            IndexWriterConfig iwc = new IndexWriterConfig(null);
            new IndexUpgrader(dir, iwc, false).upgrade();
            this.writerUpdateTimeMicros = Utils.getNowMicrosUtc();
        }
    }

    void archiveCorruptIndexFiles(File directory) {
        File newDirectory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory
                + "." + Utils.getNowMicrosUtc());
        try {
            logWarning("Archiving corrupt index files to %s", newDirectory.toPath());
            Files.createDirectory(newDirectory.toPath());
            // we assume a flat directory structure for the LUCENE directory
            FileUtils.moveOrDeleteFiles(directory, newDirectory, false);

        } catch (IOException e) {
            logWarning(e.toString());
        }
    }

    /**
     * Issues a query to verify index is healthy
     */
    private void doSelfValidationQuery() throws Throwable {
        TermQuery tq = new TermQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK, getSelfLink()));
        ServiceDocumentQueryResult rsp = new ServiceDocumentQueryResult();

        Operation op = Operation.createGet(getUri());
        EnumSet<QueryOption> options = EnumSet.of(QueryOption.INCLUDE_ALL_VERSIONS);
        IndexSearcher s = new IndexSearcher(DirectoryReader.open(this.writer, true, true));
        queryIndexPaginated(op, options, s, tq, null, Integer.MAX_VALUE, 0, null, rsp, null,
                Utils.getNowMicrosUtc());
    }

    private void handleDeleteRuntimeContext(Operation op) throws Throwable {
        DeleteQueryRuntimeContextRequest request = (DeleteQueryRuntimeContextRequest)
                op.getBodyRaw();
        if (request.context == null) {
            throw new IllegalArgumentException("Context cannot be null");
        }

        IndexSearcher nativeSearcher = (IndexSearcher) request.context.nativeSearcher;
        if (nativeSearcher == null) {
            throw new IllegalArgumentException("Native searcher must be present");
        }

        PaginatedSearcherInfo infoToRemove;
        synchronized (this.searchSync) {
            infoToRemove = removeSearcherInfoUnsafe(nativeSearcher);
        }

        if (infoToRemove == null) {
            op.complete();
            return;
        }

        try {
            infoToRemove.searcher.getIndexReader().close();
        } catch (Throwable ignored) {
        }

        op.complete();

        adjustTimeSeriesStat(STAT_NAME_PAGINATED_SEARCHER_FORCE_DELETION_COUNT,
                AGGREGATION_TYPE_SUM, 1);
    }

    private PaginatedSearcherInfo removeSearcherInfoUnsafe(IndexSearcher searcher) {
        PaginatedSearcherInfo infoToRemove = null;
        Iterator<Entry<Long, PaginatedSearcherInfo>> itr =
                this.paginatedSearchersByCreationTime.entrySet().iterator();
        while (itr.hasNext()) {
            PaginatedSearcherInfo info = itr.next().getValue();
            if (info.searcher.equals(searcher)) {
                if (!info.singleUse) {
                    throw new IllegalStateException(
                            "Context deletion is supported only for SINGLE_USE queries");
                }

                infoToRemove = info;
                itr.remove();
                break;
            }
        }

        if (infoToRemove == null) {
            return null;
        }

        long expirationTime = infoToRemove.expirationTimeMicros;
        List<PaginatedSearcherInfo> expirationList =
                this.paginatedSearchersByExpirationTime.get(expirationTime);
        expirationList.remove(infoToRemove);
        if (expirationList.isEmpty()) {
            this.paginatedSearchersByExpirationTime.remove(expirationTime);
        }

        this.searcherUpdateTimesMicros.remove(infoToRemove.searcher.hashCode());
        return infoToRemove;
    }

    private void handleBackup(Operation op) throws Throwable {

        if (!isDurable()) {
            op.fail(new IllegalStateException("Index service is not durable"));
            return;
        }

        // Delegate to LuceneDocumentIndexBackupService
        logWarning("Please use backup feature from %s.", ServiceHostManagementService.class);

        String outFileName = this.indexDirectory + "-" + Utils.getNowMicrosUtc();
        Path zipFilePath = Files.createTempFile(outFileName, ".zip");

        ServiceHostManagementService.BackupRequest backupRequest = new ServiceHostManagementService.BackupRequest();
        backupRequest.kind = ServiceHostManagementService.BackupRequest.KIND;
        backupRequest.backupType = BackupType.ZIP;
        backupRequest.destination = zipFilePath.toUri();

        // delegate backup to backup service
        Operation patch = Operation.createPatch(this, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP)
                .transferRequestHeadersFrom(op)
                .transferRefererFrom(op)
                .setBody(backupRequest)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }

                    BackupResponse response = new BackupResponse();
                    response.backupFile = backupRequest.destination;

                    op.transferResponseHeadersFrom(o);
                    op.setBodyNoCloning(response);
                    op.complete();
                });

        sendRequest(patch);
    }

    private void handleRestore(Operation op) {
        if (!isDurable()) {
            op.fail(new IllegalStateException("Index service is not durable"));
            return;
        }

        // Delegate to LuceneDocumentIndexBackupService
        logWarning("Please use restore feature from %s.", ServiceHostManagementService.class);

        RestoreRequest req = op.getBody(RestoreRequest.class);

        ServiceHostManagementService.RestoreRequest restoreRequest = new ServiceHostManagementService.RestoreRequest();
        restoreRequest.kind = ServiceHostManagementService.RestoreRequest.KIND;
        restoreRequest.destination = req.backupFile;
        restoreRequest.timeSnapshotBoundaryMicros = req.timeSnapshotBoundaryMicros;

        // delegate restore to backup service
        Operation patch = Operation.createPatch(this, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP)
                .transferRequestHeadersFrom(op)
                .transferRefererFrom(op)
                .setBody(restoreRequest)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    op.transferResponseHeadersFrom(o);
                    op.complete();
                });

        sendRequest(patch);
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
                    this.privateQueryExecutor.execute(this::handleQueryRequest);
                }
            } else {
                if (offerUpdateOperation(op)) {
                    this.privateIndexingExecutor.execute(this::handleUpdateRequest);
                }
            }
        } catch (RejectedExecutionException e) {
            op.fail(e);
        }
    }

    private void handleQueryRequest() {

        Operation op = pollQueryOperation();
        try {
            this.writerSync.acquire();
            while (op != null) {
                switch (op.getAction()) {
                case GET:
                    handleGetImpl(op);
                    break;
                case PATCH:
                    ServiceDocument sd = (ServiceDocument) op.getBodyRaw();
                    if (sd.documentKind != null) {
                        if (sd.documentKind.equals(QueryTask.KIND)) {
                            QueryTask task = (QueryTask) op.getBodyRaw();
                            handleQueryTaskPatch(op, task);
                            break;
                        }
                        if (sd.documentKind.equals(DeleteQueryRuntimeContextRequest.KIND)) {
                            handleDeleteRuntimeContext(op);
                            break;
                        }
                        if (sd.documentKind.equals(BackupRequest.KIND)) {
                            handleBackup(op);
                            break;
                        }
                        if (sd.documentKind.equals(RestoreRequest.KIND)) {
                            handleRestore(op);
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
        } catch (Throwable e) {
            checkFailureAndRecover(e);
            if (op != null) {
                op.fail(e);
            }
        } finally {
            this.writerSync.release();
        }
    }

    private void handleUpdateRequest() {
        Operation op = pollUpdateOperation();
        try {
            this.writerSync.acquire();
            while (op != null) {
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
        } catch (Throwable e) {
            checkFailureAndRecover(e);
            if (op != null) {
                op.fail(e);
            }
        } finally {
            this.writerSync.release();
        }
    }

    private void handleQueryTaskPatch(Operation op, QueryTask task) throws Throwable {
        QueryTask.QuerySpecification qs = task.querySpec;

        Query luceneQuery = (Query) qs.context.nativeQuery;
        Sort luceneSort = (Sort) qs.context.nativeSort;

        if (luceneQuery == null) {
            luceneQuery = LuceneQueryConverter.convertToLuceneQuery(task.querySpec.query,
                    qs.context);
            if (qs.options.contains(QueryOption.TIME_SNAPSHOT)) {
                Query latestDocumentClause = LongPoint.newRangeQuery(
                        ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS, 0,
                        qs.timeSnapshotBoundaryMicros);
                luceneQuery = new BooleanQuery.Builder()
                        .add(latestDocumentClause, Occur.MUST)
                        .add(luceneQuery, Occur.FILTER).build();
            }
            qs.context.nativeQuery = luceneQuery;
        }

        if (luceneSort == null && task.querySpec.options != null
                && task.querySpec.options.contains(QuerySpecification.QueryOption.SORT)) {
            luceneSort = LuceneQueryConverter.convertToLuceneSort(task.querySpec, false);
            task.querySpec.context.nativeSort = luceneSort;
        }

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

        LuceneQueryPage lucenePage = (LuceneQueryPage) qs.context.nativePage;
        IndexSearcher s = (IndexSearcher) qs.context.nativeSearcher;
        ServiceDocumentQueryResult rsp = new ServiceDocumentQueryResult();

        if (s == null && qs.resultLimit != null && qs.resultLimit > 0
                && qs.resultLimit != Integer.MAX_VALUE
                && !qs.options.contains(QueryOption.TOP_RESULTS)) {
            // this is a paginated query. If this is the start of the query, create a dedicated searcher
            // for this query and all its pages. It will be expired when the query task itself expires
            Set<String> documentKind = qs.context.kindScope;
            s = createOrUpdatePaginatedQuerySearcher(task.documentExpirationTimeMicros,
                    this.writer, documentKind, qs.options);
        }

        if (!queryIndex(s, op, null, qs.options, luceneQuery, lucenePage,
                qs.resultLimit,
                task.documentExpirationTimeMicros, task.indexLink, rsp, qs)) {
            op.setBodyNoCloning(rsp).complete();
        }
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

    private IndexSearcher createOrUpdatePaginatedQuerySearcher(long expirationMicros,
            IndexWriter w, Set<String> kindScope, EnumSet<QueryOption> queryOptions)
            throws IOException {

        boolean doNotRefresh = queryOptions.contains(QueryOption.DO_NOT_REFRESH);
        boolean singleUse = queryOptions.contains(QueryOption.SINGLE_USE);
        if (singleUse || (!doNotRefresh && kindScope == null)) {
            return createPaginatedQuerySearcher(expirationMicros, w, singleUse);
        }

        IndexSearcher searcher;
        synchronized (this.searchSync) {
            // Pass documentKind only when doNotRefresh is false.
            if (doNotRefresh) {
                searcher = getOrUpdateExistingSearcher(expirationMicros, null);
            } else {
                searcher = getOrUpdateExistingSearcher(expirationMicros, kindScope);
            }
        }

        if (searcher != null) {
            return searcher;
        }

        return createPaginatedQuerySearcher(expirationMicros, w, false);
    }

    private IndexSearcher getOrUpdateExistingSearcher(long newExpirationMicros,
            Set<String> kindScope) {

        if (this.paginatedSearchersByCreationTime.isEmpty()) {
            return null;
        }

        PaginatedSearcherInfo info = null;
        Iterator<Entry<Long, PaginatedSearcherInfo>> itr =
                this.paginatedSearchersByCreationTime.descendingMap().entrySet().iterator();
        while (itr.hasNext()) {
            PaginatedSearcherInfo i = itr.next().getValue();
            if (!i.singleUse) {
                info = i;
                break;
            }
        }

        if (info == null) {
            return null;
        }

        if (kindScope != null) {
            long searcherUpdateTime = this.searcherUpdateTimesMicros.get(info.searcher.hashCode());
            if (documentNeedsNewSearcher(null, kindScope, -1, searcherUpdateTime)) {
                return null;
            }
        }

        long currentExpirationMicros = info.expirationTimeMicros;
        if (newExpirationMicros <= currentExpirationMicros) {
            return info.searcher;
        }

        List<PaginatedSearcherInfo> expirationList = this.paginatedSearchersByExpirationTime.get(
                currentExpirationMicros);
        if (expirationList == null || !expirationList.contains(info)) {
            throw new IllegalStateException("Searcher not found in expiration list");
        }

        expirationList.remove(info);
        if (expirationList.isEmpty()) {
            this.paginatedSearchersByExpirationTime.remove(currentExpirationMicros);
        }

        info.expirationTimeMicros = newExpirationMicros;

        expirationList = this.paginatedSearchersByExpirationTime.computeIfAbsent(
                newExpirationMicros, (k) -> new ArrayList<>());
        expirationList.add(info);

        return info.searcher;
    }

    private IndexSearcher createPaginatedQuerySearcher(long expirationMicros, IndexWriter w,
            boolean singleUse) throws IOException {
        if (w == null) {
            throw new IllegalStateException("Writer not available");
        }

        adjustTimeSeriesStat(STAT_NAME_PAGINATED_SEARCHER_UPDATE_COUNT, AGGREGATION_TYPE_SUM, 1);

        long now = Utils.getNowMicrosUtc();

        IndexSearcher s = new IndexSearcher(DirectoryReader.open(w, true, true));

        PaginatedSearcherInfo info = new PaginatedSearcherInfo();
        info.creationTimeMicros = now;
        info.expirationTimeMicros = expirationMicros;
        info.singleUse = singleUse;
        info.searcher = s;

        synchronized (this.searchSync) {
            this.paginatedSearchersByCreationTime.put(info.creationTimeMicros, info);
            List<PaginatedSearcherInfo> expirationList = this.paginatedSearchersByExpirationTime
                    .computeIfAbsent(info.expirationTimeMicros, (k) -> new ArrayList<>());
            expirationList.add(info);
            this.searcherUpdateTimesMicros.put(s.hashCode(), now);
        }

        return s;
    }

    public void handleGetImpl(Operation get) throws Throwable {
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
            // Most basic query is retrieving latest document at latest version for a specific link
            queryIndexSingle(selfLink, get, version);
            return;
        }

        // Self link prefix query, returns all self links with the same prefix. A GET on a
        // factory translates to this query.
        int resultLimit = Integer.MAX_VALUE;
        selfLink = selfLink.substring(0, selfLink.length() - 1);
        Query tq = new PrefixQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK, selfLink));

        ServiceDocumentQueryResult rsp = new ServiceDocumentQueryResult();
        rsp.documentLinks = new ArrayList<>();
        if (queryIndex(null, get, selfLink, options, tq, null, resultLimit, 0, null, rsp,
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
            IndexSearcher s,
            Operation op,
            String selfLinkPrefix,
            EnumSet<QueryOption> options,
            Query tq,
            LuceneQueryPage page,
            int count,
            long expiration,
            String indexLink,
            ServiceDocumentQueryResult rsp,
            QuerySpecification qs) throws Throwable {
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

        IndexWriter w = this.writer;
        if (w == null) {
            op.fail(new CancellationException());
            return true;
        }

        Set<String> kindScope = null;

        if (qs != null && qs.context != null) {
            kindScope = qs.context.kindScope;
        }

        if (s == null) {
            s = createOrRefreshSearcher(selfLinkPrefix, kindScope, count, w,
                    options.contains(QueryOption.DO_NOT_REFRESH));
        }

        long queryStartTimeMicros = Utils.getNowMicrosUtc();
        tq = updateQuery(op, tq, queryStartTimeMicros);
        if (tq == null) {
            return false;
        }

        if (qs != null && qs.query != null && this.hasOption(ServiceOption.INSTRUMENTATION)) {
            String queryStat = getQueryStatName(qs.query);
            this.adjustStat(queryStat, 1);
        }

        ServiceDocumentQueryResult result;
        if (options.contains(QueryOption.COUNT)) {
            result = queryIndexCount(options, s, tq, rsp, qs, queryStartTimeMicros);
        } else {
            result = queryIndexPaginated(op, options, s, tq, page, count, expiration, indexLink,
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
            throws Throwable {
        IndexWriter w = this.writer;
        if (w == null) {
            op.fail(new CancellationException());
            return;
        }

        IndexSearcher s = createOrRefreshSearcher(selfLink, null, 1, w, false);

        long startNanos = System.nanoTime();
        TopDocs hits = queryIndexForVersion(selfLink, s, version, null);
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
        if (hits.totalHits == 0) {
            op.complete();
            return;
        }

        DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
        loadDoc(s, visitor, hits.scoreDocs[0].doc, this.fieldsToLoadWithExpand);

        boolean hasExpired = false;

        Long expiration = visitor.documentExpirationTimeMicros;
        if (expiration != null) {
            hasExpired = expiration <= Utils.getSystemNowMicrosUtc();
        }

        if (hasExpired) {
            op.complete();
            return;
        }

        ServiceDocument sd = getStateFromLuceneDocument(visitor, selfLink);
        op.setBodyNoCloning(sd).complete();
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
    private TopDocs queryIndexForVersion(String selfLink, IndexSearcher s, Long version, Long documentsUpdatedBeforeInMicros)
            throws IOException {
        Query tqSelfLink = new TermQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK, selfLink));

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(tqSelfLink, Occur.MUST);

        // when QueryOption.TIME_SNAPSHOT  is enabled (documentsUpdatedBeforeInMicros i.e. QuerySpecification.timeSnapshotBoundaryMicros is present)
        // perform query to find a document with link updated before supplied time.
        if (documentsUpdatedBeforeInMicros != null) {
            Query documentsUpdatedBeforeInMicrosQuery = LongPoint.newRangeQuery(
                    ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS, 0, documentsUpdatedBeforeInMicros);
            builder.add(documentsUpdatedBeforeInMicrosQuery, Occur.MUST);
        } else if (version != null) {
            Query versionQuery = LongPoint.newRangeQuery(
                    ServiceDocument.FIELD_NAME_VERSION, version, version);
            builder.add(versionQuery, Occur.MUST);
        }

        TopDocs hits = s.search(builder.build(), 1, this.versionSort, false, false);
        return hits;
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
    private Query updateQuery(Operation op, Query tq, long now) {
        Query expirationClause = LongPoint.newRangeQuery(
                ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS, 1, now);
        BooleanQuery.Builder builder = new BooleanQuery.Builder()
                .add(expirationClause, Occur.MUST_NOT)
                .add(tq, Occur.FILTER);
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
        Query rq = null;
        if (resourceQuery == null) {
            rq = new MatchNoDocsQuery();
        } else {
            rq = LuceneQueryConverter.convertToLuceneQuery(resourceQuery, null);
        }

        builder.add(rq, Occur.FILTER);
        return builder.build();
    }

    private void handleGroupByQueryTaskPatch(Operation op, QueryTask task) throws IOException {
        QuerySpecification qs = task.querySpec;
        IndexSearcher s = (IndexSearcher) qs.context.nativeSearcher;
        LuceneQueryPage page = (LuceneQueryPage) qs.context.nativePage;
        Query tq = (Query) qs.context.nativeQuery;
        Sort sort = (Sort) qs.context.nativeSort;
        if (sort == null && qs.sortTerm != null) {
            sort = LuceneQueryConverter.convertToLuceneSort(qs, false);
        }

        Sort groupSort = null;
        if (qs.groupSortTerm != null) {
            groupSort = LuceneQueryConverter.convertToLuceneSort(qs, true);
        }

        GroupingSearch groupingSearch;
        if (qs.groupByTerm.propertyType == ServiceDocumentDescription.TypeName.LONG ||
                qs.groupByTerm.propertyType == ServiceDocumentDescription.TypeName.DOUBLE) {
            groupingSearch = new GroupingSearch(qs.groupByTerm.propertyName + GROUP_BY_PROPERTY_NAME_SUFFIX);
        } else {
            groupingSearch = new GroupingSearch(
                    LuceneIndexDocumentHelper.createSortFieldPropertyName(qs.groupByTerm.propertyName));
        }

        groupingSearch.setGroupSort(groupSort);
        groupingSearch.setSortWithinGroup(sort);

        adjustTimeSeriesStat(STAT_NAME_GROUP_QUERY_COUNT, AGGREGATION_TYPE_SUM, 1);

        int groupOffset = page != null ? page.groupOffset : 0;
        int groupLimit = qs.groupResultLimit != null ? qs.groupResultLimit : 10000;

        Set<String> kindScope = qs.context.kindScope;

        if (s == null && qs.groupResultLimit != null) {
            s = createOrUpdatePaginatedQuerySearcher(task.documentExpirationTimeMicros,
                    this.writer, kindScope, qs.options);
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
            Query lucenePerGroupQuery = LuceneQueryConverter.convertToLuceneQuery(perGroupQuery,
                    qs.context);

            // for each group generate a query page link
            String pageLink = createNextPage(op, s, qs, lucenePerGroupQuery, sort,
                    null, null,
                    task.documentExpirationTimeMicros, task.indexLink, false);

            rsp.nextPageLinksPerGroup.put(groupValue, pageLink);
        }

        if (qs.groupResultLimit != null && groups.groups.length >= groupLimit) {
            // check if we need to generate a next page for the next set of group results
            groups = groupingSearch.search(s, tq, groupLimit + groupOffset, groupLimit);
            if (groups.totalGroupedHitCount > 0) {
                rsp.nextPageLink = createNextPage(op, s, qs, tq, sort,
                        null, groupLimit + groupOffset,
                        task.documentExpirationTimeMicros, task.indexLink, page != null);
            }
        }

        op.setBodyNoCloning(rsp).complete();
    }

    private ServiceDocumentQueryResult queryIndexCount(
            EnumSet<QueryOption> queryOptions,
            IndexSearcher searcher,
            Query termQuery,
            ServiceDocumentQueryResult response,
            QuerySpecification querySpec,
            long queryStartTimeMicros)
            throws Throwable {

        if (queryOptions.contains(QueryOption.INCLUDE_ALL_VERSIONS)) {
            // Special handling for queries which include all versions in order to avoid allocating
            // a large, unnecessary ScoreDocs array.
            response.documentCount = (long) searcher.count(termQuery);
            long queryTimeMicros = Utils.getNowMicrosUtc() - queryStartTimeMicros;
            response.queryTimeMicros = queryTimeMicros;
            setTimeSeriesHistogramStat(STAT_NAME_QUERY_ALL_VERSIONS_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX, queryTimeMicros);
            return response;
        }

        response.queryTimeMicros = 0L;
        TopDocs results = null;
        ScoreDoc after = null;
        long start = queryStartTimeMicros;
        int resultLimit = queryResultLimit;
        if (querySpec.resultLimit != null && querySpec.resultLimit != Integer.MAX_VALUE) {
            resultLimit = querySpec.resultLimit;
            if (querySpec.resultLimit < DEFAULT_QUERY_RESULT_LIMIT) {
                logWarning("\n*****\n"
                        + "resultLimit value is too low, query will take much longer on large result sets."
                        + "Do not set this value for COUNT queries, or set it above default of "
                        + DEFAULT_QUERY_RESULT_LIMIT
                        + "\n*****\n");
            }
        }
        do {
            results = searcher.searchAfter(after, termQuery, resultLimit);
            long queryEndTimeMicros = Utils.getNowMicrosUtc();
            long luceneQueryDurationMicros = queryEndTimeMicros - start;
            long queryDurationMicros = queryEndTimeMicros - queryStartTimeMicros;
            response.queryTimeMicros = queryDurationMicros;

            if (results == null || results.scoreDocs == null || results.scoreDocs.length == 0) {
                break;
            }

            setTimeSeriesHistogramStat(STAT_NAME_QUERY_ALL_VERSIONS_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX, luceneQueryDurationMicros);

            after = processQueryResults(querySpec, queryOptions, resultLimit, searcher,
                    response,
                    results.scoreDocs, start);

            long now = Utils.getNowMicrosUtc();
            setTimeSeriesHistogramStat(STAT_NAME_RESULT_PROCESSING_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX, now - queryEndTimeMicros);

            start = now;
        } while (true);

        response.documentLinks.clear();
        return response;
    }

    private ServiceDocumentQueryResult queryIndexPaginated(Operation op,
            EnumSet<QueryOption> options,
            IndexSearcher s,
            Query tq,
            LuceneQueryPage page,
            int count,
            long expiration,
            String indexLink,
            ServiceDocumentQueryResult rsp,
            QuerySpecification qs,
            long queryStartTimeMicros) throws Throwable {
        ScoreDoc[] hits;
        ScoreDoc after = null;
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
            // the number of desired results in the QueryTask, not the expected number of Lucene
            // documents which must be processed in order to generate these results. Adjust the
            // Lucene query page size to account for this discrepancy.
            hitCount = Math.max(resultLimit, queryPageResultLimit);
        } else {
            hitCount = resultLimit;
        }

        if (hasPage) {
            // For example, via GET of QueryTask.nextPageLink
            after = page.after;
            rsp.prevPageLink = page.previousPageLink;
        }

        Sort sort = this.versionSort;
        if (qs != null && qs.sortTerm != null) {
            // see if query is part of a task and already has a cached sort
            if (qs.context != null) {
                sort = (Sort) qs.context.nativeSort;
            }

            if (sort == null) {
                sort = LuceneQueryConverter.convertToLuceneSort(qs, false);
            }
        }

        TopDocs results = null;
        int queryCount = 0;
        rsp.queryTimeMicros = 0L;
        long start = queryStartTimeMicros;

        do {
            // Special-case handling of single-version documents to use search() instead of
            // searchAfter(). This will prevent Lucene from holding the full result set in memory.
            if (useDirectSearch) {
                if (sort == null) {
                    results = s.search(tq, hitCount);
                } else {
                    results = s.search(tq, hitCount, sort, false, false);
                }
            } else {
                if (sort == null) {
                    results = s.searchAfter(after, tq, hitCount);
                } else {
                    results = s.searchAfter(after, tq, hitCount, sort, false, false);
                }
            }

            if (results == null) {
                return rsp;
            }

            queryCount++;
            long end = Utils.getNowMicrosUtc();

            if (!hasExplicitLimit && !hasPage && !isPaginatedQuery
                    && results.totalHits > hitCount) {
                throw new IllegalStateException(
                        "Query returned large number of results, please specify a resultLimit. Results:"
                                + results.totalHits + ", QuerySpec: " + Utils.toJson(qs));
            }

            hits = results.scoreDocs;

            long queryTime = end - start;

            rsp.documentCount = 0L;
            rsp.queryTimeMicros += queryTime;
            ScoreDoc bottom = null;
            if (shouldProcessResults) {
                start = end;
                bottom = processQueryResults(qs, options, count, s, rsp, hits,
                        queryStartTimeMicros);
                end = Utils.getNowMicrosUtc();

                if (hasOption(ServiceOption.INSTRUMENTATION)) {
                    String statName = options.contains(QueryOption.INCLUDE_ALL_VERSIONS)
                            ? STAT_NAME_QUERY_ALL_VERSIONS_DURATION_MICROS
                            : STAT_NAME_QUERY_DURATION_MICROS;
                    setTimeSeriesHistogramStat(statName, AGGREGATION_TYPE_AVG_MAX, queryTime);
                    setTimeSeriesHistogramStat(STAT_NAME_RESULT_PROCESSING_DURATION_MICROS,
                            AGGREGATION_TYPE_AVG_MAX, end - start);
                }
            }

            if (count == Integer.MAX_VALUE || useDirectSearch) {
                // single pass
                break;
            }

            if (hits.length == 0) {
                break;
            }

            if (isPaginatedQuery) {
                if (!hasPage) {
                    bottom = null;
                }

                if (!hasPage || rsp.documentLinks.size() >= count
                        || hits.length < resultLimit) {
                    // query had less results then per page limit or page is full of results

                    boolean createNextPageLink = true;
                    if (hasPage) {
                        createNextPageLink = checkNextPageHasEntry(bottom, options, s,
                                tq, sort, hitCount, qs, queryStartTimeMicros);
                    }

                    if (createNextPageLink) {
                        rsp.nextPageLink = createNextPage(op, s, qs, tq, sort, bottom,
                                null, expiration, indexLink, hasPage);
                    }
                    break;
                }
            }

            after = bottom;
            resultLimit = count - rsp.documentLinks.size();
        } while (resultLimit > 0);

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            ServiceStat st = getHistogramStat(STAT_NAME_ITERATIONS_PER_QUERY);
            setStat(st, queryCount);
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
    private boolean checkNextPageHasEntry(ScoreDoc after,
            EnumSet<QueryOption> options,
            IndexSearcher s,
            Query tq,
            Sort sort,
            int count,
            QuerySpecification qs,
            long queryStartTimeMicros) throws Throwable {

        boolean hasValidNextPageEntry = false;

        // Iterate searchAfter until it finds a *valid* entry.
        // If loop reaches to the end and no valid entries found, then current page is the last page.
        while (after != null) {
            // fetch next page
            TopDocs nextPageResults;
            if (sort == null) {
                nextPageResults = s.searchAfter(after, tq, count);
            } else {
                nextPageResults = s.searchAfter(after, tq, count, sort, false, false);
            }
            if (nextPageResults == null) {
                break;
            }

            ScoreDoc[] hits = nextPageResults.scoreDocs;
            if (hits.length == 0) {
                // reached to the end
                break;
            }

            ServiceDocumentQueryResult rspForNextPage = new ServiceDocumentQueryResult();
            rspForNextPage.documents = new HashMap<>();
            after = processQueryResults(qs, options, count, s, rspForNextPage, hits,
                    queryStartTimeMicros);

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
    private String createNextPage(Operation op, IndexSearcher s, QuerySpecification qs,
            Query tq,
            Sort sort,
            ScoreDoc after,
            Integer groupOffset,
            long expiration,
            String indexLink,
            boolean hasPage) {

        URI u = UriUtils.buildUri(getHost(), UriUtils.buildUriPath(ServiceUriPaths.CORE_QUERY_PAGE,
                Utils.getNowMicrosUtc() + ""));

        // the page link must point to this node, since the index searcher and results have been
        // computed locally. Transform the link to a forwarder link, which will transparently
        // forward requests to this node
        URI forwarderUri = UriUtils.buildForwardToPeerUri(u, getHost().getId(),
                ServiceUriPaths.DEFAULT_NODE_SELECTOR, EnumSet.noneOf(ServiceOption.class));
        String nextLink = forwarderUri.getPath() + UriUtils.URI_QUERY_CHAR
                + forwarderUri.getQuery();

        URI forwarderUriOfPrevLinkForNewPage = UriUtils.buildForwardToPeerUri(op.getReferer(),
                getHost().getId(),
                ServiceUriPaths.DEFAULT_NODE_SELECTOR, EnumSet.noneOf(ServiceOption.class));
        String prevLinkForNewPage = forwarderUriOfPrevLinkForNewPage.getPath()
                + UriUtils.URI_QUERY_CHAR + forwarderUriOfPrevLinkForNewPage.getQuery();

        // Requests to core/query-page are forwarded to document-index (this service) and
        // referrer of that forwarded request is set to original query-page request.
        // This method is called when query-page wants to create new page for a paginated query.
        // If a new page is going to be created then it is safe to use query-page link
        // from referrer as previous page link of this new page being created.
        LuceneQueryPage page = null;
        if (after != null || groupOffset == null) {
            // page for documents
            page = new LuceneQueryPage(hasPage ? prevLinkForNewPage : null, after);
        } else {
            // page for group results
            page = new LuceneQueryPage(hasPage ? prevLinkForNewPage : null, groupOffset);
        }

        QuerySpecification spec = new QuerySpecification();
        qs.copyTo(spec);

        if (groupOffset == null) {
            spec.options.remove(QueryOption.GROUP_BY);
        }

        spec.context.nativeQuery = tq;
        spec.context.nativePage = page;
        spec.context.nativeSearcher = s;
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

        getHost().startService(startPost, new QueryPageService(spec, indexLink));
        return nextLink;
    }

    private ScoreDoc processQueryResults(QuerySpecification qs, EnumSet<QueryOption> options,
            int resultLimit, IndexSearcher s, ServiceDocumentQueryResult rsp, ScoreDoc[] hits,
            long queryStartTimeMicros) throws Throwable {

        ScoreDoc lastDocVisited = null;
        Set<String> fieldsToLoad = this.fieldsToLoadNoExpand;
        if (options.contains(QueryOption.EXPAND_CONTENT)
                || options.contains(QueryOption.OWNER_SELECTION)
                || options.contains(QueryOption.EXPAND_BINARY_CONTENT)
                || options.contains(QueryOption.EXPAND_SELECTED_FIELDS)) {
            fieldsToLoad = this.fieldsToLoadWithExpand;
        }

        if (options.contains(QueryOption.SELECT_LINKS)) {
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

        long searcherUpdateTime = getSearcherUpdateTime(s, queryStartTimeMicros);
        Map<String, Long> latestVersionPerLink = new HashMap<>();

        DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
        for (ScoreDoc sd : hits) {
            if (!hasCountOption && uniques.size() >= resultLimit) {
                break;
            }

            lastDocVisited = sd;
            loadDoc(s, visitor, sd.doc, fieldsToLoad);
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
                    latestVersion = getLatestVersion(s, searcherUpdateTime, link, documentVersion,
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

            if (hasCountOption) {
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
                    augmentDoc(s, visitor, sd.doc, LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE);
                    json = visitor.jsonSerializedState;
                    if (json == null) {
                        continue;
                    }
                }
            }

            if (options.contains(QueryOption.OWNER_SELECTION)) {
                if (!processQueryResultsForOwnerSelection(json, state)) {
                    continue;
                }
            }

            if (options.contains(QueryOption.EXPAND_BINARY_CONTENT) && !rsp.documents.containsKey(link)) {
                byte[] binaryData = visitor.binarySerializedState;
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
                    Field field = ReflectionUtils.getFieldIfExists(state.getClass(), selectField);
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
                Iterator<Entry<String, JsonElement>> it = jo.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<String, JsonElement> entry = it.next();
                    if (!selectFields.contains(entry.getKey())) {
                        // this is called only for primitive-typed fields, the rest are nullified already
                        it.remove();
                    }
                }

                rsp.documents.put(link, jo);
            }

            if (options.contains(QueryOption.SELECT_LINKS)) {
                processQueryResultsForSelectLinks(s, qs, rsp, visitor, sd.doc, link, state);
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

    private void loadDoc(IndexSearcher s, DocumentStoredFieldVisitor visitor, int docId, Set<String> fields) throws IOException {
        visitor.reset(fields);
        s.doc(docId, visitor);
    }

    private void augmentDoc(IndexSearcher s, DocumentStoredFieldVisitor visitor, int docId, String field) throws IOException {
        visitor.reset(field);
        s.doc(docId, visitor);
    }

    private boolean processQueryResultsForOwnerSelection(String json, ServiceDocument state) {
        String documentOwner;
        if (state == null) {
            documentOwner = Utils.fromJson(json, ServiceDocument.class).documentOwner;
        } else {
            documentOwner = state.documentOwner;
        }
        // omit the result if the documentOwner is not the same as the local owner
        if (documentOwner != null && !documentOwner.equals(getHost().getId())) {
            return false;
        }
        return true;
    }

    private ServiceDocument processQueryResultsForSelectLinks(IndexSearcher s,
            QuerySpecification qs, ServiceDocumentQueryResult rsp, DocumentStoredFieldVisitor d, int docId,
            String link,
            ServiceDocument state) throws Throwable {
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
                DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
                loadDoc(s, visitor, docId, this.fieldsToLoadWithExpand);
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
                linksPerDocument.put(
                        QuerySpecification.buildLinkCollectionItemName(qt.propertyName, index++),
                        item);
                rsp.selectedLinks.add(item);
            }
        }
        return state;
    }

    private ServiceDocument getStateFromLuceneDocument(DocumentStoredFieldVisitor doc, String link) {
        byte[] binaryStateField = doc.binarySerializedState;
        if (binaryStateField == null) {
            logWarning("State not found for %s", link);
            return null;
        }
        ServiceDocument state = (ServiceDocument) KryoSerializers.deserializeDocument(binaryStateField,
                0, binaryStateField.length);
        if (state.documentSelfLink == null) {
            state.documentSelfLink = link;
        }
        if (state.documentKind == null) {
            state.documentKind = Utils.buildKind(state.getClass());
        }
        return state;
    }

    private long getSearcherUpdateTime(IndexSearcher s, long queryStartTimeMicros) {
        if (s == null) {
            return 0L;
        }
        return this.searcherUpdateTimesMicros.getOrDefault(s.hashCode(), queryStartTimeMicros);
    }

    private long getLatestVersion(IndexSearcher s,
            long searcherUpdateTime,
            String link, long version, long documentsUpdatedBeforeInMicros) throws IOException {
        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            adjustStat(STAT_NAME_VERSION_CACHE_LOOKUP_COUNT, 1);
        }

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

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            adjustStat(STAT_NAME_VERSION_CACHE_MISS_COUNT, 1);
        }

        TopDocs td = queryIndexForVersion(link, s, null,
                documentsUpdatedBeforeInMicros > 0 ? documentsUpdatedBeforeInMicros : null);
        // Checking if total hits were Zero when QueryOption.TIME_SNAPSHOT is enabled
        if (documentsUpdatedBeforeInMicros != -1 && td.totalHits == 0) {
            return -1;
        }

        if (td.totalHits == 0) {
            return version;
        }

        DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
        loadDoc(s, visitor, td.scoreDocs[0].doc, this.fieldToLoadVersionLookup);

        long latestVersion = visitor.documentVersion;
        long updateTime = visitor.documentUpdateTimeMicros;
        // attempt to refresh or create new version cache entry, from the entry in the query results
        // The update method will reject the update if the version is stale
        updateLinkInfoCache(null, link, null, latestVersion, updateTime);
        return latestVersion;
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

    public void handleDeleteImpl(Operation delete) throws Throwable {
        setProcessingStage(ProcessingStage.STOPPED);

        this.privateIndexingExecutor.shutdown();
        this.privateQueryExecutor.shutdown();
        IndexWriter w = this.writer;
        this.writer = null;
        close(w);
        this.getHost().stopService(this);
        delete.complete();
    }

    void close(IndexWriter wr) {
        try {
            if (wr == null) {
                return;
            }
            logInfo("Document count: %d ", wr.maxDoc());
            wr.commit();
            wr.close();
        } catch (Throwable e) {

        }
    }

    protected void updateIndex(Operation updateOp) throws Throwable {
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

        IndexWriter wr = this.writer;
        if (wr == null) {
            updateOp.fail(new CancellationException());
            return;
        }
        s.documentDescription = null;

        LuceneIndexDocumentHelper indexDocHelper = this.indexDocumentHelper.get();

        indexDocHelper.addSelfLinkField(link);
        if (s.documentKind != null) {
            indexDocHelper.addKindField(s.documentKind);
        }
        indexDocHelper.addUpdateActionField(s.documentUpdateAction);
        indexDocHelper.addBinaryStateFieldToDocument(s, r.serializedDocument, desc);
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

        Document threadLocalDoc = indexDocHelper.getDoc();
        try {
            if (desc.propertyDescriptions == null
                    || desc.propertyDescriptions.isEmpty()) {
                // no additional property type information, so we will add the
                // document with common fields indexed plus the full body
                addDocumentToIndex(wr, updateOp, threadLocalDoc, s, desc);
                return;
            }

            indexDocHelper.addIndexableFieldsToDocument(s, desc);

            if (hasOption(ServiceOption.INSTRUMENTATION)) {
                int fieldCount = indexDocHelper.getDoc().getFields().size();
                setTimeSeriesStat(STAT_NAME_INDEXED_FIELD_COUNT, AGGREGATION_TYPE_SUM, fieldCount);
                ServiceStat st = getHistogramStat(STAT_NAME_FIELD_COUNT_PER_DOCUMENT);
                setStat(st, fieldCount);
            }

            addDocumentToIndex(wr, updateOp, threadLocalDoc, s, desc);
        } finally {
            // NOTE: The Document is a thread local managed by the index document helper. Its fields
            // must be cleared *after* its added to the index (above) and *before* its re-used.
            // After the fields are cleared, the document can not be used in this scope
            threadLocalDoc.clear();
        }
    }

    private void checkDocumentRetentionLimit(ServiceDocument state, ServiceDocumentDescription desc)
            throws IOException {

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
    private void checkFailureAndRecover(Throwable e) {

        // When document create or update fails with an exception. Clear the threadLocalDoc.
        Document threadLocalDoc = this.indexDocumentHelper.get().getDoc();
        threadLocalDoc.clear();

        if (getHost().isStopping()) {
            logInfo("Exception after host stop, on index service thread: %s", e.toString());
            return;
        }
        if (!(e instanceof AlreadyClosedException)) {
            logSevere("Exception on index service thread: %s", Utils.toString(e));
            return;
        }

        IndexWriter w = this.writer;
        if ((w != null && w.isOpen()) || e.getMessage().contains("IndexReader")) {
            // The already closed exception can happen due to an expired searcher, simply
            // log in that case
            adjustStat(STAT_NAME_READER_ALREADY_CLOSED_EXCEPTION_COUNT, 1);
            logWarning("Exception on index service thread: %s", Utils.toString(e));
            return;
        }

        logSevere("Exception on index service thread: %s", Utils.toString(e));
        this.adjustStat(STAT_NAME_WRITER_ALREADY_CLOSED_EXCEPTION_COUNT, 1);
        applyFileLimitRefreshWriter(true);
    }

    private void deleteAllDocumentsForSelfLinkForcedPost(IndexWriter wr, ServiceDocument sd)
            throws IOException {
        // Delete all previous versions from the index. If we do not, we will end up with
        // duplicate version history
        adjustStat(STAT_NAME_FORCED_UPDATE_DOCUMENT_DELETE_COUNT, 1);
        wr.deleteDocuments(new Term(ServiceDocument.FIELD_NAME_SELF_LINK, sd.documentSelfLink));
    }

    private void deleteAllDocumentsForSelfLink(Operation postOrDelete, String link,
            ServiceDocument state)
            throws Throwable {
        deleteDocumentsFromIndex(postOrDelete, link, state != null ? state.documentKind : null, 0, Long.MAX_VALUE);
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

    private void deleteDocumentsFromIndex(Operation delete, String link, String kind, long oldestVersion,
            long newestVersion) throws Throwable {

        IndexWriter wr = this.writer;
        if (wr == null) {
            delete.fail(new CancellationException());
            return;
        }

        deleteDocumentFromIndex(link, oldestVersion, newestVersion, wr);

        // Use time AFTER index was updated to be sure that it can be compared
        // against the time the searcher was updated and have this change
        // be reflected in the new searcher. If the start time would be used,
        // it is possible to race with updating the searcher and NOT have this
        // change be reflected in the searcher.
        updateLinkInfoCache(null, link, kind, newestVersion, Utils.getNowMicrosUtc());
        delete.complete();
    }

    private void deleteDocumentFromIndex(String link, long oldestVersion, long newestVersion,
            IndexWriter wr) throws IOException {
        Query linkQuery = new TermQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK, link));
        Query versionQuery = LongPoint.newRangeQuery(ServiceDocument.FIELD_NAME_VERSION,
                oldestVersion, newestVersion);

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(versionQuery, Occur.MUST);
        builder.add(linkQuery, Occur.MUST);
        BooleanQuery bq = builder.build();
        wr.deleteDocuments(bq);
    }

    private void addDocumentToIndex(IndexWriter wr, Operation op, Document doc, ServiceDocument sd,
            ServiceDocumentDescription desc) throws IOException {
        long startNanos = 0;
        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            startNanos = System.nanoTime();
        }

        if (op.getAction() == Action.POST
                && op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)) {
            deleteAllDocumentsForSelfLinkForcedPost(wr, sd);
        }

        wr.addDocument(doc);

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
        updateLinkInfoCache(desc, sd.documentSelfLink, sd.documentKind, sd.documentVersion,
                Utils.getNowMicrosUtc());
        op.setBody(null).complete();
        checkDocumentRetentionLimit(sd, desc);
        applyActiveQueries(op, sd, desc);
    }

    private void updateLinkInfoCache(ServiceDocumentDescription desc,
            String link, String kind, long version, long lastAccessTime) {
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

                if (kind != null) {
                    this.documentKindUpdateInfo.compute(kind, (k, entry) -> {
                        if (entry == null) {
                            entry = 0L;
                        }
                        entry = Math.max(entry, lastAccessTime);
                        return entry;
                    });
                }
            }

            // The index update time may only be increased.
            if (this.writerUpdateTimeMicros < lastAccessTime) {
                this.writerUpdateTimeMicros = lastAccessTime;
            }
        }
    }

    /**
     * Returns an updated {@link IndexSearcher} to query {@code selfLink}.
     *
     * If the index has been updated since the last {@link IndexSearcher} was created, those
     * changes will not be reflected by that {@link IndexSearcher}. However, for performance
     * reasons, we do not want to create a new one for every query either.
     *
     * We create one in one of following conditions:
     *
     *   1) No searcher for this index exists.
     *   2) The query is across many links or multiple version, not a specific one,
     *      and the index was changed.
     *   3) The query is for a specific self link AND the self link has seen an update
     *      after the searcher was last updated.
     *
     * @param selfLink
     * @param resultLimit
     * @param w
     * @return an {@link IndexSearcher} that is fresh enough to execute the specified query
     * @throws IOException
     */
    private IndexSearcher createOrRefreshSearcher(String selfLink, Set<String> kindScope,
            int resultLimit, IndexWriter w,
            boolean doNotRefresh)
            throws IOException {

        IndexSearcher s;
        boolean needNewSearcher = false;
        long threadId = Thread.currentThread().getId();
        long now = Utils.getNowMicrosUtc();
        synchronized (this.searchSync) {
            s = this.searchers.get(threadId);
            long searcherUpdateTime = getSearcherUpdateTime(s, 0);
            if (s == null) {
                needNewSearcher = true;
            } else if (documentNeedsNewSearcher(selfLink, kindScope, resultLimit,
                    searcherUpdateTime)) {
                needNewSearcher = !doNotRefresh;
            }
        }

        if (s != null && !needNewSearcher) {
            return s;
        }

        if (s != null) {
            s.getIndexReader().close();
            this.searcherUpdateTimesMicros.remove(s.hashCode());
        }

        s = new IndexSearcher(DirectoryReader.open(w, true, true));

        adjustTimeSeriesStat(STAT_NAME_SEARCHER_UPDATE_COUNT, AGGREGATION_TYPE_SUM, 1);
        synchronized (this.searchSync) {
            this.searchers.put(threadId, s);
            this.searcherUpdateTimesMicros.put(s.hashCode(), now);
            return s;
        }
    }

    private boolean documentNeedsNewSearcher(String selfLink, Set<String> kindScope,
            int resultLimit, long searcherUpdateTime) {
        if (selfLink != null && resultLimit == 1) {
            DocumentUpdateInfo du = this.updatesPerLink.get(selfLink);

            // ODL services may be created and removed due to memory pressure while searcher was not updated.
            // Then, retrieval of those services will fail because searcher doesn't know the creation yet.
            // To incorporate such service removal, also check the serviceRemovalDetectedTimeMicros.
            if (du == null && (searcherUpdateTime < this.serviceRemovalDetectedTimeMicros)) {
                return true;
            }

            if (du != null && du.updateTimeMicros >= searcherUpdateTime) {
                return true;
            } else {
                String parent = UriUtils.getParentPath(selfLink);
                Long updateTime = this.immutableParentLinks.get(parent);
                if (updateTime != null && updateTime >= searcherUpdateTime) {
                    return true;
                }
            }
        } else if (kindScope != null) {
            for (String kind : kindScope) {
                Long documentKindUpdateTime = this.documentKindUpdateInfo.get(kind);
                if (documentKindUpdateTime == null) {
                    documentKindUpdateTime = Long.MAX_VALUE;
                }
                if (searcherUpdateTime < documentKindUpdateTime) {
                    return true;
                }
            }
            adjustTimeSeriesStat(STAT_NAME_SEARCHER_REUSE_BY_DOCUMENT_KIND_COUNT,
                    AGGREGATION_TYPE_SUM, 1);
        } else if (searcherUpdateTime < this.writerUpdateTimeMicros) {
            return true;
        }

        return false;
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

    private void handleMaintenanceImpl(Operation op) throws Throwable {
        try {
            IndexWriter w = this.writer;
            if (w == null) {
                op.fail(new CancellationException());
                return;
            }

            long deadline = Utils.getSystemNowMicrosUtc() + getMaintenanceIntervalMicros();

            long startNanos = System.nanoTime();
            IndexSearcher s = createOrRefreshSearcher(null, null, Integer.MAX_VALUE, w, false);
            long endNanos = System.nanoTime();
            setTimeSeriesHistogramStat(STAT_NAME_MAINTENANCE_SEARCHER_REFRESH_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            startNanos = endNanos;
            applyDocumentExpirationPolicy(s, deadline);
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
            applyMemoryLimit();
            endNanos = System.nanoTime();
            setTimeSeriesHistogramStat(STAT_NAME_MAINTENANCE_MEMORY_LIMIT_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            startNanos = endNanos;
            w.commit();
            endNanos = System.nanoTime();
            adjustTimeSeriesStat(STAT_NAME_COMMIT_COUNT, AGGREGATION_TYPE_SUM, 1);
            setTimeSeriesHistogramStat(STAT_NAME_COMMIT_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            startNanos = endNanos;
            applyFileLimitRefreshWriter(false);
            endNanos = System.nanoTime();
            setTimeSeriesHistogramStat(STAT_NAME_MAINTENANCE_FILE_LIMIT_REFRESH_DURATION_MICROS,
                    AGGREGATION_TYPE_AVG_MAX,
                    TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));

            if (this.hasOption(ServiceOption.INSTRUMENTATION)) {
                setStat(LuceneDocumentIndexService.STAT_NAME_INDEXED_DOCUMENT_COUNT, w.numDocs());
            }

            op.complete();
        } catch (Throwable e) {
            if (this.getHost().isStopping()) {
                op.fail(new CancellationException());
                return;
            }
            logWarning("Attempting recovery due to error: %s", Utils.toString(e));
            applyFileLimitRefreshWriter(true);
            op.fail(e);
        }
    }

    private void applyFileLimitRefreshWriter(boolean force) {
        if (getHost().isStopping()) {
            return;
        }

        if (!isDurable()) {
            return;
        }

        long now = Utils.getNowMicrosUtc();
        if (now - this.writerCreationTimeMicros < getHost()
                .getMaintenanceIntervalMicros()) {
            logInfo("Skipping writer re-open, it was created recently");
            return;
        }

        File directory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory);
        Stream<Path> stream = null;
        long count;
        try {
            stream = Files.list(directory.toPath());
            count = stream.count();
            if (!force && count < indexFileCountThresholdForWriterRefresh) {
                return;
            }
        } catch (IOException e1) {
            logSevere(e1);
            return;
        } finally {
            if (stream != null) {
                stream.close();
            }
        }

        final int acquireReleaseCount = QUERY_THREAD_COUNT + UPDATE_THREAD_COUNT;
        try {
            // Do not proceed unless we have blocked all reader+writer threads. We assume
            // the semaphore is already acquired by the current thread
            this.writerSync.release();
            this.writerSync.acquire(acquireReleaseCount);
            IndexWriter w = this.writer;
            if (w == null) {
                return;
            }

            logInfo("(%s) closing all searchers, document count: %d, file count: %d",
                    this.writerSync, w.maxDoc(), count);

            for (IndexSearcher s : this.searchers.values()) {
                s.getIndexReader().close();
                this.searcherUpdateTimesMicros.remove(s.hashCode());
            }

            this.searchers.clear();

            if (!force) {
                return;
            }

            logInfo("Closing all paginated searchers (%d)",
                    this.paginatedSearchersByCreationTime.size());

            for (PaginatedSearcherInfo info : this.paginatedSearchersByCreationTime.values()) {
                try {
                    IndexSearcher s = info.searcher;
                    s.getIndexReader().close();
                } catch (Throwable ignored) {
                }
            }

            this.paginatedSearchersByCreationTime.clear();
            this.paginatedSearchersByExpirationTime.clear();
            this.searcherUpdateTimesMicros.clear();

            try {
                w.close();
            } catch (Throwable ignored) {
            }

            w = createWriter(directory, false);
            stream = Files.list(directory.toPath());
            count = stream.count();
            logInfo("(%s) reopened writer, document count: %d, file count: %d",
                    this.writerSync, w.maxDoc(), count);
        } catch (Throwable e) {
            // If we fail to re-open we should stop the host, since we can not recover.
            logSevere(e);
            logWarning("Stopping local host since index is not accessible");
            close(this.writer);
            this.writer = null;
            sendRequest(Operation.createDelete(this, ServiceUriPaths.CORE_MANAGEMENT));
        } finally {
            // release all but one, so we stay owning one reference to the semaphore
            this.writerSync.release(acquireReleaseCount - 1);
            if (stream != null) {
                stream.close();
            }
        }
    }

    private void applyDocumentVersionRetentionPolicy(long deadline) throws Throwable {
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
                IndexWriter wr = this.writer;
                if (wr == null) {
                    return;
                }
                deleteDocumentsFromIndex(dummyDelete, e.getKey(), null,  0, e.getValue());
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

        Map<Long, List<PaginatedSearcherInfo>> entriesToClose = new HashMap<>();
        long activePaginatedQueries;
        synchronized (this.searchSync) {
            Iterator<Entry<Long, List<PaginatedSearcherInfo>>> itr =
                    this.paginatedSearchersByExpirationTime.entrySet().iterator();
            while (itr.hasNext()) {
                Entry<Long, List<PaginatedSearcherInfo>> entry = itr.next();
                long expirationMicros = entry.getKey();
                if (expirationMicros > now) {
                    break;
                }

                List<PaginatedSearcherInfo> expirationList = entry.getValue();
                for (PaginatedSearcherInfo info : expirationList) {
                    this.paginatedSearchersByCreationTime.remove(info.creationTimeMicros);
                    this.searcherUpdateTimesMicros.remove(info.searcher.hashCode());
                }

                entriesToClose.put(expirationMicros, expirationList);
                itr.remove();
            }

            activePaginatedQueries = this.paginatedSearchersByCreationTime.size();
        }

        setTimeSeriesStat(STAT_NAME_ACTIVE_PAGINATED_QUERIES, AGGREGATION_TYPE_AVG_MAX,
                activePaginatedQueries);

        for (Entry<Long, List<PaginatedSearcherInfo>> entry : entriesToClose.entrySet()) {
            for (PaginatedSearcherInfo info : entry.getValue()) {
                logFine("Closing paginated query searcher, expired at %d", entry.getKey());
                try {
                    info.searcher.getIndexReader().close();
                } catch (Throwable ignored) {
                }
            }
        }
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
            // update index time to force searcher update, per thread
            this.writerUpdateTimeMicros = Utils.getNowMicrosUtc();
        }

        if (count == 0) {
            return;
        }
        this.serviceRemovalDetectedTimeMicros = Utils.getNowMicrosUtc();

        logInfo("Cleared %d document update entries", count);
    }

    private void applyDocumentExpirationPolicy(IndexSearcher s, long deadline) throws Throwable {

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

            DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
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

                adjustTimeSeriesStat(STAT_NAME_DOCUMENT_EXPIRATION_COUNT, AGGREGATION_TYPE_SUM, 1);

                // update document with one that has all fields, including binary state
                augmentDoc(s, visitor, scoreDoc.doc, LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE);
                ServiceDocument serviceDocument = null;
                try {
                    serviceDocument = getStateFromLuceneDocument(visitor, documentSelfLink);
                } catch (Throwable e) {
                    logWarning("Error deserializing state for %s: %s", documentSelfLink,
                            e.getMessage());
                }

                if (dummyDelete == null) {
                    dummyDelete = Operation.createDelete(null);
                }

                deleteAllDocumentsForSelfLink(dummyDelete, documentSelfLink, serviceDocument);
            }
        } while (Utils.getSystemNowMicrosUtc() < deadline);
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

    void setWriterUpdateTimeMicros(long writerUpdateTimeMicros) {
        this.writerUpdateTimeMicros = writerUpdateTimeMicros;
    }
}
