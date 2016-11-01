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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexUpgrader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
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
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.MemoryLimitType;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.serialization.KryoSerializers;
import com.vmware.xenon.services.common.QueryFilter.QueryFilterException;
import com.vmware.xenon.services.common.QueryPageService.LuceneQueryPage;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;

public class LuceneDocumentIndexService extends StatelessService {

    public static String SELF_LINK = ServiceUriPaths.CORE_DOCUMENT_INDEX;

    public static final int QUERY_THREAD_COUNT = Integer.getInteger(
            Utils.PROPERTY_NAME_PREFIX
                    + LuceneDocumentIndexService.class.getSimpleName()
                    + "QUERY_THREAD_COUNT",
            Utils.DEFAULT_THREAD_COUNT * 2);

    public static final int UPDATE_THREAD_COUNT = Integer.getInteger(
            Utils.PROPERTY_NAME_PREFIX
                    + LuceneDocumentIndexService.class.getSimpleName()
                    + "UPDATE_THREAD_COUNT",
            Utils.DEFAULT_THREAD_COUNT / 2);

    public static final String FILE_PATH_LUCENE = "lucene";

    public static final int DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH = 10000;

    public static final int DEFAULT_INDEX_SEARCHER_COUNT_THRESHOLD = 200;

    private String indexDirectory;

    private static int EXPIRED_DOCUMENT_SEARCH_THRESHOLD = 1000;

    private static int INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH = DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH;

    public static void setIndexFileCountThresholdForWriterRefresh(int count) {
        INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH = count;
    }

    public static int getIndexFileCountThresholdForWriterRefresh() {
        return INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH;
    }

    public static void setExpiredDocumentSearchThreshold(int count) {
        EXPIRED_DOCUMENT_SEARCH_THRESHOLD = count;
    }

    public static int getExpiredDocumentSearchThreshold() {
        return EXPIRED_DOCUMENT_SEARCH_THRESHOLD;
    }

    private static final String LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE = "binarySerializedState";

    private static final String LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE = "jsonSerializedState";

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

    public static final String STAT_NAME_FIELD_COUNT_PER_DOCUMENT = "fieldCountPerDocument";

    public static final String STAT_NAME_INDEXING_DURATION_MICROS = "indexingDurationMicros";

    public static final String STAT_NAME_SEARCHER_UPDATE_COUNT = "indexSearcherUpdateCount";

    private static final String STAT_NAME_WRITER_ALREADY_CLOSED_EXCEPTION_COUNT = "indexWriterAlreadyClosedFailureCount";

    public static final String STAT_NAME_SERVICE_DELETE_COUNT = "serviceDeleteCount";

    public static final String STAT_NAME_DOCUMENT_EXPIRATION_COUNT = "expiredDocumentCount";

    public static final String STAT_NAME_DOCUMENT_EXPIRATION_FORCED_MAINTENANCE_COUNT = "expiredDocumentForcedMaintenanceCount";

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

    /**
     * Searcher refresh time, per thread
     */
    private ThreadLocal<Long> searcherUpdateTimeMicros = new ThreadLocal<Long>() {
        @Override
        protected Long initialValue() {
            return 0L;
        }
    };

    /**
     * Map of searchers used for paginated query tasks, indexed by creation time
     */
    protected TreeMap<Long, List<IndexSearcher>> searchersForPaginatedQueries = new TreeMap<>();

    protected IndexWriter writer = null;

    protected Map<String, QueryTask> activeQueries = new ConcurrentSkipListMap<>();

    private long writerUpdateTimeMicros;

    private long writerCreationTimeMicros;

    private final Map<String, Long> linkAccessTimes = new HashMap<>();
    private final Map<String, Long> linkDocumentRetentionEstimates = new HashMap<>();
    private long linkAccessMemoryLimitMB;

    private Sort versionSort;

    private ExecutorService privateIndexingExecutor;

    private ExecutorService privateQueryExecutor;

    private Set<String> fieldsToLoadNoExpand;
    private Set<String> fieldsToLoadWithExpand;

    private RoundRobinOperationQueue queryQueue = RoundRobinOperationQueue.create();
    private RoundRobinOperationQueue updateQueue = RoundRobinOperationQueue.create();

    private URI uri;

    public static class BackupRequest extends ServiceDocument {
        URI backupFile;
        static final String KIND = Utils.buildKind(BackupRequest.class);
    }

    public static class RestoreRequest extends ServiceDocument {
        URI backupFile;
        static final String KIND = Utils.buildKind(RestoreRequest.class);
    }

    public LuceneDocumentIndexService() {
        this(FILE_PATH_LUCENE);
    }

    public LuceneDocumentIndexService(String indexDirectory) {
        super(ServiceDocument.class);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        this.indexDirectory = indexDirectory;
    }

    @Override
    public void handleStart(final Operation post) {
        super.setMaintenanceIntervalMicros(getHost().getMaintenanceIntervalMicros() * 5);
        // index service getUri() will be invoked on every load and save call for every operation,
        // so its worth caching (plus we only have a very small number of index services
        this.uri = super.getUri();

        File directory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory);
        this.privateQueryExecutor = Executors.newFixedThreadPool(QUERY_THREAD_COUNT,
                r -> new Thread(r, getUri() + "/queries/" + Utils.getSystemNowMicrosUtc()));
        this.privateIndexingExecutor = Executors.newFixedThreadPool(UPDATE_THREAD_COUNT,
                r -> new Thread(r, getSelfLink() + "/updates/" + Utils.getSystemNowMicrosUtc()));

        initializeInstance();

        // create durable index writer
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
                    logWarning("Failure creating index writer, will retry");
                    close(this.writer);
                    archiveCorruptIndexFiles(directory);
                    continue;
                }
                logWarning("Failure creating index writer: %s", Utils.toString(e));
                post.fail(e);
                return;
            }
        }

        initializeStats();

        post.complete();
    }

    private void initializeInstance() {
        this.searchSync = new Object();
        this.searchersForPaginatedQueries.clear();
        this.versionSort = new Sort(new SortedNumericSortField(ServiceDocument.FIELD_NAME_VERSION,
                SortField.Type.LONG, true));

        this.fieldsToLoadNoExpand = new HashSet<>();
        this.fieldsToLoadNoExpand.add(ServiceDocument.FIELD_NAME_SELF_LINK);
        this.fieldsToLoadNoExpand.add(ServiceDocument.FIELD_NAME_VERSION);
        this.fieldsToLoadNoExpand.add(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS);
        this.fieldsToLoadNoExpand.add(ServiceDocument.FIELD_NAME_UPDATE_ACTION);
        this.fieldsToLoadNoExpand.add(ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS);
        this.fieldsToLoadWithExpand = new HashSet<>(this.fieldsToLoadNoExpand);
        this.fieldsToLoadWithExpand.add(LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE);
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
        ServiceStat dayStat = getTimeSeriesStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_DAY,
                (int) TimeUnit.DAYS.toHours(1), TimeUnit.HOURS.toMillis(1), type);
        this.setStat(dayStat, v);
        ServiceStat hourStat = getTimeSeriesStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR,
                (int) TimeUnit.HOURS.toMinutes(1), TimeUnit.MINUTES.toMillis(1), type);
        this.setStat(hourStat, v);
    }

    private void adjustTimeSeriesStat(String name, EnumSet<AggregationType> type, double delta) {
        ServiceStat dayStat = getTimeSeriesStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_DAY,
                (int) TimeUnit.DAYS.toHours(1), TimeUnit.HOURS.toMillis(1), type);
        this.adjustStat(dayStat, delta);
        ServiceStat hourStat = getTimeSeriesStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR,
                (int) TimeUnit.HOURS.toMinutes(1), TimeUnit.MINUTES.toMillis(1), type);
        this.adjustStat(hourStat, delta);
    }

    private void setTimeSeriesHistogramStat(String name, EnumSet<AggregationType> type, double v) {
        ServiceStat dayStat = getTimeSeriesHistogramStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_DAY,
                (int) TimeUnit.DAYS.toHours(1), TimeUnit.HOURS.toMillis(1), type);
        this.setStat(dayStat, v);
        ServiceStat hourStat = getTimeSeriesHistogramStat(name + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR,
                (int) TimeUnit.HOURS.toMinutes(1), TimeUnit.MINUTES.toMillis(1), type);
        this.setStat(hourStat, v);
    }

    public IndexWriter createWriter(File directory, boolean doUpgrade) throws Exception {
        Analyzer analyzer = new SimpleAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        Long totalMBs = getHost().getServiceMemoryLimitMB(getSelfLink(), MemoryLimitType.EXACT);
        if (totalMBs != null) {
            long cacheSizeMB = (totalMBs * 3) / 4;
            cacheSizeMB = Math.max(1, cacheSizeMB);
            iwc.setRAMBufferSizeMB(cacheSizeMB);
            this.linkAccessMemoryLimitMB = totalMBs / 4;
        }

        Directory dir = MMapDirectory.open(directory.toPath());

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
            this.linkAccessTimes.clear();
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

    private void archiveCorruptIndexFiles(File directory) {
        File newDirectory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory
                + "." + Utils.getNowMicrosUtc());
        try {
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
        queryIndex(op, options, s, tq, null, Integer.MAX_VALUE, 0, null, rsp, null);
    }

    private void handleBackup(Operation op, BackupRequest req) throws Throwable {
        SnapshotDeletionPolicy snapshotter = null;
        IndexCommit commit = null;
        handleMaintenanceImpl(true);
        IndexWriter w = this.writer;
        if (w == null) {
            op.fail(new CancellationException());
            return;
        }
        try {
            // Create a snapshot so the index files won't be deleted.
            snapshotter = (SnapshotDeletionPolicy) w.getConfig().getIndexDeletionPolicy();
            commit = snapshotter.snapshot();

            String indexDirectory = UriUtils.buildUriPath(getHost().getStorageSandbox().getPath(),
                    this.indexDirectory);

            // Add the files in the commit to a zip file.
            List<URI> fileList = FileUtils.filesToUris(indexDirectory, commit.getFileNames());
            req.backupFile = FileUtils.zipFiles(fileList,
                    this.indexDirectory + "-" + Utils.getNowMicrosUtc());

            op.setBody(req).complete();
        } catch (Exception e) {
            this.logSevere(e);
            throw e;
        } finally {
            if (snapshotter != null) {
                snapshotter.release(commit);
            }
            w.deleteUnusedFiles();
        }
    }

    private void handleRestore(Operation op, RestoreRequest req) {
        IndexWriter w = this.writer;
        if (w == null) {
            op.fail(new CancellationException());
            return;
        }

        // We already have a slot in the semaphore.  Acquire the rest.
        final int semaphoreCount = QUERY_THREAD_COUNT + UPDATE_THREAD_COUNT - 1;
        try {

            this.writerSync.acquire(semaphoreCount);
            close(w);

            File directory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory);
            // Copy whatever was there out just in case.
            if (directory.exists()) {
                // We know the file list won't be null because directory.exists() returned true,
                // but Findbugs doesn't know that, so we make it happy.
                File[] files = directory.listFiles();
                if (files != null && files.length > 0) {
                    this.logInfo("archiving existing index %s", directory);
                    archiveCorruptIndexFiles(directory);
                }
            }

            this.logInfo("restoring index %s from %s md5sum(%s)", directory, req.backupFile,
                    FileUtils.md5sum(new File(req.backupFile)));
            FileUtils.extractZipArchive(new File(req.backupFile), directory.toPath());
            this.writerUpdateTimeMicros = Utils.getNowMicrosUtc();
            createWriter(directory, true);
            op.complete();
            this.logInfo("restore complete");
        } catch (Throwable e) {
            logSevere(e);
            op.fail(e);
        } finally {
            this.writerSync.release(semaphoreCount);
        }
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handleRequest(Operation op) {
        Action a = op.getAction();
        if (a == Action.PUT) {
            getHost().failRequestActionNotSupported(op);
            return;
        }

        if (a == Action.PATCH && op.isRemote()) {
            // PATCH is reserved for in-process QueryTaskService
            getHost().failRequestActionNotSupported(op);
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
                        if (sd.documentKind.equals(BackupRequest.KIND)) {
                            BackupRequest backupRequest = (BackupRequest) op.getBodyRaw();
                            handleBackup(op, backupRequest);
                            break;
                        }
                        if (sd.documentKind.equals(RestoreRequest.KIND)) {
                            RestoreRequest backupRequest = (RestoreRequest) op.getBodyRaw();
                            handleRestore(op, backupRequest);
                            break;
                        }
                    }
                    getHost().failRequestActionNotSupported(op);
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
                    updateIndex(op);
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
            luceneQuery = LuceneQueryConverter.convertToLuceneQuery(task.querySpec.query);
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
            s = createPaginatedQuerySearcher(task.documentExpirationTimeMicros, this.writer);
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

    private IndexSearcher createPaginatedQuerySearcher(long expirationMicros, IndexWriter w)
            throws IOException {
        if (w == null) {
            throw new IllegalStateException("Writer not available");
        }
        IndexSearcher s = new IndexSearcher(DirectoryReader.open(w, true, true));
        synchronized (this.searchSync) {
            List<IndexSearcher> searchers = this.searchersForPaginatedQueries.get(expirationMicros);
            if (searchers == null) {
                searchers = new ArrayList<>();
            }
            searchers.add(s);
            this.searchersForPaginatedQueries.put(expirationMicros, searchers);
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
        String subject = null;
        if (!getHost().isAuthorizationEnabled()
                || op.getAuthorizationContext().isSystemUser()) {
            subject = SystemUserService.SELF_LINK;
        } else {
            subject = op.getAuthorizationContext().getClaims().getSubject();
        }
        return subject;
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

        if (options.contains(QueryOption.EXPAND_CONTENT)) {
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

        if (s == null) {
            s = createOrRefreshSearcher(selfLinkPrefix, count, w,
                    options.contains(QueryOption.DO_NOT_REFRESH));
        }

        tq = updateQuery(op, tq);
        if (tq == null) {
            return false;
        }
        ServiceDocumentQueryResult result = queryIndex(op, options, s, tq, page,
                count, expiration, indexLink, rsp, qs);
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

        IndexSearcher s = createOrRefreshSearcher(selfLink, 1, w, false);

        long startNanos = System.nanoTime();
        TopDocs hits = searchByVersion(selfLink, s, version);
        long durationNanos = System.nanoTime() - startNanos;
        setTimeSeriesHistogramStat(STAT_NAME_QUERY_SINGLE_DURATION_MICROS,
                AGGREGATION_TYPE_AVG_MAX, TimeUnit.NANOSECONDS.toMicros(durationNanos));

        if (hits.totalHits == 0) {
            op.complete();
            return;
        }

        Document doc = s.getIndexReader().document(hits.scoreDocs[0].doc,
                this.fieldsToLoadWithExpand);

        if (checkAndDeleteExpiratedDocuments(selfLink, s, hits.scoreDocs[0].doc, doc,
                Utils.getSystemNowMicrosUtc())) {
            op.complete();
            return;
        }

        ServiceDocument sd = getStateFromLuceneDocument(doc, selfLink);
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
    private TopDocs searchByVersion(String selfLink, IndexSearcher s, Long version)
            throws IOException {
        Query tqSelfLink = new TermQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK, selfLink));

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(tqSelfLink, Occur.MUST);

        if (version != null) {
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
     * Augment the query argument with the resource group query specified
     * by the operation's authorization context.
     *
     * If the operation was executed by the system user, the query argument
     * is returned unmodified.
     *
     * If no query needs to be executed return null
     *
     * @return Augmented query.
     */
    private Query updateQuery(Operation op, Query tq) {
        Query rq = null;
        AuthorizationContext ctx = op.getAuthorizationContext();

        // Allow operation if isAuthorizationEnabled is set to false
        if (!this.getHost().isAuthorizationEnabled()) {
            return tq;
        }

        if (ctx == null) {
            // Don't allow operation if no authorization context and auth is enabled
            return null;
        }

        // Allow unconditionally if this is the system user
        if (ctx.isSystemUser()) {
            return tq;
        }

        // If the resource query in the authorization context is unspecified,
        // use a Lucene query that doesn't return any documents so that every
        // result will be empty.
        if (ctx.getResourceQuery(Action.GET) == null) {
            rq = new MatchNoDocsQuery();
        } else {
            rq = LuceneQueryConverter.convertToLuceneQuery(ctx.getResourceQuery(Action.GET));
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder()
                .add(rq, Occur.FILTER)
                .add(tq, Occur.FILTER);
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

        GroupingSearch groupingSearch = new GroupingSearch(qs.groupByTerm.propertyName);
        groupingSearch.setGroupSort(groupSort);
        groupingSearch.setSortWithinGroup(sort);

        adjustTimeSeriesStat(STAT_NAME_GROUP_QUERY_COUNT, AGGREGATION_TYPE_SUM, 1);

        int groupOffset = page != null ? page.groupOffset : 0;
        int groupLimit = qs.groupResultLimit != null ? qs.groupResultLimit : 10000;

        if (s == null && qs.groupResultLimit != null) {
            s = createPaginatedQuerySearcher(task.documentExpirationTimeMicros, this.writer);
        }

        if (s == null) {
            s = createOrRefreshSearcher(null, Integer.MAX_VALUE, this.writer,
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
            String groupValue = ((BytesRef) groupDocs.groupValue).utf8ToString();

            // we need to modify the query to include a top level clause that restricts scope
            // to documents with the groupBy field and value
            QueryTask.Query clause = new QueryTask.Query()
                    .setTermPropertyName(qs.groupByTerm.propertyName)
                    .setTermMatchValue(groupValue)
                    .setTermMatchType(MatchType.TERM);
            clause.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
            if (perGroupQuery.booleanClauses == null) {
                QueryTask.Query topLevelClause = perGroupQuery;
                perGroupQuery.addBooleanClause(topLevelClause);
            }
            perGroupQuery.addBooleanClause(clause);
            Query lucenePerGroupQuery = LuceneQueryConverter.convertToLuceneQuery(perGroupQuery);

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

    private ServiceDocumentQueryResult queryIndex(Operation op,
            EnumSet<QueryOption> options,
            IndexSearcher s,
            Query tq,
            LuceneQueryPage page,
            int count,
            long expiration,
            String indexLink,
            ServiceDocumentQueryResult rsp,
            QuerySpecification qs) throws Throwable {
        ScoreDoc[] hits;
        ScoreDoc after = null;
        boolean isPaginatedQuery = count != Integer.MAX_VALUE
                && !options.contains(QueryOption.TOP_RESULTS);
        boolean hasPage = page != null;
        boolean shouldProcessResults = true;
        int resultLimit = count;

        if (hasPage) {
            // For example, via GET of QueryTask.nextPageLink
            after = page.after;
            rsp.prevPageLink = page.previousPageLink;
        } else if (isPaginatedQuery) {
            // QueryTask.resultLimit was set, but we don't have a page param yet,
            // which means this is the initial POST to create the QueryTask.
            // Since we are going to throw away TopDocs.hits in this case,
            // just set the limit to 1 and do not process the results.
            resultLimit = 1;
            shouldProcessResults = false;
            rsp.documentCount = 1L;
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

        rsp.queryTimeMicros = 0L;
        long queryStartTimeMicros = Utils.getNowMicrosUtc();
        long start = queryStartTimeMicros;

        do {
            if (sort == null) {
                results = s.searchAfter(after, tq, count);
            } else {
                results = s.searchAfter(after, tq, count, sort, false, false);
            }

            long end = Utils.getNowMicrosUtc();

            if (results == null) {
                return rsp;
            }

            hits = results.scoreDocs;

            long queryTime = end - start;

            rsp.documentCount = 0L;
            rsp.queryTimeMicros += queryTime;
            ScoreDoc bottom = null;
            if (shouldProcessResults) {
                start = Utils.getNowMicrosUtc();
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

            if (!isPaginatedQuery && !options.contains(QueryOption.TOP_RESULTS)) {
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
                        createNextPageLink = checkNextPageHasEntry(bottom, options, s, tq, sort,
                                count, qs, queryStartTimeMicros);
                    }

                    if (createNextPageLink) {
                        expiration += queryTime;
                        rsp.nextPageLink = createNextPage(op, s, qs, tq, sort, bottom, null,
                                expiration,
                                indexLink,
                                hasPage);
                    }

                    break;
                }
            }

            after = bottom;
            resultLimit = count - rsp.documentLinks.size();
        } while (resultLimit > 0);

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
                || options.contains(QueryOption.OWNER_SELECTION)) {
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
        if (qs != null && qs.context != null && qs.context.documentLinkWhiteList != null) {
            linkWhiteList = qs.context.documentLinkWhiteList;
        }

        Map<String, Long> latestVersions = new HashMap<>();
        for (ScoreDoc sd : hits) {
            if (uniques.size() >= resultLimit) {
                break;
            }

            lastDocVisited = sd;
            Document d = s.getIndexReader().document(sd.doc, fieldsToLoad);
            String link = d.get(ServiceDocument.FIELD_NAME_SELF_LINK);
            String originalLink = link;

            // ignore results not in supplied white list
            if (linkWhiteList != null && !linkWhiteList.contains(link)) {
                continue;
            }

            IndexableField versionField = d.getField(ServiceDocument.FIELD_NAME_VERSION);
            Long documentVersion = versionField.numericValue().longValue();

            Long latestVersion = null;

            if (hasIncludeAllVersionsOption) {
                // Decorate link with version. If a document is marked deleted, at any version,
                // we will include it in the results
                link = UriUtils.buildPathWithVersion(link, documentVersion);
            } else {
                // We first determine what is the latest document version.
                // We then use the latest version to determine if the current document result is relevant.
                latestVersion = latestVersions.get(link);
                if (latestVersion == null) {
                    latestVersion = getLatestVersion(s, link);
                    latestVersions.put(link, latestVersion);
                }

                if (documentVersion < latestVersion) {
                    continue;
                }

                boolean isDeleted = Action.DELETE.toString().equals(d
                        .get(ServiceDocument.FIELD_NAME_UPDATE_ACTION));

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

            if (checkAndDeleteExpiratedDocuments(link, s, sd.doc, d, queryStartTimeMicros)) {
                // ignore all document versions if the link has expired
                latestVersions.put(link, Long.MAX_VALUE);
                continue;
            }

            if (hasCountOption) {
                // count unique instances of this link
                uniques.add(link);
                // we only want to count the link once, so set version to highest
                latestVersions.put(link, Long.MAX_VALUE);
                continue;
            }

            String json = null;
            ServiceDocument state = null;

            if (options.contains(QueryOption.EXPAND_CONTENT)
                    || options.contains(QueryOption.OWNER_SELECTION)) {
                state = getStateFromLuceneDocument(d, originalLink);
                if (state == null) {
                    // support reading JSON serialized state for backwards compatibility
                    json = d.get(LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE);
                    if (json == null) {
                        continue;
                    }
                } else {
                    json = Utils.toJson(state);
                }
            }

            if (options.contains(QueryOption.OWNER_SELECTION)) {
                if (!processQueryResultsForOwnerSelection(json, state)) {
                    continue;
                }
            }

            if (options.contains(QueryOption.EXPAND_CONTENT) && !rsp.documents.containsKey(link)) {
                if (options.contains(QueryOption.EXPAND_BUILTIN_CONTENT_ONLY)) {
                    ServiceDocument stateClone = new ServiceDocument();
                    state.copyTo(stateClone);
                    rsp.documents.put(link, stateClone);
                } else {
                    JsonObject jo = new JsonParser().parse(json).getAsJsonObject();
                    rsp.documents.put(link, jo);
                }
            }

            if (options.contains(QueryOption.SELECT_LINKS)) {
                state = processQueryResultsForSelectLinks(s, qs, rsp, d, sd.doc, link, state);
            }

            uniques.add(link);
        }

        if (hasCountOption) {
            rsp.documentCount = (long) uniques.size();
        } else {
            rsp.documentLinks.clear();
            rsp.documentLinks.addAll(uniques);
            rsp.documentCount = (long) rsp.documentLinks.size();
        }

        return lastDocVisited;
    }

    private boolean processQueryResultsForOwnerSelection(String json, ServiceDocument state) {
        String documentOwner = null;
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
            QuerySpecification qs, ServiceDocumentQueryResult rsp, Document d, int docId,
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
            String linkValue = d.get(qt.propertyName);
            if (linkValue != null) {
                linksPerDocument.put(qt.propertyName, linkValue);
                rsp.selectedLinks.add(linkValue);
                continue;
            }

            // if there is no stored field with the link term property name, it might be
            // a field with a collection of links. We do not store those in lucene, they are
            // part of the binary serialized state.
            if (state == null) {
                d = s.getIndexReader().document(docId, this.fieldsToLoadWithExpand);
                state = getStateFromLuceneDocument(d, link);
                if (state == null) {
                    logWarning("Skipping link %s, can not find serialized state", link);
                    continue;
                }
            }

            java.lang.reflect.Field linkCollectionField = ReflectionUtils.getField(
                    state.getClass(), qt.propertyName);
            if (linkCollectionField == null) {
                logWarning("Skipping link %s, can not find field", link);
                continue;
            }
            Object fieldValue = linkCollectionField.get(state);
            if (!(fieldValue instanceof Collection<?>)) {
                logWarning("Skipping link %s, field is not a collection", link);
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

    private ServiceDocument getStateFromLuceneDocument(Document doc, String link) {
        BytesRef binaryStateField = doc.getBinaryValue(LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE);
        if (binaryStateField == null) {
            logWarning("State not found for %s", link);
            return null;
        }

        ServiceDocument state = (ServiceDocument) KryoSerializers.deserializeDocument(
                binaryStateField.bytes,
                binaryStateField.offset, binaryStateField.length);
        if (state.documentSelfLink == null) {
            state.documentSelfLink = link;
        }
        if (state.documentKind == null) {
            state.documentKind = Utils.buildKind(state.getClass());
        }
        return state;
    }

    private long getLatestVersion(IndexSearcher s, String link) throws IOException {
        IndexableField versionField;
        long latestVersion;
        TopDocs td = searchByVersion(link, s, null);
        Document latestVersionDoc = s.getIndexReader().document(td.scoreDocs[0].doc,
                this.fieldsToLoadNoExpand);
        versionField = latestVersionDoc.getField(ServiceDocument.FIELD_NAME_VERSION);
        latestVersion = versionField.numericValue().longValue();
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

    private void close(IndexWriter wr) {
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

        s.documentDescription = null;

        Document doc = new Document();

        Field updateActionField = new StoredField(ServiceDocument.FIELD_NAME_UPDATE_ACTION,
                s.documentUpdateAction);
        doc.add(updateActionField);

        addBinaryStateFieldToDocument(s, r.serializedDocument, desc, doc);

        Field selfLinkField = new StringField(ServiceDocument.FIELD_NAME_SELF_LINK,
                link,
                Field.Store.YES);
        doc.add(selfLinkField);
        Field sortedSelfLinkField = new SortedDocValuesField(ServiceDocument.FIELD_NAME_SELF_LINK,
                new BytesRef(link));
        doc.add(sortedSelfLinkField);

        String kind = s.documentKind;
        if (kind != null) {
            Field kindField = new StringField(ServiceDocument.FIELD_NAME_KIND,
                    kind,
                    Field.Store.NO);
            doc.add(kindField);
        }

        if (s.documentAuthPrincipalLink != null) {
            Field principalField = new StringField(ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK,
                    s.documentAuthPrincipalLink,
                    Field.Store.NO);
            doc.add(principalField);
        }

        if (s.documentTransactionId != null) {
            Field transactionField = new StringField(ServiceDocument.FIELD_NAME_TRANSACTION_ID,
                    s.documentTransactionId,
                    Field.Store.NO);
            doc.add(transactionField);
        }

        addNumericField(doc, ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                s.documentUpdateTimeMicros, true);

        if (s.documentExpirationTimeMicros > 0) {
            addNumericField(doc, ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS,
                    s.documentExpirationTimeMicros, true);
        }

        addNumericField(doc, ServiceDocument.FIELD_NAME_VERSION,
                s.documentVersion, true);

        if (desc.propertyDescriptions == null
                || desc.propertyDescriptions.isEmpty()) {
            // no additional property type information, so we will add the
            // document with common fields indexed plus the full body
            addDocumentToIndex(updateOp, doc, s, desc);
            return;
        }

        addIndexableFieldsToDocument(doc, s, desc);
        addDocumentToIndex(updateOp, doc, s, desc);

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            int fieldCount = doc.getFields().size();
            setTimeSeriesStat(STAT_NAME_INDEXED_FIELD_COUNT, AGGREGATION_TYPE_SUM, fieldCount);
            ServiceStat st = getHistogramStat(STAT_NAME_FIELD_COUNT_PER_DOCUMENT);
            setStat(st, fieldCount);
        }
    }

    private void addBinaryStateFieldToDocument(ServiceDocument s, byte[] serializedDocument,
            ServiceDocumentDescription desc, Document doc) {
        try {
            int count = 0;
            if (serializedDocument == null) {
                Output o = KryoSerializers.serializeDocumentForIndexing(s,
                        desc.serializedStateSizeLimit);
                count = o.position();
                serializedDocument = o.getBuffer();
            } else {
                count = serializedDocument.length;
            }
            Field bodyField = new StoredField(LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE,
                    serializedDocument, 0, count);
            doc.add(bodyField);
        } catch (KryoException ke) {
            throw new IllegalArgumentException(
                    "Failure serializing state of service " + s.documentSelfLink
                            + ", possibly due to size limit."
                            + " Service author should override getDocumentTemplate() and adjust"
                            + " ServiceDocumentDescription.serializedStateSizeLimit. Cause: "
                            + ke.toString());
        }
    }

    private void addIndexableFieldsToDocument(Document doc, Object podo,
            ServiceDocumentDescription sd) {
        for (Entry<String, PropertyDescription> e : sd.propertyDescriptions.entrySet()) {
            String name = e.getKey();
            PropertyDescription pd = e.getValue();
            if (pd.usageOptions != null
                    && pd.usageOptions.contains(PropertyUsageOption.INFRASTRUCTURE)) {
                continue;
            }
            Object v = ReflectionUtils.getPropertyValue(pd, podo);
            addIndexableFieldToDocument(doc, v, pd, name);
        }
    }

    /**
     * Add single indexable field to the Lucene {@link Document}.
     * This function recurses if the field value is a PODO, map, array, or collection.
     */
    private void addIndexableFieldToDocument(Document doc, Object podo, PropertyDescription pd,
            String fieldName) {
        Field luceneField = null;
        Field luceneDocValuesField = null;
        Field.Store fsv = Field.Store.NO;
        boolean isSorted = false;
        boolean expandField = false;
        Object v = podo;
        if (v == null) {
            return;
        }

        EnumSet<PropertyIndexingOption> opts = pd.indexingOptions;

        if (opts != null) {
            if (opts.contains(PropertyIndexingOption.STORE_ONLY)) {
                return;
            }
            if (opts.contains(PropertyIndexingOption.SORT)) {
                isSorted = true;
            }
            if (opts.contains(PropertyIndexingOption.EXPAND)) {
                expandField = true;
            }
        }

        if (pd.usageOptions != null) {
            if (pd.usageOptions.contains(PropertyUsageOption.LINK)) {
                fsv = Field.Store.YES;
            }
            if (pd.usageOptions.contains(PropertyUsageOption.LINKS)) {
                expandField = true;
            }
        }

        boolean isStored = fsv == Field.Store.YES;

        if (v instanceof String) {
            String stringValue = v.toString();
            if (opts == null) {
                luceneField = new StringField(fieldName, stringValue, fsv);
            } else {
                if (opts.contains(PropertyIndexingOption.CASE_INSENSITIVE)) {
                    stringValue = stringValue.toLowerCase();
                }
                if (opts.contains(PropertyIndexingOption.TEXT)) {
                    luceneField = new TextField(fieldName, stringValue, fsv);
                } else {
                    luceneField = new StringField(fieldName, stringValue, fsv);
                }
            }
            if (isSorted) {
                luceneDocValuesField = new SortedDocValuesField(fieldName, new BytesRef(
                        stringValue));
            }
        } else if (v instanceof URI) {
            String uriValue = QuerySpecification.toMatchValue((URI) v);
            luceneField = new StringField(fieldName, uriValue, fsv);
            if (isSorted) {
                luceneDocValuesField = new SortedDocValuesField(fieldName, new BytesRef(
                        v.toString()));
            }
        } else if (pd.typeName.equals(TypeName.ENUM)) {
            String enumValue = QuerySpecification.toMatchValue((Enum<?>) v);
            luceneField = new StringField(fieldName, enumValue, fsv);
            if (isSorted) {
                luceneDocValuesField = new SortedDocValuesField(fieldName, new BytesRef(
                        v.toString()));
            }
        } else if (pd.typeName.equals(TypeName.LONG)) {
            long value = ((Number) v).longValue();
            addNumericField(doc, fieldName, value, isStored);
        } else if (pd.typeName.equals(TypeName.DATE)) {
            // Index as microseconds since UNIX epoch
            long value = ((Date) v).getTime() * 1000;
            addNumericField(doc, fieldName, value, isStored);
        } else if (pd.typeName.equals(TypeName.DOUBLE)) {
            double value = ((Number) v).doubleValue();
            addNumericField(doc, fieldName, value, isStored);
        } else if (pd.typeName.equals(TypeName.BOOLEAN)) {
            String booleanValue = QuerySpecification.toMatchValue((boolean) v);
            luceneField = new StringField(fieldName, booleanValue, fsv);
            if (isSorted) {
                luceneDocValuesField = new SortedDocValuesField(fieldName, new BytesRef(
                        (booleanValue)));
            }
        } else if (pd.typeName.equals(TypeName.BYTES)) {
            // Don't store bytes in the index
        } else if (pd.typeName.equals(TypeName.PODO)) {
            // Ignore all complex fields if they are not explicitly marked with EXPAND.
            // We special case all fields of TaskState to ensure task based services have
            // a guaranteed minimum level indexing and query behavior
            if (!(v instanceof TaskState) && !expandField) {
                return;
            }
            addObjectIndexableFieldToDocument(doc, v, pd, fieldName);
            return;
        } else if (expandField && pd.typeName.equals(TypeName.MAP)) {
            addMapIndexableFieldToDocument(doc, v, pd, fieldName);
            return;
        } else if (expandField && (pd.typeName.equals(TypeName.COLLECTION))) {
            addCollectionIndexableFieldToDocument(doc, v, pd, fieldName);
            return;
        } else {
            luceneField = new StringField(fieldName, v.toString(), fsv);
            if (isSorted) {
                luceneDocValuesField = new SortedDocValuesField(fieldName, new BytesRef(
                        v.toString()));
            }
        }

        if (luceneField != null) {
            doc.add(luceneField);
        }

        if (luceneDocValuesField != null) {
            doc.add(luceneDocValuesField);
        }
    }

    private void addObjectIndexableFieldToDocument(Document doc, Object v, PropertyDescription pd,
            String fieldNamePrefix) {
        for (Entry<String, PropertyDescription> e : pd.fieldDescriptions.entrySet()) {
            PropertyDescription fieldDescription = e.getValue();
            if (pd.indexingOptions.contains(PropertyIndexingOption.SORT)) {
                fieldDescription.indexingOptions.add(PropertyIndexingOption.SORT);
            }
            Object fieldValue = ReflectionUtils.getPropertyValue(fieldDescription, v);
            String fieldName = QuerySpecification.buildCompositeFieldName(fieldNamePrefix,
                    e.getKey());
            addIndexableFieldToDocument(doc, fieldValue, fieldDescription, fieldName);
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private void addMapIndexableFieldToDocument(Document doc, Object v, PropertyDescription pd,
            String fieldNamePrefix) {
        final String errorMsg = "Field not supported. Map keys must be of type String.";

        Map m = (Map) v;
        if (pd.indexingOptions.contains(PropertyIndexingOption.SORT)) {
            pd.elementDescription.indexingOptions.add(PropertyIndexingOption.SORT);
        }

        for (Object o : m.entrySet()) {
            Entry entry = (Entry) o;
            Object mapKey = entry.getKey();
            if (!(mapKey instanceof String)) {
                throw new IllegalArgumentException(errorMsg);
            }

            addIndexableFieldToDocument(doc, entry.getValue(), pd.elementDescription,
                    QuerySpecification.buildCompositeFieldName(fieldNamePrefix, (String) mapKey));

            if (pd.indexingOptions.contains(PropertyIndexingOption.FIXED_ITEM_NAME)) {
                addIndexableFieldToDocument(doc, entry.getKey(), new PropertyDescription(),
                        fieldNamePrefix);

                addIndexableFieldToDocument(doc, entry.getValue(), pd.elementDescription,
                        fieldNamePrefix);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private void addCollectionIndexableFieldToDocument(Document doc, Object v,
            PropertyDescription pd, String fieldNamePrefix) {
        fieldNamePrefix = QuerySpecification.buildCollectionItemName(fieldNamePrefix);

        Collection c;
        if (v instanceof Collection) {
            c = (Collection) v;
        } else {
            c = Arrays.asList((Object[]) v);
        }
        if (pd.indexingOptions.contains(PropertyIndexingOption.SORT)) {
            pd.elementDescription.indexingOptions.add(PropertyIndexingOption.SORT);
        }
        for (Object cv : c) {
            if (cv == null) {
                continue;
            }

            addIndexableFieldToDocument(doc, cv, pd.elementDescription, fieldNamePrefix);
        }
    }

    private boolean checkAndDeleteExpiratedDocuments(String link, IndexSearcher searcher,
            Integer docId,
            Document doc, long now)
            throws Throwable {
        long expiration = 0;
        boolean hasExpired = false;
        IndexableField expirationValue = doc
                .getField(ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS);
        if (expirationValue != null) {
            expiration = expirationValue.numericValue().longValue();
            hasExpired = expiration <= now;
        }

        if (!hasExpired) {
            return false;
        }

        adjustTimeSeriesStat(STAT_NAME_DOCUMENT_EXPIRATION_COUNT, AGGREGATION_TYPE_SUM,
                1);

        // update document with one that has all fields, including binary state
        doc = searcher.getIndexReader().document(docId, this.fieldsToLoadWithExpand);

        ServiceDocument s = null;
        try {
            s = getStateFromLuceneDocument(doc, link);
        } catch (Throwable e) {
            logWarning("Error deserializing state for %s: %s", link, e.getMessage());
        }

        deleteAllDocumentsForSelfLink(searcher, Operation.createDelete(null), link, s);
        return true;
    }

    private void checkDocumentRetentionLimit(ServiceDocument state,
            ServiceDocumentDescription desc) {
        if (desc.versionRetentionLimit == ServiceDocumentDescription.FIELD_VALUE_DISABLED_VERSION_RETENTION) {
            return;
        }
        synchronized (this.linkDocumentRetentionEstimates) {
            if (this.linkDocumentRetentionEstimates.containsKey(state.documentSelfLink)) {
                return;
            }

            long limit = Math.max(1, desc.versionRetentionLimit);
            if (state.documentVersion < limit) {
                return;
            }

            // schedule this self link for retention policy: it might have exceeded the version limit
            this.linkDocumentRetentionEstimates.put(state.documentSelfLink, limit);
        }
    }

    /**
     * Will attempt to re-open index writer to recover from a specific exception. The method
     * assumes the caller has acquired the writer semaphore
     */
    private void checkFailureAndRecover(Throwable e) {
        if (!(e instanceof AlreadyClosedException)) {
            if (this.writer != null && !getHost().isStopping()) {
                logSevere(e);
            }
            return;
        }

        this.adjustStat(STAT_NAME_WRITER_ALREADY_CLOSED_EXCEPTION_COUNT, 1);
        applyFileLimitRefreshWriter(true);
    }

    private void deleteAllDocumentsForSelfLink(IndexSearcher s, Operation postOrDelete, String link,
            ServiceDocument state)
            throws Throwable {
        deleteDocumentsFromIndex(s, postOrDelete, link, 0);
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

    /**
     * Deletes all indexed documents with range of deleteCount,indexed with the specified self link
     *
     * @throws Throwable
     */
    private void deleteDocumentsFromIndex(IndexSearcher s, Operation delete, String link,
            long versionsToKeep) throws Throwable {
        IndexWriter wr = this.writer;
        if (wr == null) {
            delete.fail(new CancellationException());
            return;
        }

        Query linkQuery = new TermQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK,
                link));

        TopDocs results;

        results = s.search(linkQuery, Integer.MAX_VALUE, this.versionSort, false, false);
        if (results == null) {
            return;
        }

        ScoreDoc[] hits = results.scoreDocs;

        if (hits == null || hits.length == 0) {
            return;
        }

        Document hitDoc;

        if (versionsToKeep == 0) {
            // we are asked to delete everything, no need to sort or query
            wr.deleteDocuments(linkQuery);
            this.writerUpdateTimeMicros = Utils.getNowMicrosUtc();
            delete.complete();
            return;
        }

        int versionCount = hits.length;

        hitDoc = s.doc(hits[versionCount - 1].doc);
        long versionLowerBound = Long.parseLong(hitDoc.get(ServiceDocument.FIELD_NAME_VERSION));

        hitDoc = s.doc(hits[0].doc);
        long versionUpperBound = Long.parseLong(hitDoc.get(ServiceDocument.FIELD_NAME_VERSION));

        // If the number of versions found are already less than the limit
        // then there is nothing to delete. Just exit.
        if (versionCount <= versionsToKeep) {
            return;
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        // grab the document at the tail of the results, and use it to form a new query
        // that will delete all documents from that document up to the version at the
        // retention limit
        hitDoc = s.doc(hits[(int) versionsToKeep].doc);
        long cutOffVersion = Long.parseLong(hitDoc.get(ServiceDocument.FIELD_NAME_VERSION));

        Query versionQuery = LongPoint.newRangeQuery(
                ServiceDocument.FIELD_NAME_VERSION, versionLowerBound, cutOffVersion);

        builder.add(versionQuery, Occur.MUST);
        builder.add(linkQuery, Occur.MUST);
        BooleanQuery bq = builder.build();

        results = s.search(bq, Integer.MAX_VALUE);

        logInfo("Version grooming for %s found %d versions from %d to %d. Trimming %d versions from %d to %d",
                link, versionCount, versionLowerBound, versionUpperBound,
                results.scoreDocs.length, versionLowerBound, cutOffVersion);

        wr.deleteDocuments(bq);

        // We have observed that sometimes Lucene search does not return all the document
        // versions in the index. Normally, the number of documents returned should be
        // equal to or more than the delta between the lower and upper versions. It can be more
        // because of duplicate document versions. If that's not the case, we add the
        // link back for retention so that the next grooming run can cleanup the missed document.
        if (versionCount < versionUpperBound - versionLowerBound + 1) {
            logWarning("Adding %s back for version grooming since versionCount %d " +
                            "was lower than version delta from %d to %d.",
                    link, versionCount, versionLowerBound, versionUpperBound);
            synchronized (this.linkDocumentRetentionEstimates) {
                this.linkDocumentRetentionEstimates.put(link, versionsToKeep);
            }
        }

        long now = Utils.getNowMicrosUtc();

        // Use time AFTER index was updated to be sure that it can be compared
        // against the time the searcher was updated and have this change
        // be reflected in the new searcher. If the start time would be used,
        // it is possible to race with updating the searcher and NOT have this
        // change be reflected in the searcher.
        updateLinkAccessTime(now, link);

        delete.complete();
    }

    private void addDocumentToIndex(Operation op, Document doc, ServiceDocument sd,
            ServiceDocumentDescription desc) throws IOException {
        IndexWriter wr = this.writer;
        if (wr == null) {
            op.fail(new CancellationException());
            return;
        }

        long startNanos = System.nanoTime();

        wr.addDocument(doc);
        long durationNanos = System.nanoTime() - startNanos;
        setTimeSeriesStat(STAT_NAME_INDEXED_DOCUMENT_COUNT, AGGREGATION_TYPE_SUM, 1);
        setTimeSeriesHistogramStat(STAT_NAME_INDEXING_DURATION_MICROS, AGGREGATION_TYPE_AVG_MAX,
                TimeUnit.NANOSECONDS.toMicros(durationNanos));

        // Use time AFTER index was updated to be sure that it can be compared
        // against the time the searcher was updated and have this change
        // be reflected in the new searcher. If the start time would be used,
        // it is possible to race with updating the searcher and NOT have this
        // change be reflected in the searcher.
        updateLinkAccessTime(Utils.getNowMicrosUtc(), sd.documentSelfLink);

        op.setBody(null).complete();
        checkDocumentRetentionLimit(sd, desc);
        applyActiveQueries(op, sd, desc);
    }

    private void updateLinkAccessTime(long t, String link) {
        synchronized (this.searchSync) {
            // This map is cleared in applyMemoryLimit while holding this lock,
            // so it is added to here while also holding the lock for symmetry.
            this.linkAccessTimes.put(link, t);

            // The index update time may only be increased.
            if (this.writerUpdateTimeMicros < t) {
                this.writerUpdateTimeMicros = t;
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
    private IndexSearcher createOrRefreshSearcher(String selfLink, int resultLimit, IndexWriter w,
            boolean doNotRefresh)
            throws IOException {

        IndexSearcher s;
        boolean needNewSearcher = false;
        long threadId = Thread.currentThread().getId();
        long now = Utils.getNowMicrosUtc();
        synchronized (this.searchSync) {
            s = this.searchers.get(threadId);
            if (s == null) {
                needNewSearcher = true;
            } else if (selfLink != null && resultLimit == 1) {
                Long perLinkUpdateTime = this.linkAccessTimes.get(selfLink);
                if (perLinkUpdateTime != null
                        && perLinkUpdateTime.compareTo(this.searcherUpdateTimeMicros.get()) >= 0) {
                    needNewSearcher = !doNotRefresh;
                }
            } else if (this.searcherUpdateTimeMicros.get() < this.writerUpdateTimeMicros) {
                needNewSearcher = !doNotRefresh;
            }
        }

        if (s != null && !needNewSearcher) {
            return s;
        }

        if (s != null) {
            s.getIndexReader().close();
        }

        s = new IndexSearcher(DirectoryReader.open(w, true, true));

        adjustTimeSeriesStat(STAT_NAME_SEARCHER_UPDATE_COUNT, AGGREGATION_TYPE_SUM, 1);
        synchronized (this.searchSync) {
            this.searchers.put(threadId, s);
            this.searcherUpdateTimeMicros.set(now);
            return s;
        }
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public void handleMaintenance(final Operation post) {
        this.privateIndexingExecutor.execute(() -> {
            try {
                this.writerSync.acquire();
                handleMaintenanceImpl(false);
                post.complete();
            } catch (Throwable e) {
                post.fail(e);
            } finally {
                this.writerSync.release();
            }

        });
    }

    private void handleMaintenanceImpl(boolean forceMerge) throws Throwable {
        try {
            IndexWriter w = this.writer;
            if (w == null) {
                return;
            }

            IndexSearcher s = createOrRefreshSearcher(null, Integer.MAX_VALUE, w, false);

            applyDocumentExpirationPolicy(s);
            applyDocumentVersionRetentionPolicy();
            applyMemoryLimit();
            applyIndexUpdates(w);

            applyFileLimitRefreshWriter(false);
        } catch (Throwable e) {
            if (this.getHost().isStopping()) {
                return;
            }
            logWarning("Attempting recovery due to error: %s", Utils.toString(e));
            applyFileLimitRefreshWriter(true);
            throw e;
        }
    }

    void applyIndexUpdates(IndexWriter w) throws IOException {
        long startNanos = System.nanoTime();
        w.commit();
        long durationNanos = System.nanoTime() - startNanos;
        setTimeSeriesHistogramStat(STAT_NAME_COMMIT_DURATION_MICROS, AGGREGATION_TYPE_AVG_MAX,
                TimeUnit.NANOSECONDS.toMicros(durationNanos));
        adjustTimeSeriesStat(STAT_NAME_COMMIT_COUNT, AGGREGATION_TYPE_SUM, 1);
    }

    private void applyFileLimitRefreshWriter(boolean force) {
        if (getHost().isStopping()) {
            return;
        }

        long now = Utils.getNowMicrosUtc();
        if (now - this.writerCreationTimeMicros < getHost()
                .getMaintenanceIntervalMicros()) {
            logInfo("Skipping writer re-open, it was created recently");
            return;
        }

        File directory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory);
        long count;
        try {
            count = Files.list(directory.toPath()).count();
            if (!force && count < INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH) {
                return;
            }
        } catch (IOException e1) {
            logSevere(e1);
            return;
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

            logInfo("(%s) closing all readers, document count: %d, file count: %d",
                    this.writerSync, w.maxDoc(), count);

            for (IndexSearcher s : this.searchers.values()) {
                s.getIndexReader().close();
            }

            this.searchers.clear();

            if (!force) {
                return;
            }

            try {
                w.close();
            } catch (Throwable e) {
            }

            w = createWriter(directory, false);
            count = Files.list(directory.toPath()).count();
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
        }
    }

    private void applyDocumentVersionRetentionPolicy()
            throws Throwable {

        Operation dummyDelete = Operation.createDelete(null);
        int count = 0;
        Map<String, Long> links = new HashMap<>();
        synchronized (this.linkDocumentRetentionEstimates) {
            links.putAll(this.linkDocumentRetentionEstimates);
            this.linkDocumentRetentionEstimates.clear();
        }

        for (Entry<String, Long> e : links.entrySet()) {
            IndexWriter wr = this.writer;
            if (wr == null) {
                return;
            }
            IndexSearcher s = createOrRefreshSearcher(null, Integer.MAX_VALUE, wr, false);
            deleteDocumentsFromIndex(s, dummyDelete, e.getKey(), e.getValue());
            count++;
        }

        if (!links.isEmpty()) {
            logInfo("Applied retention policy to %d links", count);
        }
    }

    private void applyMemoryLimit() {
        if (getHost().isStopping()) {
            return;
        }
        // close any paginated query searchers that have expired
        long now = Utils.getNowMicrosUtc();
        applyMemoryLimitToLinkAccessTimes();

        Map<Long, List<IndexSearcher>> entriesToClose = new HashMap<>();
        synchronized (this.searchSync) {
            Iterator<Entry<Long, List<IndexSearcher>>> itr = this.searchersForPaginatedQueries
                    .entrySet().iterator();
            while (itr.hasNext()) {
                Entry<Long, List<IndexSearcher>> entry = itr.next();
                if (entry.getKey() > now) {
                    // all entries beyond this one, are in the future, since we use a sorted tree map
                    break;
                }
                entriesToClose.put(entry.getKey(), entry.getValue());
                itr.remove();
            }
            setTimeSeriesStat(STAT_NAME_ACTIVE_PAGINATED_QUERIES, AGGREGATION_TYPE_AVG_MAX,
                    this.searchersForPaginatedQueries.size());
        }

        for (Entry<Long, List<IndexSearcher>> entry : entriesToClose.entrySet()) {
            List<IndexSearcher> searchers = entry.getValue();
            for (IndexSearcher s : searchers) {
                try {
                    logFine("Closing paginated query searcher, expired at %d", entry.getKey());
                    s.getIndexReader().close();
                } catch (Throwable e) {

                }
            }
        }
    }

    void applyMemoryLimitToLinkAccessTimes() {
        long memThresholdBytes = this.linkAccessMemoryLimitMB * 1024 * 1024;
        final int bytesPerLinkEstimate = 256;
        int count = 0;

        // Note: this code will be updated in the future. It currently calls a host
        // method, inside a lock, which is always a bad idea. The getServiceStage()
        // method is lock free, but its still brittle. We can eliminate the linkAccessTimes
        // list all together/
        // TODO https://www.pivotaltracker.com/story/show/133390149
        synchronized (this.searchSync) {
            if (this.linkAccessTimes.isEmpty()) {
                return;
            }
            if (memThresholdBytes > this.linkAccessTimes.size() * bytesPerLinkEstimate) {
                return;
            }
            count = this.linkAccessTimes.size();
            Iterator<Entry<String, Long>> li = this.linkAccessTimes.entrySet().iterator();
            while (li.hasNext()) {
                Entry<String, Long> e = li.next();
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
        logInfo("Cleared %d link access times", count);
    }

    private void applyDocumentExpirationPolicy(IndexSearcher s) throws Throwable {
        long expirationUpperBound = Utils.getNowMicrosUtc();

        Query versionQuery = LongPoint.newRangeQuery(
                ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS, 1L, expirationUpperBound);

        TopDocs results = s.search(versionQuery, EXPIRED_DOCUMENT_SEARCH_THRESHOLD);
        if (results.totalHits == 0) {
            return;
        }

        if (results.totalHits > EXPIRED_DOCUMENT_SEARCH_THRESHOLD) {
            adjustTimeSeriesStat(STAT_NAME_DOCUMENT_EXPIRATION_FORCED_MAINTENANCE_COUNT,
                    AGGREGATION_TYPE_SUM, 1);
        }

        // The expiration query will return all versions for a link. Use a set so we only delete once per link
        Set<String> links = new HashSet<>();
        long now = Utils.getNowMicrosUtc();
        for (ScoreDoc sd : results.scoreDocs) {
            Document d = s.getIndexReader().document(sd.doc, this.fieldsToLoadNoExpand);
            String link = d.get(ServiceDocument.FIELD_NAME_SELF_LINK);
            IndexableField versionField = d.getField(ServiceDocument.FIELD_NAME_VERSION);
            long versionExpired = versionField.numericValue().longValue();
            long latestVersion = this.getLatestVersion(s, link);
            if (versionExpired < latestVersion) {
                continue;
            }
            if (!links.add(link)) {
                continue;
            }
            checkAndDeleteExpiratedDocuments(link, s, sd.doc, d, now);
        }
    }

    private void applyActiveQueries(Operation op, ServiceDocument latestState,
            ServiceDocumentDescription desc) {
        if (this.activeQueries.isEmpty()) {
            return;
        }

        // set current context from the operation so all acive query task notifications carry the
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
            if (activeTask.querySpec.options.contains(QueryOption.EXPAND_CONTENT)) {
                patchBody.results.documents = new HashMap<>();
                patchBody.results.documents.put(latestState.documentSelfLink, latestState);
            }

            // Send PATCH to continuous query task with document that passed the query filter.
            // Any subscribers will get notified with the body containing just this document
            sendRequest(Operation.createPatch(this, activeTask.documentSelfLink).setBodyNoCloning(
                    patchBody));
        }
    }

    public static Document addNumericField(Document doc, String propertyName, long propertyValue,
            boolean stored) {
        // StoredField is used if the property needs to be stored in the lucene document
        if (stored) {
            doc.add(new StoredField(propertyName, propertyValue));
        }

        // LongPoint adds an index field to the document that allows for efficient search
        // and range queries
        doc.add(new LongPoint(propertyName, propertyValue));

        // NumericDocValues allow for efficient group operations for a property.
        // TODO Investigate and revert code to use 'sort' to determine the type of DocValuesField
        doc.add(new NumericDocValuesField(propertyName, propertyValue));
        return doc;
    }

    public static Document addNumericField(Document doc, String propertyName, double propertyValue,
            boolean stored) {
        long longPropertyValue = NumericUtils.doubleToSortableLong(propertyValue);

        // StoredField is used if the property needs to be stored in the lucene document
        if (stored) {
            doc.add(new StoredField(propertyName, propertyValue));
        }

        // DoublePoint adds an index field to the document that allows for efficient search
        // and range queries
        doc.add(new DoublePoint(propertyName, propertyValue));

        // NumericDocValues allow for efficient group operations for a property.
        // TODO Investigate and revert code to use 'sort' to determine the type of DocValuesField
        doc.add(new NumericDocValuesField(propertyName, longPropertyValue));
        return doc;
    }
}
