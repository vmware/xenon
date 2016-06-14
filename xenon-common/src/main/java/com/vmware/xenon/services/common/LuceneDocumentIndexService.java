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
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.esotericsoftware.kryo.KryoException;
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
import com.vmware.xenon.common.QueryFilterUtils;
import com.vmware.xenon.common.ReflectionUtils;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.MemoryLimitType;
import com.vmware.xenon.common.ServiceMaintenanceRequest;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryPageService.LuceneQueryPage;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

public class LuceneDocumentIndexService extends StatelessService {

    public static String SELF_LINK = ServiceUriPaths.CORE_DOCUMENT_INDEX;

    public static final String FILE_PATH_LUCENE = "lucene";

    private String indexDirectory;


    private static final int DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH = 10000;

    private static final int DEFAULT_INDEX_SEARCHER_COUNT_THRESHOLD = 200;

    private static int EXPIRED_DOCUMENT_SEARCH_THRESHOLD = 1000;

    private static int INDEX_SEARCHER_COUNT_THRESHOLD = DEFAULT_INDEX_SEARCHER_COUNT_THRESHOLD;

    private static int INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH = DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH;

    public static void setSearcherCountThreshold(int count) {
        INDEX_SEARCHER_COUNT_THRESHOLD = count;
    }

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

    public static final String STAT_NAME_QUERY_DURATION_MICROS = "queryDurationMicros";

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

    protected static final int UPDATE_THREAD_COUNT = 4;

    protected static final int QUERY_THREAD_COUNT = 2;

    protected Object searchSync;
    protected Queue<IndexSearcher> searchersPendingClose = new ConcurrentLinkedQueue<>();
    protected TreeMap<Long, List<IndexSearcher>> searchersForPaginatedQueries = new TreeMap<>();
    protected IndexSearcher searcher = null;
    protected IndexWriter writer = null;
    protected final Semaphore writerAvailable = new Semaphore(
            UPDATE_THREAD_COUNT + QUERY_THREAD_COUNT);

    protected Map<String, QueryTask> activeQueries = new ConcurrentSkipListMap<>();

    private long searcherUpdateTimeMicros;

    private long indexUpdateTimeMicros;

    private long indexWriterCreationTimeMicros;

    private final Map<String, Long> linkAccessTimes = new HashMap<>();
    private final Map<String, Long> linkDocumentRetentionEstimates = new HashMap<>();
    private long linkAccessMemoryLimitMB;

    private Sort versionSort;

    private ExecutorService privateIndexingExecutor;

    private ExecutorService privateQueryExecutor;

    private Set<String> fieldsToLoadNoExpand;
    private Set<String> fieldsToLoadWithExpand;

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
                r -> new Thread(r, getUri() + "/queries/" + Utils.getNowMicrosUtc()));
        this.privateIndexingExecutor = Executors.newFixedThreadPool(UPDATE_THREAD_COUNT,
                r -> new Thread(r, getSelfLink() + "/updates/" + Utils.getNowMicrosUtc()));

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

        post.complete();
    }

    private void initializeInstance() {
        this.searchSync = new Object();
        this.searcher = null;
        this.searchersForPaginatedQueries.clear();
        this.searchersPendingClose.clear();

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


        this.writer = new IndexWriter(dir, iwc);
        this.writer.commit();
        this.linkAccessTimes.clear();
        this.indexUpdateTimeMicros = Utils.getNowMicrosUtc();
        this.indexWriterCreationTimeMicros = this.indexUpdateTimeMicros;
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
            this.indexUpdateTimeMicros = Utils.getNowMicrosUtc();
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
        queryIndex(op, options, s, tq, null, null, Integer.MAX_VALUE, 0,
                null, rsp, null);
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

            this.writerAvailable.acquire(semaphoreCount);
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
            this.indexUpdateTimeMicros = Utils.getNowMicrosUtc();
            createWriter(directory, true);
            op.complete();
            this.logInfo("restore complete");
        } catch (Throwable e) {
            logSevere(e);
            op.fail(e);
        } finally {
            this.writerAvailable.release(semaphoreCount);
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

        ExecutorService exec = a == Action.GET ? this.privateQueryExecutor
                : this.privateIndexingExecutor;
        if (exec.isShutdown()) {
            op.fail(new CancellationException());
            return;
        }
        exec.execute(() -> {
            try {
                this.writerAvailable.acquire();
                switch (a) {
                case DELETE:
                    handleDeleteImpl(op);
                    break;
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
                case POST:
                    updateIndex(op);
                    break;
                default:
                    getHost().failRequestActionNotSupported(op);
                    break;
                }
            } catch (Throwable e) {
                checkFailureAndRecover(e);
                op.fail(e);
            } finally {
                this.writerAvailable.release();
            }
        });
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
            luceneSort = LuceneQueryConverter.convertToLuceneSort(task.querySpec);
            task.querySpec.context.nativeSort = luceneSort;
        }

        LuceneQueryPage lucenePage = (LuceneQueryPage) qs.context.nativePage;
        IndexSearcher s = (IndexSearcher) qs.context.nativeSearcher;
        ServiceDocumentQueryResult rsp = new ServiceDocumentQueryResult();

        if (qs.options.contains(QueryOption.CONTINUOUS)) {
            switch (task.taskInfo.stage) {
            case CREATED:
                logWarning("Task %s is in invalid state: %s", task.taskInfo.stage);
                op.fail(new IllegalStateException("Stage not supported"));
                return;
            case STARTED:
                QueryTask clonedTask = new QueryTask();
                clonedTask.documentSelfLink = task.documentSelfLink;
                clonedTask.querySpec = task.querySpec;
                clonedTask.querySpec.context.filter = QueryFilter.create(qs.query);
                this.activeQueries.put(task.documentSelfLink, clonedTask);
                this.setStat(STAT_NAME_ACTIVE_QUERY_FILTERS, this.activeQueries.size());
                logInfo("Activated continuous query task: %s", task.documentSelfLink);
                break;
            case CANCELLED:
            case FAILED:
            case FINISHED:
                this.activeQueries.remove(task.documentSelfLink);
                this.setStat(STAT_NAME_ACTIVE_QUERY_FILTERS, this.activeQueries.size());
                op.complete();
                return;
            default:
                break;

            }
        }

        if (s == null && qs.resultLimit != null && qs.resultLimit > 0
                && qs.resultLimit != Integer.MAX_VALUE
                && !qs.options.contains(QueryOption.TOP_RESULTS)) {
            // this is a paginated query. If this is the start of the query, create a dedicated searcher
            // for this query and all its pages. It will be expired when the query task itself expires
            s = createPaginatedQuerySearcher(task.documentExpirationTimeMicros, this.writer);
        }

        if (!queryIndex(s, op, null, qs.options, luceneQuery, luceneSort, lucenePage,
                qs.resultLimit,
                task.documentExpirationTimeMicros, task.indexLink, rsp, qs)) {
            op.setBodyNoCloning(rsp).complete();
        }
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
        Map<String, String> params = UriUtils.parseUriQueryParams(get.getUri());
        String cap = params.get(UriUtils.URI_PARAM_CAPABILITY);
        EnumSet<QueryOption> options = EnumSet.noneOf(QueryOption.class);
        ServiceOption targetIndex = ServiceOption.NONE;
        Long version = null;

        if (cap != null) {
            targetIndex = ServiceOption.valueOf(cap);
        }

        if (params.containsKey(UriUtils.URI_PARAM_INCLUDE_DELETED)) {
            options.add(QueryOption.INCLUDE_DELETED);
        }

        if (params.containsKey(ServiceDocument.FIELD_NAME_VERSION)) {
            version = Long.parseLong(params.get(ServiceDocument.FIELD_NAME_VERSION));
        }

        String selfLink = params.get(ServiceDocument.FIELD_NAME_SELF_LINK);
        String fieldToExpand = params.get(UriUtils.URI_PARAM_ODATA_EXPAND);
        if (fieldToExpand == null) {
            fieldToExpand = params.get(UriUtils.URI_PARAM_ODATA_EXPAND_NO_DOLLAR_SIGN);
        }
        if (fieldToExpand != null
                && fieldToExpand.equals(ServiceDocumentQueryResult.FIELD_NAME_DOCUMENT_LINKS)) {
            options.add(QueryOption.EXPAND_CONTENT);
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
        if (queryIndex(null, get, selfLink, options, tq,
                null, null, resultLimit, 0, null, rsp, null)) {
            return;
        }

        if (targetIndex == ServiceOption.PERSISTENCE) {
            // specific index requested but no results, return empty response
            get.setBodyNoCloning(rsp).complete();
            return;
        }

        // no results in the index, search the service host started services
        queryServiceHost(selfLink + UriUtils.URI_WILDCARD_CHAR, options, get);
    }

    private boolean queryIndex(
            IndexSearcher s,
            Operation op,
            String selfLinkPrefix,
            EnumSet<QueryOption> options,
            Query tq,
            Sort sort,
            LuceneQueryPage page,
            int count,
            long expiration,
            String indexLink,
            ServiceDocumentQueryResult rsp,
            QuerySpecification qs) throws Throwable {
        if (options == null) {
            options = EnumSet.noneOf(QueryOption.class);
        }

        if (options.contains(QueryOption.COUNT) && options.contains(QueryOption.EXPAND_CONTENT)) {
            op.fail(new IllegalArgumentException("COUNT can not be combined with EXPAND: %s"
                    + options.toString()));
            return true;
        }

        if (options.contains(QueryOption.EXPAND_CONTENT)) {
            rsp.documents = new HashMap<>();
        }

        if (options.contains(QueryOption.COUNT)) {
            rsp.documentCount = 0L;
            sort = null;
        } else {
            rsp.documentLinks = new ArrayList<>();
        }

        IndexWriter w = this.writer;
        if (w == null) {
            op.fail(new CancellationException());
            return true;
        }

        if (sort == null) {
            sort = this.versionSort;
        }

        if (s == null) {
            // If DO_NOT_REFRESH is set use the existing searcher.
            s = this.searcher;
            if (!options.contains(QueryOption.DO_NOT_REFRESH) || s == null) {
                s = updateSearcher(selfLinkPrefix, count, w);
            }
        }

        tq = updateQuery(op, tq);
        if (tq == null) {
            return false;
        }
        ServiceDocumentQueryResult result = queryIndex(op, options, s, tq, sort, page,
                count, expiration, indexLink, rsp, qs);
        if (result != null) {
            result.documentOwner = getHost().getId();
            if (!options.contains(QueryOption.COUNT) && result.documentLinks.isEmpty()) {
                return false;
            }
            op.setBodyNoCloning(result).complete();
            return true;
        }

        return false;
    }

    private void queryIndexSingle(String selfLink, Operation op, Long version)
            throws Throwable {
        IndexWriter w = this.writer;
        if (w == null) {
            op.fail(new CancellationException());
            return;
        }

        IndexSearcher s = updateSearcher(selfLink, 1, w);
        long start = Utils.getNowMicrosUtc();
        TopDocs hits = searchByVersion(selfLink, s, version);
        long end = Utils.getNowMicrosUtc();
        if (hits.totalHits == 0) {
            op.complete();
            return;
        }

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            ServiceStat st = getHistogramStat(STAT_NAME_QUERY_SINGLE_DURATION_MICROS);
            setStat(st, end - start);
        }

        Document doc = s.getIndexReader().document(hits.scoreDocs[0].doc,
                this.fieldsToLoadWithExpand);

        if (checkAndDeleteExpiratedDocuments(selfLink, s, hits.scoreDocs[0].doc, doc,
                Utils.getNowMicrosUtc())) {
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
    private TopDocs searchByVersion(String selfLink, IndexSearcher s, Long version) throws IOException {
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

    private ServiceDocumentQueryResult queryIndex(Operation op,
            EnumSet<QueryOption> options,
            IndexSearcher s,
            Query tq,
            Sort sort,
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
                return null;
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
                    ServiceStat st = getHistogramStat(statName);
                    setStat(st, queryTime);

                    st = getHistogramStat(STAT_NAME_RESULT_PROCESSING_DURATION_MICROS);
                    setStat(st, end - start);
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
                        rsp.nextPageLink = createNextPage(op, s, qs, tq, sort, bottom, count,
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

    private String createNextPage(Operation op, IndexSearcher s, QuerySpecification qs,
            Query tq,
            Sort sort,
            ScoreDoc after,
            int count,
            long expiration,
            String indexLink,
            boolean hasPage) {

        URI u = UriUtils.buildUri(getHost(), UriUtils.buildUriPath(
                ServiceUriPaths.CORE,
                "query-page",
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
        // referer of that forwarded request is set to original query-page request.
        // This method is called when query-page wants to create new page for a paginated query.
        // If a new page is going to be created then it is safe to use query-page link
        // from referer as previous page link of this new page being created.
        LuceneQueryPage page = new LuceneQueryPage(hasPage ? prevLinkForNewPage : null, after);

        QuerySpecification spec = new QuerySpecification();
        spec.options = qs.options;
        spec.context.nativeQuery = tq;
        spec.context.nativePage = page;
        spec.context.nativeSearcher = s;
        spec.context.nativeSort = sort;
        spec.resultLimit = count;
        spec.linkTerms = qs.linkTerms;

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
        if (options.contains(QueryOption.EXPAND_CONTENT) || options.contains(QueryOption.OWNER_SELECTION)) {
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

        Map<String, Long> latestVersions = new HashMap<>();
        for (ScoreDoc sd : hits) {
            if (uniques.size() >= resultLimit) {
                break;
            }

            lastDocVisited = sd;
            Document d = s.getIndexReader().document(sd.doc, fieldsToLoad);
            String link = d.get(ServiceDocument.FIELD_NAME_SELF_LINK);
            IndexableField versionField = d.getField(ServiceDocument.FIELD_NAME_VERSION);
            Long documentVersion = versionField.numericValue().longValue();

            // We first determine what is the latest document version.
            // We then use the latest version to determine if the current document result is relevant.
            Long latestVersion = latestVersions.get(link);
            if (latestVersion == null) {
                latestVersion = getLatestVersion(s, link);
                latestVersions.put(link, latestVersion);
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
                    if (rsp.selectedLinks != null) {
                        rsp.selectedLinks.remove(link);
                    }
                }
                continue;
            }

            if (!options.contains(QueryOption.INCLUDE_ALL_VERSIONS)) {
                if (documentVersion < latestVersion) {
                    continue;
                }
            } else {
                // decorate link with version
                link = UriUtils.buildPathWithVersion(link, documentVersion);
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

            if (options.contains(QueryOption.EXPAND_CONTENT) || options.contains(QueryOption.OWNER_SELECTION)) {
                state = getStateFromLuceneDocument(d, link);
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

            if (options.contains(QueryOption.EXPAND_CONTENT)) {
                if (!rsp.documents.containsKey(link)) {
                    rsp.documents.put(link, new JsonParser().parse(json).getAsJsonObject());
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
        if (rsp.selectedLinks == null) {
            rsp.selectedLinks = new HashMap<>();
        }
        Map<String, String> linksPerDocument = rsp.selectedLinks.get(link);
        if (linksPerDocument == null) {
            linksPerDocument = new HashMap<>();
            rsp.selectedLinks.put(link, linksPerDocument);
        }

        for (QueryTask.QueryTerm qt : qs.linkTerms) {
            String linkValue = d.get(qt.propertyName);
            if (linkValue != null) {
                linksPerDocument.put(qt.propertyName, linkValue);
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

        ServiceDocument state = (ServiceDocument) Utils.fromDocumentBytes(
                binaryStateField.bytes,
                binaryStateField.offset, binaryStateField.length);
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
                    s.documentUpdateTimeMicros, true, false);

        if (s.documentExpirationTimeMicros > 0) {
            addNumericField(doc, ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS,
                        s.documentExpirationTimeMicros, true, false);
        }

        addNumericField(doc, ServiceDocument.FIELD_NAME_VERSION,
                    s.documentVersion, true, true);

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
            ServiceStat st = getStat(STAT_NAME_INDEXED_FIELD_COUNT);
            adjustStat(st, fieldCount);
            st = getHistogramStat(STAT_NAME_FIELD_COUNT_PER_DOCUMENT);
            setStat(st, fieldCount);
        }
    }

    private void addBinaryStateFieldToDocument(ServiceDocument s, byte[] serializedDocument,
            ServiceDocumentDescription desc, Document doc) {
        try {
            int count = 0;
            if (serializedDocument == null) {
                serializedDocument = Utils.getBuffer(desc.serializedStateSizeLimit);
                count = Utils.toBytes(s, serializedDocument, 0);
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
            if (opts != null && opts.contains(PropertyIndexingOption.TEXT)) {
                luceneField = new TextField(fieldName, v.toString(), fsv);
            } else {
                luceneField = new StringField(fieldName, v.toString(), fsv);
            }
            if (isSorted) {
                luceneDocValuesField = new SortedDocValuesField(fieldName, new BytesRef(
                        v.toString()));
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
            addNumericField(doc, fieldName, value, isStored, isSorted);
        } else if (pd.typeName.equals(TypeName.DATE)) {
            // Index as microseconds since UNIX epoch
            long value = ((Date) v).getTime() * 1000;
            addNumericField(doc, fieldName, value, isStored, isSorted);
        } else if (pd.typeName.equals(TypeName.DOUBLE)) {
            double value = ((Number) v).doubleValue();
            addNumericField(doc, fieldName, value, isStored, isSorted);
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
            addIndexableFieldToDocument(doc,
                    entry.getValue(),
                    pd.elementDescription,
                    QuerySpecification.buildCompositeFieldName(fieldNamePrefix, (String) mapKey));
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

        adjustStat(STAT_NAME_DOCUMENT_EXPIRATION_COUNT, 1);

        // update document with one that has all fields, including binary state
        doc = searcher.getIndexReader().document(docId, this.fieldsToLoadWithExpand);

        ServiceDocument s = null;
        try {
            s = getStateFromLuceneDocument(doc, link);
        } catch (Throwable e) {
            logWarning("Error deserializing state for %s: %s", link, e.getMessage());
        }

        deleteAllDocumentsForSelfLink(Operation.createDelete(null), link, s);
        return true;
    }

    private void checkDocumentRetentionLimit(ServiceDocument state,
            ServiceDocumentDescription desc) {
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
        reOpenWriterSynchronously();
    }

    private void deleteAllDocumentsForSelfLink(Operation postOrDelete, String link,
            ServiceDocument state)
                    throws Throwable {
        deleteDocumentsFromIndex(postOrDelete, link, 0);
        ServiceStat st = getStat(STAT_NAME_SERVICE_DELETE_COUNT);
        adjustStat(st, 1);
        logFine("%s expired", link);
        if (state == null) {
            return;
        }

        applyActiveQueries(state, null);
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
    private void deleteDocumentsFromIndex(Operation delete, String link,
            long versionsToKeep) throws Throwable {
        IndexWriter wr = this.writer;
        if (wr == null) {
            delete.fail(new CancellationException());
            return;
        }

        Query linkQuery = new TermQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK,
                link));

        IndexSearcher s = updateSearcher(link, Integer.MAX_VALUE, wr);
        if (s == null) {
            delete.fail(new CancellationException());
            return;
        }

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
            this.indexUpdateTimeMicros = Utils.getNowMicrosUtc();
            delete.complete();
            return;
        }

        hitDoc = s.doc(hits[hits.length - 1].doc);
        long versionLowerBound = Long.parseLong(hitDoc.get(ServiceDocument.FIELD_NAME_VERSION));

        // if the number of documents found for the passed self-link are already less than the
        // version limit, then skip version retention.
        if (hits.length <= versionsToKeep) {
            hitDoc = s.doc(hits[0].doc);
            long versionUpperBound = Long.parseLong(hitDoc.get(ServiceDocument.FIELD_NAME_VERSION));
            logWarning("Skipping index trimming for %s from %d to %d. query returned :%d",
                    link, versionLowerBound, versionUpperBound, hits.length);

            // Let's make sure the documentSelfLink is registered for retention so that
            // in-case we missed an update because the searcher was stale, we will perform
            // the clean-up in the next handleMaintenance cycle.
            synchronized (this.linkDocumentRetentionEstimates) {
                this.linkDocumentRetentionEstimates.put(link, versionsToKeep);
            }
            return;
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        // grab the document at the tail of the results, and use it to form a new query
        // that will delete all documents from that document up to the version at the
        // retention limit
        hitDoc = s.doc(hits[(int) versionsToKeep].doc);
        long versionUpperBound = Long.parseLong(hitDoc.get(ServiceDocument.FIELD_NAME_VERSION));

        Query versionQuery = LongPoint.newRangeQuery(
                    ServiceDocument.FIELD_NAME_VERSION, versionLowerBound, versionUpperBound);

        builder.add(versionQuery, Occur.MUST);
        builder.add(linkQuery, Occur.MUST);
        BooleanQuery bq = builder.build();

        results = s.search(bq, Integer.MAX_VALUE);

        logInfo("trimming index for %s from %d to %d, query returned %d", link, hits.length,
                versionsToKeep, results.totalHits);

        wr.deleteDocuments(bq);
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

        long start = Utils.getNowMicrosUtc();
        wr.addDocument(doc);
        long end = Utils.getNowMicrosUtc();

        // Use time AFTER index was updated to be sure that it can be compared
        // against the time the searcher was updated and have this change
        // be reflected in the new searcher. If the start time would be used,
        // it is possible to race with updating the searcher and NOT have this
        // change be reflected in the searcher.
        updateLinkAccessTime(end, sd.documentSelfLink);

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            ServiceStat s = getHistogramStat(STAT_NAME_INDEXING_DURATION_MICROS);
            setStat(s, end - start);
        }

        op.setBody(null).complete();
        checkDocumentRetentionLimit(sd, desc);
        applyActiveQueries(sd, desc);
    }

    private void updateLinkAccessTime(long t, String link) {
        synchronized (this.searchSync) {
            // This map is cleared in applyMemoryLimit while holding this lock,
            // so it is added to here while also holding the lock for symmetry.
            this.linkAccessTimes.put(link, t);

            // The index update time may only be increased.
            if (this.indexUpdateTimeMicros < t) {
                this.indexUpdateTimeMicros = t;
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
    private IndexSearcher updateSearcher(String selfLink, int resultLimit, IndexWriter w) throws IOException {
        IndexSearcher s;
        boolean needNewSearcher = false;
        long now = Utils.getNowMicrosUtc();

        synchronized (this.searchSync) {
            s = this.searcher;
            if (s == null) {
                needNewSearcher = true;
            } else if (selfLink != null && resultLimit == 1) {
                Long latestUpdate = this.linkAccessTimes.get(selfLink);
                if (latestUpdate != null
                        && latestUpdate.compareTo(this.searcherUpdateTimeMicros) >= 0) {
                    needNewSearcher = true;
                }
            } else if (this.searcherUpdateTimeMicros < this.indexUpdateTimeMicros) {
                needNewSearcher = true;
            }

            if (!needNewSearcher) {
                return s;
            }
        }

        // Create a new searcher outside the lock. Another thread might race us and
        // also create a searcher, but that is OK: the most recent one will be used.
        s = new IndexSearcher(DirectoryReader.open(w, true, true));

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            ServiceStat st = getStat(STAT_NAME_SEARCHER_UPDATE_COUNT);
            adjustStat(st, 1);
        }

        synchronized (this.searchSync) {
            this.searchersPendingClose.add(s);
            if (this.searcherUpdateTimeMicros < now) {
                this.searcher = s;
                this.searcherUpdateTimeMicros = now;
            }
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
                this.writerAvailable.acquire();
                handleMaintenanceImpl(false);
                post.complete();
            } catch (Throwable e) {
                post.fail(e);
            } finally {
                this.writerAvailable.release();
            }

        });
    }

    private void handleMaintenanceImpl(boolean forceMerge) throws Throwable {
        try {
            long start = Utils.getNowMicrosUtc();

            IndexWriter w = this.writer;
            if (w == null) {
                return;
            }

            setStat(STAT_NAME_INDEXED_DOCUMENT_COUNT, w.maxDoc());

            adjustStat(STAT_NAME_COMMIT_COUNT, 1.0);
            long end = Utils.getNowMicrosUtc();
            setStat(STAT_NAME_COMMIT_DURATION_MICROS, end - start);

            IndexSearcher s = updateSearcher(null, Integer.MAX_VALUE, w);
            if (s == null) {
                return;
            }

            applyDocumentExpirationPolicy(w);
            applyDocumentVersionRetentionPolicy();
            w.commit();

            applyMemoryLimit();

            boolean reOpenWriter = applyIndexSearcherAndFileLimit();

            if (!forceMerge && !reOpenWriter) {
                return;
            }
            reOpenWriterSynchronously();
        } catch (Throwable e) {
            if (this.getHost().isStopping()) {
                return;
            }
            logWarning("Attempting recovery due to error: %s", e.getMessage());
            reOpenWriterSynchronously();
            throw e;
        }
    }

    private boolean applyIndexSearcherAndFileLimit() {
        File directory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory);
        String[] list = directory.list();
        int count = list == null ? 0 : list.length;

        boolean reOpenWriter = count >= INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH;

        int searcherCount = this.searchersPendingClose.size();
        if (searcherCount < INDEX_SEARCHER_COUNT_THRESHOLD && !reOpenWriter) {
            return reOpenWriter;
        }

        // We always close index searchers before re-opening the index writer, otherwise we risk
        // loosing pending commits on writer re-open. Notice this code executes if we either have
        // too many index files on disk, thus we need to re-open the writer to consolidate, or
        // when we have too many pending searchers
        final int acquireReleaseCount = QUERY_THREAD_COUNT + UPDATE_THREAD_COUNT;
        try {
            if (getHost().isStopping()) {
                return false;
            }

            this.writerAvailable.release();
            this.writerAvailable.acquire(acquireReleaseCount);
            this.searcher = null;

            logInfo("Closing %d pending searchers, index file count: %d", searcherCount, count);

            for (IndexSearcher s : this.searchersPendingClose) {
                try {
                    s.getIndexReader().close();
                } catch (Throwable e) {
                }
            }
            this.searchersPendingClose.clear();

            IndexWriter w = this.writer;
            if (w != null) {
                try {
                    w.deleteUnusedFiles();
                } catch (Throwable e) {
                }
            }

        } catch (InterruptedException e1) {
            logSevere(e1);
        } finally {
            // release all but one, so we stay owning one reference to the semaphore
            this.writerAvailable.release(acquireReleaseCount - 1);
        }

        return reOpenWriter;
    }

    private void reOpenWriterSynchronously() {

        final int acquireReleaseCount = QUERY_THREAD_COUNT + UPDATE_THREAD_COUNT;
        try {

            if (getHost().isStopping()) {
                return;
            }

            // Do not proceed unless we have blocked all reader+writer threads. We assume
            // the semaphore is already acquired by the current thread
            this.writerAvailable.release();
            this.writerAvailable.acquire(acquireReleaseCount);

            IndexWriter w = this.writer;

            long now = Utils.getNowMicrosUtc();
            if (now - this.indexWriterCreationTimeMicros < getHost()
                    .getMaintenanceIntervalMicros()) {
                logInfo("Skipping writer re-open, it was created recently");
                return;
            }

            File directory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory);
            try {
                if (w != null) {
                    w.close();
                }
            } catch (Throwable e) {
            }

            w = createWriter(directory, false);
            logInfo("Reopened writer, document count: %d", w.maxDoc());
        } catch (Throwable e) {
            // If we fail to re-open we should stop the host, since we can not recover.
            logSevere(e);
            logWarning("Stopping local host since index is not accessible");
            close(this.writer);
            this.writer = null;
            sendRequest(Operation.createDelete(this, ServiceUriPaths.CORE_MANAGEMENT));
        } finally {
            // release all but one, so we stay owning one reference to the semaphore
            this.writerAvailable.release(acquireReleaseCount - 1);
        }
    }

    private void applyDocumentVersionRetentionPolicy()
            throws Throwable {
        IndexWriter wr = this.writer;
        if (wr == null) {
            return;
        }

        Operation dummyDelete = Operation.createDelete(null);
        int count = 0;
        Map<String, Long> links = new HashMap<>();
        synchronized (this.linkDocumentRetentionEstimates) {
            links.putAll(this.linkDocumentRetentionEstimates);
            this.linkDocumentRetentionEstimates.clear();
        }

        for (Entry<String, Long> e : links.entrySet()) {
            deleteDocumentsFromIndex(dummyDelete, e.getKey(), e.getValue());
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

        long memThresholdBytes = this.linkAccessMemoryLimitMB * 1024 * 1024;
        final int bytesPerLinkEstimate = 256;
        int count = 0;
        synchronized (this.searchSync) {
            if (this.linkAccessTimes.isEmpty()) {
                return;
            }
            if (memThresholdBytes < this.linkAccessTimes.size() * bytesPerLinkEstimate) {
                count = this.linkAccessTimes.size();
                this.linkAccessTimes.clear();
                // force searcher update next time updateSearcher is called
                if (this.searcher != null) {
                    this.searchersPendingClose.add(this.searcher);
                }
                this.searcher = null;
            }
        }

        if (count > 0) {
            logInfo("Cleared %d link access times", count);
        }

        // close any paginated query searchers that have expired
        long now = Utils.getNowMicrosUtc();
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
            setStat(STAT_NAME_ACTIVE_PAGINATED_QUERIES, this.searchersForPaginatedQueries.size());
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

    private void applyDocumentExpirationPolicy(IndexWriter w) throws Throwable {
        // its ok if we miss a document update, we will catch it, and refresh the searcher on the next update or maintenance
        IndexSearcher s =
                this.searcher != null ? this.searcher : updateSearcher(null, Integer.MAX_VALUE, w);
        if (s == null) {
            return;
        }

        long expirationUpperBound = Utils.getNowMicrosUtc();

        Query versionQuery = LongPoint.newRangeQuery(
                    ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS, 1L, expirationUpperBound);

        TopDocs results = s.search(versionQuery, EXPIRED_DOCUMENT_SEARCH_THRESHOLD);
        if (results.totalHits == 0) {
            return;
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

        // More documents to be expired trigger maintenance right away.
        if (results.totalHits > EXPIRED_DOCUMENT_SEARCH_THRESHOLD) {

            adjustStat(STAT_NAME_DOCUMENT_EXPIRATION_FORCED_MAINTENANCE_COUNT, 1);

            ServiceMaintenanceRequest body = ServiceMaintenanceRequest.create();
            Operation servicePost = Operation
                    .createPost(UriUtils.buildUri(getHost(), getSelfLink()))
                    .setReferer(getHost().getUri())
                    .setBody(body);
            // servicePost can be cached
            handleMaintenance(servicePost);
        }
    }

    private void applyActiveQueries(ServiceDocument latestState, ServiceDocumentDescription desc) {
        if (this.activeQueries.isEmpty()) {
            return;
        }

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

    public static Document addNumericField(Document doc, String propertyName,long propertyValue,
                                           boolean stored, boolean sorted) {
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
                                           boolean stored, boolean sorted) {
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
