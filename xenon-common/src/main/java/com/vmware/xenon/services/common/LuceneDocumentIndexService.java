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
import java.util.concurrent.CancellationException;
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
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexUpgrader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
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
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.LuceneQueryPageService.LuceneQueryPage;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

public class LuceneDocumentIndexService extends StatelessService {

    public static String SELF_LINK = ServiceUriPaths.CORE_DOCUMENT_INDEX;

    public static final String FILE_PATH_LUCENE = "lucene";

    private String indexDirectory;

    public static final int INDEX_FILE_COUNT_THRESHOLD_FOR_REOPEN = 1000;

    private static final String LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE = "binarySerializedState";

    private static final String LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE = "jsonSerializedState";

    public static final String STAT_NAME_ACTIVE_QUERY_FILTERS = "activeQueryFilters";

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

    protected static final int UPDATE_THREAD_COUNT = 4;

    protected static final int QUERY_THREAD_COUNT = 2;

    private static final String DELETE_ACTION = Action.DELETE.toString().intern();

    protected final Object searchSync = new Object();
    protected IndexSearcher searcher = null;
    protected IndexWriter writer = null;
    protected final Semaphore writerAvailable = new Semaphore(
            UPDATE_THREAD_COUNT + QUERY_THREAD_COUNT);

    protected Map<String, QueryTask> activeQueries = new ConcurrentSkipListMap<>();

    private long searcherUpdateTimeMicros;

    private long indexUpdateTimeMicros;

    private long indexWriterCreationTimeMicros;

    private final ConcurrentSkipListMap<String, Long> linkAccessTimes = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<String, Long> linkDocumentRetentionEstimates = new ConcurrentSkipListMap<>();

    private Sort versionSort;

    private ExecutorService privateIndexingExecutor;

    private ExecutorService privateQueryExecutor;

    private final FieldType longStoredField = numericDocType(FieldType.NumericType.LONG, true);
    private final FieldType longUnStoredField = numericDocType(FieldType.NumericType.LONG, false);
    private final FieldType doubleStoredField = numericDocType(FieldType.NumericType.DOUBLE, true);
    private final FieldType doubleUnStoredField = numericDocType(FieldType.NumericType.DOUBLE,
            false);

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
        this.versionSort = new Sort(new SortField(ServiceDocument.FIELD_NAME_VERSION,
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

        // create durable index writer
        for (int retryCount = 0; retryCount < 2; retryCount++) {
            try {
                createWriter(directory, true);
                // we do not actually know if the index is OK, until we try to query
                doSelfValidationQuery();
                break;
            } catch (Throwable e) {
                adjustStat(STAT_NAME_INDEX_LOAD_RETRY_COUNT, 1);
                logWarning("failure creating index writer: %s", Utils.toString(e));
                if (retryCount < 1) {
                    close(this.writer);
                    archiveCorruptIndexFiles(directory);
                    continue;
                }
                post.fail(e);
                return;
            }
        }

        post.complete();
    }

    public IndexWriter createWriter(File directory, boolean doUpgrade) throws Exception {
        Directory dir = MMapDirectory.open(directory.toPath());
        Analyzer analyzer = new SimpleAnalyzer();

        // Upgrade the index in place if necessary.
        if (doUpgrade && DirectoryReader.indexExists(dir)) {
            upgradeIndex(dir);
        }

        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        iwc.setIndexDeletionPolicy(new SnapshotDeletionPolicy(
                new KeepOnlyLastCommitDeletionPolicy()));
        Long totalMBs = getHost().getServiceMemoryLimitMB(getSelfLink(), MemoryLimitType.EXACT);
        if (totalMBs != null) {
            // give half to the index, the other half we keep for service caching context
            totalMBs = Math.max(1, totalMBs / 2);
            iwc.setRAMBufferSizeMB(totalMBs);
        }

        this.writer = new IndexWriter(dir, iwc);
        this.writer.commit();
        this.linkAccessTimes.clear();
        this.indexUpdateTimeMicros = Utils.getNowMicrosUtc();
        this.indexWriterCreationTimeMicros = this.indexUpdateTimeMicros;
        return this.writer;
    }

    private void upgradeIndex(Directory dir) throws IOException {
        boolean doUpgrade = false;
        IndexWriterConfig iwc = new IndexWriterConfig(null);

        CheckIndex chkIndex = new CheckIndex(dir);

        try {
            for (CheckIndex.Status.SegmentInfoStatus segmentInfo : chkIndex
                    .checkIndex().segmentInfos) {
                if (!segmentInfo.version.equals(Version.LATEST)) {
                    logInfo("Found Index version %s", segmentInfo.version.toString());
                    doUpgrade = true;
                    break;
                }
            }
        } finally {
            chkIndex.close();
        }

        if (doUpgrade) {
            logInfo("Upgrading index to %s", Version.LATEST.toString());
            new IndexUpgrader(dir, iwc, false).upgrade();
            this.indexUpdateTimeMicros = Utils.getNowMicrosUtc();
        }
    }

    private void archiveCorruptIndexFiles(File directory) {
        File newDirectory = new File(new File(getHost().getStorageSandbox()), FILE_PATH_LUCENE
                + "." + Utils.getNowMicrosUtc());
        try {
            Files.createDirectory(newDirectory.toPath());
            // we assume a flat directory structure for the LUCENE directory
            FileUtils.moveOrDeleteFiles(directory, newDirectory, false);
        } catch (IOException e) {
            logWarning(e.toString());
        }
    }

    private void doSelfValidationQuery() throws Throwable {
        TermQuery tq = new TermQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK, getSelfLink()));
        ServiceDocumentQueryResult rsp = new ServiceDocumentQueryResult();
        queryIndexWithWriter(Operation.createGet(getUri()), EnumSet
                .of(QueryOption.INCLUDE_ALL_VERSIONS), tq,
                null, null, Integer.MAX_VALUE, 0, null, rsp, ServiceOption.PERSISTENCE,
                new IndexSearcher(
                        DirectoryReader.open(this.writer, true)));
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
                    FILE_PATH_LUCENE);

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
            if (directory.exists() && directory.listFiles().length > 0) {
                this.logInfo("archiving existing index %s", directory);
                archiveCorruptIndexFiles(directory);
            }

            this.logInfo("restoring index %s from %s md5sum(%s)", directory, req.backupFile,
                    FileUtils.md5sum(new File(req.backupFile)));
            FileUtils.extractZipArchive(new File(req.backupFile), directory.toPath());
            this.indexUpdateTimeMicros = Utils.getNowMicrosUtc();
            createWriter(directory, true);
            op.complete();
            this.logInfo("restore complete");
        } catch (Exception e) {
            this.logSevere(e);
            op.fail(e);
        } finally {
            this.writerAvailable.release(semaphoreCount);
        }
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

    private void handleQueryTaskPatch(Operation op, QueryTask task) {
        try {
            QueryTask.QuerySpecification qs = task.querySpec;

            Query luceneQuery = (Query) qs.context.nativeQuery;
            Sort luceneSort = (Sort) qs.context.nativeSort;
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

            if (!queryIndex(s, op, null, qs.options, luceneQuery, luceneSort, lucenePage,
                    qs.resultLimit,
                    task.documentExpirationTimeMicros, task.indexLink, rsp)) {
                op.setBodyNoCloning(rsp).complete();
            }
        } catch (Throwable e) {
            logSevere(e);
            op.fail(e);
        }
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
            queryIndexSingle(selfLink, options, get, version);
            return;
        }

        // Self link prefix query, returns all self links with the same prefix. A GET on a
        // factory translates to this query.
        int resultLimit = Integer.MAX_VALUE;
        selfLink = selfLink.substring(0, selfLink.length() - 1);
        Query tq = new PrefixQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK, selfLink));

        ServiceDocumentQueryResult rsp = new ServiceDocumentQueryResult();
        rsp.documentLinks = new ArrayList<>();
        if (queryIndex(null, get, selfLink, options, tq, null, null, resultLimit, 0, null, rsp)) {
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
            ServiceDocumentQueryResult rsp) throws Throwable {
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
        } else if (queryIndexWithWriter(op, options, tq, sort, page, count, expiration, indexLink,
                rsp,
                ServiceOption.PERSISTENCE, s)) {
            // target index had results or request failed
            return true;
        }

        return false;
    }

    private void queryIndexSingle(String selfLink, EnumSet<QueryOption> options, Operation op, Long version)
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

        BytesRef binaryState = doc.getBinaryValue(LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE);

        if (binaryState != null) {
            ServiceDocument state = (ServiceDocument) Utils.fromDocumentBytes(binaryState.bytes,
                    binaryState.offset,
                    binaryState.length);
            op.setBodyNoCloning(state);
        }
        op.complete();
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

        BooleanQuery bq = new BooleanQuery();
        bq.add(tqSelfLink, Occur.MUST);

        if (version != null) {
            NumericRangeQuery<Long> versionQuery = NumericRangeQuery.newLongRange(
                    ServiceDocument.FIELD_NAME_VERSION, version, version,
                    true,
                    true);

            bq.add(versionQuery, Occur.MUST);
        }

        TopDocs hits = s.search(bq, 1, this.versionSort, false, false);
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

    boolean queryIndexWithWriter(Operation op,
            EnumSet<QueryOption> options,
            Query tq,
            Sort sort,
            LuceneQueryPage page,
            int count,
            long expiration,
            String indexLink,
            ServiceDocumentQueryResult rsp,
            ServiceOption targetIndex,
            IndexSearcher s) throws Throwable {
        Object resultBody;

        resultBody = queryIndex(op, targetIndex, options, s, tq, sort, page, count, expiration,
                indexLink, rsp);
        if (count == 1 && resultBody instanceof String) {
            op.setBodyNoCloning(resultBody).complete();
            return true;
        }

        ServiceDocumentQueryResult result = (ServiceDocumentQueryResult) resultBody;
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
        if (ctx.getResourceQuery() == null) {
            rq = new MatchNoDocsQuery();
        } else {
            rq = LuceneQueryConverter.convertToLuceneQuery(ctx.getResourceQuery());
        }

        BooleanQuery bq = new BooleanQuery();
        bq.add(rq, Occur.FILTER);
        bq.add(tq, Occur.FILTER);
        return bq;
    }

    private Object queryIndex(Operation op, ServiceOption targetIndex,
            EnumSet<QueryOption> options,
            IndexSearcher s,
            Query tq,
            Sort sort,
            LuceneQueryPage page,
            int count,
            long expiration,
            String indexLink,
            ServiceDocumentQueryResult rsp) throws Throwable {
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
            rsp.prevPageLink = page.link;
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

            rsp.documentCount = Long.valueOf(0);
            rsp.queryTimeMicros += queryTime;
            ScoreDoc bottom = null;
            if (shouldProcessResults) {
                start = Utils.getNowMicrosUtc();
                bottom = processQueryResults(targetIndex, options, count, s, rsp, hits,
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
                    expiration += queryTime;
                    rsp.nextPageLink = createNextPage(op, s, options, tq, sort, bottom, count,
                            expiration,
                            indexLink,
                            hasPage);
                    break;
                }
            }

            after = bottom;
            resultLimit = count - rsp.documentLinks.size();
        } while (true && resultLimit > 0);

        return rsp;
    }

    private String createNextPage(Operation op, IndexSearcher s, EnumSet<QueryOption> options,
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
        spec.options = options;
        spec.context.nativeQuery = tq;
        spec.context.nativePage = page;
        spec.context.nativeSearcher = s;
        spec.context.nativeSort = sort;
        spec.resultLimit = count;

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

        getHost().startService(startPost, new LuceneQueryPageService(spec, indexLink));
        return nextLink;
    }

    private ScoreDoc processQueryResults(ServiceOption targetIndex, EnumSet<QueryOption> options,
            int resultLimit, IndexSearcher s, ServiceDocumentQueryResult rsp, ScoreDoc[] hits,
            long queryStartTimeMicros) throws Throwable {

        ScoreDoc lastDocVisited = null;
        Set<String> fieldsToLoad = this.fieldsToLoadNoExpand;
        if (options.contains(QueryOption.EXPAND_CONTENT)) {
            fieldsToLoad = this.fieldsToLoadWithExpand;
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

            boolean isDeleted = DELETE_ACTION.equals(d
                    .get(ServiceDocument.FIELD_NAME_UPDATE_ACTION));

            if (isDeleted && !options.contains(QueryOption.INCLUDE_DELETED)) {
                // ignore a document if its marked deleted and it has the latest version
                if (documentVersion >= latestVersion) {
                    uniques.remove(link);
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

            if (options.contains(QueryOption.EXPAND_CONTENT)) {
                String json = null;
                ServiceDocument state = getStateFromLuceneDocument(d, link);
                if (state == null) {
                    // support reading JSON serialized state for backwards compatibility
                    json = d.get(LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE);
                    if (json == null) {
                        continue;
                    }
                } else {
                    json = Utils.toJson(state);
                }
                if (!rsp.documents.containsKey(link)) {
                    rsp.documents.put(link, new JsonParser().parse(json).getAsJsonObject());
                }
            }
            uniques.add(link);
        }

        if (hasCountOption) {
            rsp.documentCount = Long.valueOf(uniques.size());
        } else {
            rsp.documentLinks.clear();
            rsp.documentLinks.addAll(uniques);
            rsp.documentCount = Long.valueOf(rsp.documentLinks.size());
        }

        return lastDocVisited;
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
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_QUEUING)
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

    public static FieldType numericDocType(FieldType.NumericType type, boolean store) {
        FieldType t = new FieldType();
        t.setStored(store);
        t.setDocValuesType(DocValuesType.NUMERIC);
        t.setIndexOptions(IndexOptions.DOCS);
        t.setNumericType(type);
        t.setNumericPrecisionStep(QueryTask.DEFAULT_PRECISION_STEP);
        return t;
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

        addBinaryStateFieldToDocument(s, desc, doc);

        Field selfLinkField = new StringField(ServiceDocument.FIELD_NAME_SELF_LINK,
                link,
                Field.Store.YES);
        doc.add(selfLinkField);
        Field sortedSelfLinkField = new SortedDocValuesField(ServiceDocument.FIELD_NAME_SELF_LINK,
                new BytesRef(link));
        doc.add(sortedSelfLinkField);

        if (s.documentKind != null) {
            Field kindField = new StringField(ServiceDocument.FIELD_NAME_KIND,
                    s.documentKind,
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

        Field timestampField = new LongField(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                s.documentUpdateTimeMicros, this.longStoredField);
        doc.add(timestampField);

        if (s.documentExpirationTimeMicros > 0) {
            Field expirationTimeMicrosField = new LongField(
                    ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS,
                    s.documentExpirationTimeMicros, this.longStoredField);
            doc.add(expirationTimeMicrosField);
        }

        Field versionField = new LongField(ServiceDocument.FIELD_NAME_VERSION,
                s.documentVersion, this.longStoredField);

        doc.add(versionField);

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

    private void addBinaryStateFieldToDocument(ServiceDocument s,
            ServiceDocumentDescription desc, Document doc) {
        try {
            byte[] content = Utils.getBuffer(desc.serializedStateSizeLimit);
            int count = Utils.toBytes(s, content, 0);
            Field bodyField = new StoredField(LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE,
                    content, 0, count);
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
        Object v = podo;
        if (v == null) {
            return;
        }

        EnumSet<PropertyIndexingOption> opts = pd.indexingOptions;

        if (opts != null && opts.contains(PropertyIndexingOption.STORE_ONLY)) {
            return;
        }

        if (opts != null && opts.contains(PropertyIndexingOption.SORT)) {
            isSorted = true;
        }

        boolean expandField = opts != null && opts.contains(PropertyIndexingOption.EXPAND);

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
            if (v instanceof Integer) {
                int i = (int) v;
                v = i * 1L;
            }
            luceneField = new LongField(fieldName, (long) v,
                    fsv == Store.NO ? this.longUnStoredField : this.longStoredField);
        } else if (pd.typeName.equals(TypeName.DATE)) {
            // Index as microseconds since UNIX epoch
            Date dt = (Date) v;
            luceneField = new LongField(fieldName, dt.getTime() * 1000,
                    fsv == Store.NO ? this.longUnStoredField : this.longStoredField);
        } else if (pd.typeName.equals(TypeName.DOUBLE)) {
            luceneField = new DoubleField(fieldName, (double) v,
                    fsv == Store.NO ? this.doubleUnStoredField : this.doubleStoredField);
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
        } else if (expandField && (pd.typeName.equals(TypeName.COLLECTION) ||
                pd.typeName.equals(TypeName.ARRAY))) {
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

    /**
     * Will attempt to re-open index writer to recover from a specific exception. The method
     * assumes the caller has acquired the writer semaphore
     */
    private void checkFailureAndRecover(Throwable e) {
        if (this.writer != null) {
            logSevere(e);
        }
        if (!(e instanceof AlreadyClosedException)) {
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

        Document hitDoc = s.doc(hits[0].doc);

        if (versionsToKeep == 0) {
            // we are asked to delete everything, no need to sort or query
            wr.deleteDocuments(linkQuery);
            this.indexUpdateTimeMicros = Utils.getNowMicrosUtc();
            delete.complete();
            return;
        }

        if (hits.length < versionsToKeep) {
            return;
        }

        BooleanQuery bq = new BooleanQuery();
        // grab the document at the tail of the results, and use it to form a new query
        // that will delete all documents from that document up to the version at the
        // retention
        // limit
        hitDoc = s.doc(hits[hits.length - 1].doc);
        long versionLowerBound = Long.parseLong(hitDoc.get(ServiceDocument.FIELD_NAME_VERSION));
        hitDoc = s.doc(hits[(int) versionsToKeep - 1].doc);
        long versionUpperBound = Long.parseLong(hitDoc.get(ServiceDocument.FIELD_NAME_VERSION));

        NumericRangeQuery<Long> versionQuery = NumericRangeQuery.newLongRange(
                ServiceDocument.FIELD_NAME_VERSION, versionLowerBound, versionUpperBound,
                true,
                true);

        bq.add(versionQuery, Occur.MUST);
        bq.add(linkQuery, Occur.MUST);
        results = s.search(bq, Integer.MAX_VALUE);
        long now = Utils.getNowMicrosUtc();
        logInfo("trimming index for %s from %d to %d, query returned %d", link, hits.length,
                versionsToKeep, results.totalHits);
        wr.deleteDocuments(bq);
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

        String link = sd.documentSelfLink;
        long start = Utils.getNowMicrosUtc();
        wr.addDocument(doc);
        updateLinkAccessTime(start, link);
        long end = Utils.getNowMicrosUtc();
        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            ServiceStat s = getHistogramStat(STAT_NAME_INDEXING_DURATION_MICROS);
            setStat(s, end - start);
        }

        op.setBody(null).complete();
        checkDocumentRetentionLimit(sd, desc);
        applyActiveQueries(sd, desc);
    }

    private void updateLinkAccessTime(long start, String link) {
        this.linkAccessTimes.put(link, start);
        this.indexUpdateTimeMicros = start;
    }

    private IndexSearcher updateSearcher(String selfLink, int resultLimit, IndexWriter w)
            throws IOException {

        // We want avoid creating a new IndexSearcher, per query. So we create one in one of
        // following conditions:
        // 1) no searcher for this index exists
        // 2) the query is across many links or multiple version, not a specific one and index was
        // changed
        // 3) the query is for a specific self link AND the self link has seen an update after the
        // last query executed

        IndexSearcher s = null;
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

        // outside the lock create a new searcher. Another thread might race us and also create a
        // searcher, but that is OK, we will use the most recent one
        s = new IndexSearcher(DirectoryReader.open(w, true));

        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            ServiceStat st = getStat(STAT_NAME_SEARCHER_UPDATE_COUNT);
            adjustStat(st, 1);
        }

        synchronized (this.searchSync) {
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

        int count = 0;
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

            applyDocumentExpirationPolicy(w);
            applyDocumentVersionRetentionPolicy(w);
            w.commit();

            applyMemoryLimit();

            File directory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory);
            String[] list = directory.list();
            count = list == null ? 0 : list.length;

            if (!forceMerge && count < INDEX_FILE_COUNT_THRESHOLD_FOR_REOPEN) {
                return;
            }
            reOpenWriterSynchronously();
        } catch (Throwable e) {
            logWarning("Attempting recovery due to error: %s", e.getMessage());
            reOpenWriterSynchronously();
            throw e;
        }
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
            String[] list = directory.list();
            int count = list == null ? 0 : list.length;
            try {
                if (w != null) {
                    logInfo("Before: File count: %d, document count: %d", count, w.maxDoc());
                    w.close();
                }
            } catch (Throwable e) {
            }

            w = createWriter(directory, false);
            list = directory.list();
            count = list == null ? 0 : list.length;
            logInfo("After: File count: %d, document count: %d", count, w.maxDoc());
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

    private void applyDocumentVersionRetentionPolicy(IndexWriter w)
            throws Throwable {
        IndexWriter wr = this.writer;
        if (wr == null) {
            return;
        }

        IndexSearcher s = this.searcher;
        if (s == null) {
            return;
        }

        Operation dummyDelete = Operation.createDelete(null);
        int count = 0;
        Iterator<Entry<String, Long>> it = this.linkDocumentRetentionEstimates.entrySet()
                .iterator();
        while (it.hasNext()) {
            Entry<String, Long> e = it.next();
            Query linkQuery = new TermQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK,
                    e.getKey()));
            int documentCount = s.count(linkQuery);
            int pastRetentionLimitVersions = (int) (documentCount - e.getValue());
            if (pastRetentionLimitVersions <= 0) {
                continue;
            }

            it.remove();
            // trim durable index for this link
            deleteDocumentsFromIndex(dummyDelete, e.getKey(), e.getValue());
            count++;
        }

        if (!this.linkDocumentRetentionEstimates.isEmpty()) {
            logInfo("Applied retention policy to %d links", count);
        }
    }

    private void applyMemoryLimit() throws InterruptedException, IOException {
        if (getHost().isStopping()) {
            return;
        }

        synchronized (this.searchSync) {
            if (this.linkAccessTimes.isEmpty()) {
                return;
            }
            this.linkAccessTimes.clear();
            // refresh the searcher, since we cleared the link access map
            this.searcher = null;
        }
        updateSearcher(null, Integer.MAX_VALUE, this.writer);
    }

    private void applyDocumentExpirationPolicy(IndexWriter w) throws Throwable {
        IndexSearcher s = updateSearcher(null, Integer.MAX_VALUE, w);
        if (s == null) {
            return;
        }

        long expirationUpperBound = Utils.getNowMicrosUtc();

        NumericRangeQuery<Long> versionQuery = NumericRangeQuery.newLongRange(
                ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS, 1L, expirationUpperBound,
                true,
                true);

        TopDocs results = s.search(versionQuery, Integer.MAX_VALUE);
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
}
