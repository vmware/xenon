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
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.MemoryLimitType;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

public class LuceneBlobIndexService extends StatelessService {
    public static enum BlobIndexOption {
        /**
         * Key is deleted after a successful query
         */
        SINGLE_USE_KEYS,

        /**
         * Index is created on start. If an older index exists, its deleted.
         */
        CREATE
    }

    public static Operation createPost(ServiceHost host, String key, Object blob) {
        return createPost(host, SELF_LINK, key, blob);
    }

    public static Operation createGet(ServiceHost host, String key) {
        return createGet(host, SELF_LINK, key);
    }

    protected static Operation createPost(ServiceHost host, String indexPath, String key,
            Object blob) {
        URI indexUri = UriUtils.buildUri(
                host,
                indexPath,
                LuceneBlobIndexService.URI_PARAM_NAME_KEY + "=" + key + "&" +
                        LuceneBlobIndexService.URI_PARAM_NAME_UPDATE_TIME + "="
                        + Utils.getNowMicrosUtc());
        return Operation.createPost(indexUri).setBodyNoCloning(blob);
    }

    protected static Operation createGet(ServiceHost host, String indexPath, String key) {
        URI indexUri = UriUtils.buildUri(host, indexPath,
                LuceneBlobIndexService.URI_PARAM_NAME_KEY + "=" + key);
        return Operation.createGet(indexUri);
    }

    public static final String SELF_LINK = ServiceUriPaths.CORE_BLOB_INDEX;

    public static final String FILE_PATH = "lucene-blob-index";

    public static final String URI_PARAM_NAME_KEY = "key";

    private static final String URI_PARAM_NAME_UPDATE_TIME = "updateTime";

    private static final String LUCENE_FIELD_NAME_BINARY_CONTENT = "binaryContent";

    private String indexDirectory;

    private IndexSearcher searcher = null;
    private IndexWriter writer = null;

    private long searcherUpdateTimeMicros;

    private long indexUpdateTimeMicros;

    private EnumSet<BlobIndexOption> indexOptions;

    private Sort timeSort;

    private final int maxBinaryContextSizeBytes = 1024 * 1024;

    private ExecutorService singleThreadedExecutor;

    public LuceneBlobIndexService() {
        this.indexDirectory = FILE_PATH;
        this.indexOptions = EnumSet.noneOf(BlobIndexOption.class);
    }

    public LuceneBlobIndexService(EnumSet<BlobIndexOption> options, String indexDirectory) {
        super(ServiceDocument.class);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
        this.indexDirectory = indexDirectory;
        this.indexOptions = options;
    }

    @Override
    public void handleStart(final Operation post) {
        // The service index searcher and maintenance semantics rely on a single thread
        // making all updates. If the executor ever is changed to use multiple threads, this
        // service should mirror the logic in LuceneDocumentIndexService
        this.singleThreadedExecutor = getHost().allocateExecutor(this, 1);

        super.setMaintenanceIntervalMicros(getHost().getMaintenanceIntervalMicros() * 5);
        File directory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory);
        this.timeSort = new Sort(new SortField(URI_PARAM_NAME_UPDATE_TIME,
                SortField.Type.LONG, true));
        try {
            this.writer = createWriter(directory);
        } catch (Throwable e) {
            logSevere("Failure creating index writer on directory %s: %s", directory,
                    Utils.toString(e));
            post.fail(e);
            return;
        }
        post.complete();
    }

    public IndexWriter createWriter(File directory) throws IOException {
        Directory dir = MMapDirectory.open(directory.toPath());
        Analyzer analyzer = new SimpleAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        if (this.indexOptions.contains(BlobIndexOption.CREATE)) {
            iwc.setOpenMode(OpenMode.CREATE);
        } else {
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        }
        Long totalMBs = getHost().getServiceMemoryLimitMB(getSelfLink(), MemoryLimitType.EXACT);
        if (totalMBs != null) {
            totalMBs = Math.max(1, totalMBs);
            iwc.setRAMBufferSizeMB(totalMBs);
        }
        IndexWriter w = new IndexWriter(dir, iwc);
        w.commit();
        return w;
    }

    @Override
    public void handleRequest(Operation op) {
        Action a = op.getAction();
        if (a == Action.PUT || a == Action.PATCH) {
            getHost().failRequestActionNotSupported(op);
            return;
        }

        this.singleThreadedExecutor.execute(() -> {
            try {
                switch (a) {
                case DELETE:
                    handleDelete(op);
                    break;
                case GET:
                    handleGet(op);
                    break;
                case POST:
                    handlePost(op);
                    break;
                case PUT:
                default:
                    break;
                }
            } catch (Throwable e) {
                op.fail(e);
            }
        });
    }

    @Override
    public void handleGet(Operation get) {
        try {
            Map<String, String> params = UriUtils.parseUriQueryParams(get.getUri());
            String key = params.get(URI_PARAM_NAME_KEY);

            if (key == null) {
                get.fail(new IllegalArgumentException("key query parameter is required"));
                return;
            }
            queryIndex(key, get);
        } catch (Throwable e) {
            logSevere(e);
            get.fail(e);
        }
    }

    private void queryIndex(String key, Operation op)
            throws Throwable {
        IndexWriter w = this.writer;
        if (w == null) {
            op.fail(new CancellationException());
            return;
        }

        IndexSearcher s = updateSearcher(w);
        Query linkQuery = new TermQuery(new Term(URI_PARAM_NAME_KEY, key));
        TopDocs hits = s.search(linkQuery, 1, this.timeSort, false, false);
        if (hits.totalHits == 0) {
            op.complete();
            return;
        }

        Document hitDoc = s.doc(hits.scoreDocs[0].doc);
        BytesRef content = hitDoc.getBinaryValue(LUCENE_FIELD_NAME_BINARY_CONTENT);
        long updateTime = Long.parseLong(hitDoc.get(URI_PARAM_NAME_UPDATE_TIME));
        Object hydratedInstance = Utils.fromBytes(content.bytes, content.offset, content.length);
        applyBlobRetentionPolicy(linkQuery, updateTime);
        op.setBodyNoCloning(hydratedInstance).complete();

    }

    public void handlePost(Operation post) {
        if (post.isRemote()) {
            post.fail(new IllegalStateException("Remote requests not allowed"));
            return;
        }

        Map<String, String> params = UriUtils.parseUriQueryParams(post.getUri());
        String key = params.get(URI_PARAM_NAME_KEY);
        if (key == null) {
            post.fail(new IllegalArgumentException("key query parameter is required"));
            return;
        }

        String updateTimeParam = params.get(URI_PARAM_NAME_UPDATE_TIME);

        if (updateTimeParam == null) {
            post.fail(new IllegalArgumentException("update time query parameter is required"));
            return;
        }

        long updateTime = Long.parseLong(updateTimeParam);
        IndexWriter wr = this.writer;
        if (wr == null) {
            post.fail(new CancellationException());
            return;
        }

        try {
            Object content = post.getBodyRaw();
            if (content == null) {
                post.fail(new IllegalArgumentException("service instance is required"));
                return;
            }
            byte[] binaryContent = new byte[this.maxBinaryContextSizeBytes];
            int count = Utils.toBytes(content, binaryContent, 0);
            Document doc = new Document();
            Field binaryContentField = new StoredField(LUCENE_FIELD_NAME_BINARY_CONTENT,
                    binaryContent, 0, count);
            doc.add(binaryContentField);
            Field keyField = new StringField(URI_PARAM_NAME_KEY,
                    key,
                    Field.Store.NO);
            doc.add(keyField);

            LuceneDocumentIndexService.addNumericField(
                    doc, URI_PARAM_NAME_UPDATE_TIME, updateTime, true, false);

            wr.addDocument(doc);
            this.indexUpdateTimeMicros = Utils.getNowMicrosUtc();
            post.setBody(null).complete();
        } catch (Throwable e) {
            logSevere(e);
            post.fail(e);
        }
    }

    @Override
    public void handleDelete(Operation delete) {
        if (delete.hasBody()) {
            getHost().failRequestActionNotSupported(delete);
            return;
        }

        setProcessingStage(ProcessingStage.STOPPED);
        IndexWriter w = this.writer;
        this.writer = null;
        close(w);
        this.singleThreadedExecutor.shutdownNow();
        delete.complete();
    }

    private void close(IndexWriter wr) {
        try {
            if (wr == null) {
                return;
            }
            wr.commit();
            wr.close();
        } catch (Throwable e) {

        }
    }


    private IndexSearcher updateSearcher(IndexWriter w)
            throws IOException {
        long now = Utils.getNowMicrosUtc();

        // we do not synchronize the searcher update since this service uses a single thread
        // to schedule all queries, updates and maintenance. If this changes, the code below
        // must become synchronized similar to LuceneDocumentIndexService.updateSearcher

        IndexSearcher s = this.searcher;
        if (s != null && this.searcherUpdateTimeMicros > this.indexUpdateTimeMicros) {
            return s;
        }

        s = new IndexSearcher(DirectoryReader.open(w, true, true));

        if (this.searcherUpdateTimeMicros < now) {
            closeSearcherSafe();
            this.searcher = s;
            this.searcherUpdateTimeMicros = now;
        }
        return this.searcher;
    }

    private void applyBlobRetentionPolicy(Query linkQuery, long updateTime)
            throws IOException {
        IndexWriter wr = this.writer;
        if (wr == null) {
            return;
        }

        if (!this.indexOptions.contains(BlobIndexOption.SINGLE_USE_KEYS)) {
            return;
        }

        // Query all blobs that satisfy the passed linkQuery and have
        // URI_PARAM_NAME_UPDATE_TIME field set to less than or
        // equal to updateTime
        Query timeQuery = LongPoint.newRangeQuery(
                URI_PARAM_NAME_UPDATE_TIME,
                Long.MIN_VALUE,
                updateTime);
        BooleanQuery.Builder builder = new BooleanQuery.Builder()
                .add(linkQuery, Occur.MUST)
                .add(timeQuery, Occur.MUST);
        wr.deleteDocuments(builder.build());
        this.indexUpdateTimeMicros = Utils.getNowMicrosUtc();
    }

    @Override
    public void handleMaintenance(Operation post) {
        this.singleThreadedExecutor.execute(() -> {
            handleMaintenanceSafe(post);
        });
    }

    private void handleMaintenanceSafe(Operation post) {
        try {
            IndexWriter w = this.writer;
            if (w == null) {
                post.complete();
                return;
            }
            w.commit();

            setStat(LuceneDocumentIndexService.STAT_NAME_INDEXED_DOCUMENT_COUNT, w.maxDoc());
            File directory = new File(new File(getHost().getStorageSandbox()), this.indexDirectory);
            String[] list = directory.list();
            int count = list == null ? 0 : list.length;
            if (count > LuceneDocumentIndexService.getIndexFileCountThresholdForWriterRefresh()) {
                logInfo("Index file count: %d, document count: %d", count, w.maxDoc());
                closeSearcherSafe();
                w.deleteUnusedFiles();
            }
            post.complete();
        } catch (Throwable e) {
            logSevere(e);
            post.fail(e);
        }
    }

    private void closeSearcherSafe() {
        if (this.searcher == null) {
            return;
        }
        try {
            this.searcher.getIndexReader().close();
            this.searcher = null;
        } catch (Throwable e) {
        }
    }
}
