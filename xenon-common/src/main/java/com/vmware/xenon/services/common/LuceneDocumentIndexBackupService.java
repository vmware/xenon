/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import static com.vmware.xenon.services.common.LuceneDocumentIndexService.QUERY_THREAD_COUNT;
import static com.vmware.xenon.services.common.LuceneDocumentIndexService.UPDATE_THREAD_COUNT;
import static com.vmware.xenon.services.common.ServiceHostManagementService.BackupType.DIRECTORY;
import static com.vmware.xenon.services.common.ServiceHostManagementService.BackupType.STREAM;
import static com.vmware.xenon.services.common.ServiceHostManagementService.BackupType.ZIP;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;

import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.CommitInfo;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.MaintenanceRequest;
import com.vmware.xenon.services.common.ServiceHostManagementService.BackupRequest;
import com.vmware.xenon.services.common.ServiceHostManagementService.BackupType;
import com.vmware.xenon.services.common.ServiceHostManagementService.RestoreRequest;

/**
 * Handle backup and restore of lucene index files.
 *
 * This service works with both default index service {@link LuceneDocumentIndexService}, and in-memory index service
 * {@link InMemoryLuceneDocumentIndexService}.
 *
 * Since in-memory index is opt-in service, manual registration of this service is required to perform in-memory index backup/restore.
 * To register, initialize this service with in-memory index service, and start the service with
 * {@link ServiceUriPaths#CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP}".
 *
 * @see ServiceHostManagementService
 * @see LuceneDocumentIndexService
 */
public class LuceneDocumentIndexBackupService extends StatelessService {

    public static final String SELF_LINK = ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP;

    /**
     * Helper method to return a consumer that triggers incremental backup for auto-backup.
     * The returned consumer is expected to be used for a subscription callback on {@link LuceneDocumentIndexService}
     * commit event.
     *
     * @return a consumer to trigger incremental backup upon CommitInfo event.
     */
    public static Consumer<Operation> createAutoBackupConsumer(ServiceHost host, Service managementService) {
        URI autoBackupDirPath = host.getState().autoBackupDirectoryReference;

        BackupRequest backupRequest = new BackupRequest();
        backupRequest.kind = BackupRequest.KIND;
        backupRequest.destination = autoBackupDirPath;
        backupRequest.backupType = BackupType.DIRECTORY;

        AtomicBoolean autoBackupInProgress = new AtomicBoolean(false);
        AtomicReference<CommitInfo> receivedWhileBackupInProgress = new AtomicReference<>();

        Logger logger = Logger.getLogger(LuceneDocumentIndexBackupService.class.getName());

        return (notifyOp) -> {
            // immediately complete subscription
            notifyOp.complete();

            // check auto-backup is currently enabled or not
            if (!host.isAutoBackupEnabled()) {
                return;
            }

            // since it is in-memory call, body is a raw object. check notification event type.
            if (!(notifyOp.getBodyRaw() instanceof CommitInfo)) {
                return;
            }

            if (autoBackupInProgress.get()) {
                incrementAutoBackupStat(managementService, ServiceHostManagementService.STAT_NAME_AUTO_BACKUP_SKIPPED_COUNT);

                // keep the last commit info to reuse at re-trigger request. The commit info is used just for informational
                // purpose for what it provides(sequenceNumber).
                //   e.g.: 100 commits arrived while performing backup, it will trigger one auto-backup after finishing
                //         previous one and the newly triggered backup will cover all 100 commits.
                CommitInfo commitInfo = notifyOp.getBody(CommitInfo.class);
                receivedWhileBackupInProgress.set(commitInfo);
                logger.info(() -> String.format("Auto backup is in progress. Next backup is scheduled. sequenceNumber=%s",
                        commitInfo.sequenceNumber));
                return;
            }

            Operation backupOp = Operation.createPatch(host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP)
                    .setBody(backupRequest)
                    .setReferer(host.getUri())
                    .setCompletion((op, ex) -> {
                        if (ex != null) {
                            logger.severe(String.format("Auto backup failed. %s", Utils.toString(ex)));
                            return;
                        }

                        incrementAutoBackupStat(managementService, ServiceHostManagementService.STAT_NAME_AUTO_BACKUP_PERFORMED_COUNT);
                        autoBackupInProgress.set(false);

                        // When it had received notification while processing auto-backup, re-trigger the
                        // notification event in order to make sure the last commit gets backed up
                        CommitInfo commitInfo = receivedWhileBackupInProgress.getAndSet(null);
                        if (commitInfo != null) {
                            logger.info(() -> String.format("Re-trigger auto backup for sequenceNumber=%s", commitInfo.sequenceNumber));
                            Operation notification = Operation.createPatch(notifyOp.getUri())
                                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NOTIFICATION)
                                    .setBody(commitInfo)
                                    .setReferer(notifyOp.getReferer());
                            host.sendRequest(notification);
                        }
                    });

            autoBackupInProgress.set(true);
            host.sendRequest(backupOp);
        };
    }

    private static void incrementAutoBackupStat(Service service, String statKeyPrefix) {
        String hourlyStatKey = statKeyPrefix + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;
        String dailyStatKey = statKeyPrefix + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
        service.adjustStat(hourlyStatKey, 1);
        service.adjustStat(dailyStatKey, 1);
    }

    private LuceneDocumentIndexService indexService;

    public LuceneDocumentIndexBackupService(LuceneDocumentIndexService indexService) {
        this.indexService = indexService;
    }

    @Override
    public void handlePatch(Operation patch) {

        if (patch.isRemote()) {
            // PATCH is reserved for in-process from LuceneDocumentIndexService
            Operation.failActionNotSupported(patch);
            return;
        }

        Object request = patch.getBodyRaw();
        if (request instanceof BackupRequest) {
            handleBackup(patch);
            return;
        } else if (request instanceof RestoreRequest) {
            try {
                handleRestore(patch);
            } catch (IOException e) {
                patch.fail(e);
            }
            return;
        }

        Operation.failActionNotSupported(patch);
    }


    private void handleBackup(Operation op) {

        BackupRequest backupRequest = op.getBody(BackupRequest.class);

        // validate request
        Exception validationError = validateBackupRequest(backupRequest);
        if (validationError != null) {
            op.fail(validationError);
            return;
        }

        if (this.indexService.writer == null) {
            op.fail(new CancellationException("writer is null"));
            return;
        }

        // call maintenance request, then perform backup
        MaintenanceRequest maintenanceRequest = new MaintenanceRequest();
        Operation post = Operation.createPost(this, this.indexService.getSelfLink()).setBody(maintenanceRequest);
        sendWithDeferredResult(post)
                .whenComplete((o, x) -> {
                    if (x != null) {
                        op.fail(x);
                        return;
                    }

                    // perform backup
                    try {
                        handleBackupInternal(op, backupRequest);
                    } catch (Exception e) {
                        logSevere("Failed to handle backup request. %s", Utils.toString(e));
                        op.fail(e);
                        return;
                    }
                });
    }

    private void handleBackupInternal(Operation originalOp, BackupRequest backupRequest) throws IOException {
        String indexDirectoryName = this.indexService.indexDirectory;
        boolean isStreamBackup = STREAM == backupRequest.backupType;

        Path localDestinationPath;
        if (isStreamBackup) {
            // create a temp file to store the backup and stream from it
            String outFileName = indexDirectoryName + "-" + Utils.getNowMicrosUtc();
            localDestinationPath = Files.createTempFile(outFileName, ".zip");
        } else {
            localDestinationPath = Paths.get(backupRequest.destination);
        }

        // take snapshot
        boolean isZipBackup = EnumSet.of(ZIP, STREAM).contains(backupRequest.backupType);
        try {
            takeSnapshot(localDestinationPath, isZipBackup);
        } catch (IOException e) {
            logSevere(e);
            Files.deleteIfExists(localDestinationPath);
            throw e;
        }


        if (isStreamBackup) {
            // upload the result.
            Operation uploadOp = Operation.createPost(backupRequest.destination)
                    .transferRequestHeadersFrom(originalOp)
                    .transferRefererFrom(originalOp)
                    .setCompletion((oop, oox) -> {
                        // delete temp backup file
                        try {
                            Files.deleteIfExists(localDestinationPath);
                        } catch (IOException e) {
                            logWarning("Failed to delete temporary backup file %s: %s", localDestinationPath, Utils.toString(e));
                        }

                        if (oox != null) {
                            originalOp.fail(oox);
                            return;
                        }

                        // finish original request
                        originalOp.complete();
                    });
            FileUtils.putFile(getHost().getClient(), uploadOp, localDestinationPath.toFile());
        } else {
            originalOp.complete();
        }
    }


    private void takeSnapshot(Path destinationPath, boolean isZipBackup) throws IOException {

        IndexWriter writer = this.indexService.writer;
        boolean isInMemoryIndex = isInMemoryIndex();

        SnapshotDeletionPolicy snapshotter = null;
        IndexCommit commit = null;
        long backupStartTime = System.currentTimeMillis();
        try {
            // Create a snapshot so the index files won't be deleted.
            writer.commit();
            snapshotter = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
            commit = snapshotter.snapshot();

            if (isZipBackup) {
                Path tempDir = null;
                try {
                    List<URI> fileList = new ArrayList<>();
                    if (isInMemoryIndex) {
                        tempDir = Files.createTempDirectory("lucene-in-memory-backup");
                        copyInMemoryLuceneIndexToDirectory(commit, tempDir);
                        List<URI> files = Files.list(tempDir).map(Path::toUri).collect(toList());
                        fileList.addAll(files);
                    } else {

                        Path indexDirectoryPath = getIndexDirectoryPath();
                        List<URI> files = commit.getFileNames().stream()
                                .map(indexDirectoryPath::resolve)
                                .map(Path::toUri)
                                .collect(toList());
                        fileList.addAll(files);
                    }

                    // Add files in the commit to a zip file.
                    FileUtils.zipFiles(fileList, destinationPath.toFile());
                } finally {
                    if (tempDir != null) {
                        FileUtils.deleteFiles(tempDir.toFile());
                    }
                }
            } else {
                // incremental backup

                // create destination dir if not exist
                if (!Files.exists(destinationPath)) {
                    Files.createDirectory(destinationPath);
                }

                Set<String> sourceFileNames = new HashSet<>(commit.getFileNames());

                Set<String> destFileNames = Files.list(destinationPath)
                        .filter(Files::isRegularFile)
                        .map(path -> path.getFileName().toString())
                        .collect(toSet());

                Path tempDir = null;
                try {
                    Path indexDirectoryPath;
                    if (isInMemoryIndex) {
                        // copy files into temp directory and point index directory path to temp dir
                        tempDir = Files.createTempDirectory("lucene-in-memory-backup");
                        copyInMemoryLuceneIndexToDirectory(commit, tempDir);
                        indexDirectoryPath = tempDir;
                    } else {
                        indexDirectoryPath = getIndexDirectoryPath();
                    }

                    // add files exist in source but not in dest
                    Set<String> toAdd = new HashSet<>(sourceFileNames);
                    toAdd.removeAll(destFileNames);
                    for (String filename : toAdd) {
                        Path source = indexDirectoryPath.resolve(filename);
                        Path target = destinationPath.resolve(filename);
                        Files.copy(source, target);
                    }

                    // delete files exist in dest but not in source
                    Set<String> toDelete = new HashSet<>(destFileNames);
                    toDelete.removeAll(sourceFileNames);
                    for (String filename : toDelete) {
                        Path path = destinationPath.resolve(filename);
                        Files.delete(path);
                    }

                    long backupEndTime = System.currentTimeMillis();
                    logInfo("Incremental backup performed. dir=%s, added=%d, deleted=%d, took=%dms",
                            destinationPath, toAdd.size(), toDelete.size(), backupEndTime - backupStartTime);
                } finally {
                    if (tempDir != null) {
                        FileUtils.deleteFiles(tempDir.toFile());
                    }
                }
            }
        } finally {
            if (snapshotter != null && commit != null) {
                snapshotter.release(commit);
            }
            writer.deleteUnusedFiles();
        }
    }

    private Path getIndexDirectoryPath() {
        String indexDirectoryName = this.indexService.indexDirectory;
        URI storageSandbox = getHost().getStorageSandbox();
        return Paths.get(storageSandbox).resolve(indexDirectoryName);
    }

    private void copyInMemoryLuceneIndexToDirectory(IndexCommit commit, Path directoryPath) throws IOException {
        Directory from = commit.getDirectory();
        try (Directory to = new NIOFSDirectory(directoryPath)) {
            for (String filename : commit.getFileNames()) {
                to.copyFrom(from, filename, filename, IOContext.DEFAULT);
            }
        }
    }

    private Exception validateBackupRequest(BackupRequest backupRequest) {
        URI destinationUri = backupRequest.destination;
        if (destinationUri == null) {
            return new IllegalStateException("backup destination must be specified.");
        }
        if (backupRequest.backupType == ZIP) {
            Path destinationPath = Paths.get(destinationUri);
            if (Files.isDirectory(destinationPath)) {
                String message = String.format("destination %s must be a local file for zip backup.", destinationPath);
                return new IllegalStateException(message);
            }
        } else if (backupRequest.backupType == DIRECTORY) {
            Path destinationPath = Paths.get(destinationUri);
            if (Files.isRegularFile(destinationPath)) {
                String message = String.format("destination %s must be a local directory for incremental backup.", destinationPath);
                return new IllegalStateException(message);
            }
        }

        return null;
    }


    private void handleRestore(Operation op) throws IOException {
        ServiceHostManagementService.RestoreRequest restoreRequest = op.getBody(RestoreRequest.class);

        // validation
        if (restoreRequest.destination == null) {
            op.fail(new IllegalStateException("destination is required."));
            return;
        }

        String scheme = restoreRequest.destination.getScheme();
        boolean isFromRemote = UriUtils.HTTP_SCHEME.equals(scheme) || UriUtils.HTTPS_SCHEME.equals(scheme);


        Path backupFilePath;
        if (isFromRemote) {
            // create a temporary file and download the remote content
            backupFilePath = Files.createTempFile("restore-" + Utils.getNowMicrosUtc(), ".zip");
        } else {
            backupFilePath = Paths.get(restoreRequest.destination);
        }

        Operation downloadFileOp = Operation.createGet(restoreRequest.destination)
                .transferRefererFrom(op)
                .setCompletion((o, x) -> {
                    if (x != null) {
                        op.fail(x);
                    }

                    // perform restore from local file/dir
                    restoreFromLocal(op, backupFilePath, restoreRequest.timeSnapshotBoundaryMicros);

                    if (isFromRemote) {
                        try {
                            Files.deleteIfExists(backupFilePath);
                        } catch (IOException e) {
                            logWarning("Failed to delete temporary backup file %s: %s",
                                    backupFilePath, Utils.toString(e));
                        }
                    }
                });


        // perform download
        if (isFromRemote) {
            FileUtils.getFile(getHost().getClient(), downloadFileOp, backupFilePath.toFile());
        } else {
            // skip download
            downloadFileOp.complete();
        }
    }


    private void restoreFromLocal(Operation op, Path restoreFrom, Long timeSnapshotBoundaryMicros) {

        IndexWriter w = this.indexService.writer;
        if (w == null) {
            op.fail(new CancellationException("writer is null"));
            return;
        }

        boolean isInMemoryIndex = isInMemoryIndex();
        boolean restoreFromZipFile = Files.isRegularFile(restoreFrom);

        // resolve index directory path for filesystem based index
        Path restoreTo = null;
        if (!isInMemoryIndex) {
            restoreTo = getIndexDirectoryPath();
        }

        Set<Path> pathToDeleteAtFinally = new HashSet<>();
        // We already have a slot in the semaphore.  Acquire the rest.
        final int semaphoreCount = QUERY_THREAD_COUNT + UPDATE_THREAD_COUNT - 1;
        try {

            this.indexService.writerSync.acquire(semaphoreCount);
            this.indexService.close(w);

           // extract zip file to temp directory
            if (restoreFromZipFile && isInMemoryIndex) {
                Path tempDir = Files.createTempDirectory("restore-" + Utils.getSystemNowMicrosUtc());
                pathToDeleteAtFinally.add(tempDir);

                logInfo("extracting zip file to temporal directory %s", tempDir);
                FileUtils.extractZipArchive(restoreFrom.toFile(), tempDir);

                // now behave as if it was restoring from directory
                restoreFromZipFile = false;
                restoreFrom = tempDir;
            }

            IndexWriter newWriter;
            if (restoreFromZipFile) {
                // index service is always on filesystem since zip with in-memory is already checked above
                // perform restore from zip file (original behavior)
                logInfo("restoring index %s from %s md5sum(%s)", restoreTo, restoreFrom,
                        FileUtils.md5sum(restoreFrom.toFile()));
                FileUtils.extractZipArchive(restoreFrom.toFile(), restoreTo);
                newWriter = this.indexService.createWriter(restoreTo.toFile(), true);
            } else {
                // perform restore from directory

                if (isInMemoryIndex) {
                    logInfo("restoring in-memory index from directory %s", restoreFrom);

                    // copy to lucene ram directory
                    Directory from = MMapDirectory.open(restoreFrom);
                    Directory to = new RAMDirectory();
                    for (String filename : from.listAll()) {
                        to.copyFrom(from, filename, filename, IOContext.DEFAULT);
                    }

                    newWriter = this.indexService.createWriterWithLuceneDirectory(to, true);

                } else {
                    logInfo("restoring index %s from directory %s", restoreTo, restoreFrom);

                    // Copy whatever was there out just in case.
                    if (Files.list(restoreTo).count() > 0) {
                        logInfo("archiving existing index %s", restoreTo);
                        this.indexService.archiveCorruptIndexFiles(restoreTo.toFile());
                    }

                    FileUtils.copyFiles(restoreFrom.toFile(), restoreTo.toFile());
                    newWriter = this.indexService.createWriter(restoreTo.toFile(), true);
                }
            }

            // perform time snapshot recovery
            if (timeSnapshotBoundaryMicros != null) {
                performTimeSnapshotRecovery(timeSnapshotBoundaryMicros, newWriter);
            }

            this.indexService.setWriterUpdateTimeMicros(Utils.getNowMicrosUtc());

            op.complete();
            logInfo("restore complete");
        } catch (Exception e) {
            logSevere(e);
            op.fail(e);
        } finally {
            this.indexService.writerSync.release(semaphoreCount);

            for (Path path : pathToDeleteAtFinally) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                }
            }
        }
    }

    private void performTimeSnapshotRecovery(Long timeSnapshotBoundaryMicros, IndexWriter newWriter)
            throws IOException {

        // For documents with metadata indexing enabled, the version which was current at
        // the restore time may have subsequently been marked as not current. Update the
        // current field for any such documents.

        IndexSearcher searcher = new IndexSearcher(
                DirectoryReader.open(newWriter, true, true));

        Query updateTimeQuery = LongPoint.newRangeQuery(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                timeSnapshotBoundaryMicros + 1, Long.MAX_VALUE);

        Sort selfLinkSort = new Sort(new SortField(
                LuceneIndexDocumentHelper.createSortFieldPropertyName(
                        ServiceDocument.FIELD_NAME_SELF_LINK),
                SortField.Type.STRING));

        final int pageSize = 10000;

        Set<String> prevPageLinks = new HashSet<>();
        ScoreDoc after = null;
        while (true) {
            TopDocs results = searcher.searchAfter(after, updateTimeQuery, pageSize, selfLinkSort,
                    false, false);
            if (results == null || results.scoreDocs == null || results.scoreDocs.length == 0) {
                break;
            }

            Set<String> pageLinks = new HashSet<>();
            DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
            for (ScoreDoc sd : results.scoreDocs) {
                visitor.reset(ServiceDocument.FIELD_NAME_SELF_LINK);
                searcher.doc(sd.doc, visitor);
                if (prevPageLinks.contains(visitor.documentSelfLink)) {
                    pageLinks.add(visitor.documentSelfLink);
                    continue;
                }

                if (!pageLinks.add(visitor.documentSelfLink)) {
                    continue;
                }

                updateCurrentAttributeForSelfLink(searcher, timeSnapshotBoundaryMicros,
                        visitor.documentSelfLink, newWriter);
            }

            if (results.scoreDocs.length < pageSize) {
                break;
            }

            after = results.scoreDocs[results.scoreDocs.length - 1];
            prevPageLinks = pageLinks;
        }

        // Now that metadata indexing attributes have been updated appropriately, delete any
        // documents which were created after the restore point.
        Query luceneQuery = LongPoint.newRangeQuery(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                timeSnapshotBoundaryMicros + 1, Long.MAX_VALUE);
        newWriter.deleteDocuments(luceneQuery);
    }

    private void updateCurrentAttributeForSelfLink(IndexSearcher searcher,
            long timeSnapshotBoundaryMicros, String selfLink, IndexWriter newWriter)
            throws IOException {

        Query selfLinkClause = new TermQuery(new Term(ServiceDocument.FIELD_NAME_SELF_LINK,
                selfLink));
        Query updateTimeClause = LongPoint.newRangeQuery(
                ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS, 0, timeSnapshotBoundaryMicros);
        Query booleanQuery = new BooleanQuery.Builder()
                .add(selfLinkClause, Occur.MUST)
                .add(updateTimeClause, Occur.MUST)
                .build();

        Sort versionSort = new Sort(new SortedNumericSortField(ServiceDocument.FIELD_NAME_VERSION,
                SortField.Type.LONG, true));

        TopDocs results = searcher.search(booleanQuery, 1, versionSort, false, false);
        if (results == null || results.scoreDocs == null || results.scoreDocs.length == 0) {
            return;
        }

        DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
        visitor.reset(LuceneIndexDocumentHelper.FIELD_NAME_INDEXING_ID);
        searcher.doc(results.scoreDocs[0].doc, visitor);
        if (visitor.documentIndexingId == null) {
            return;
        }

        Term indexingIdTerm = new Term(LuceneIndexDocumentHelper.FIELD_NAME_INDEXING_ID,
                visitor.documentIndexingId);
        newWriter.updateNumericDocValue(indexingIdTerm,
                LuceneIndexDocumentHelper.FIELD_NAME_INDEXING_METADATA_VALUE_CURRENT, 1L);
    }

    private boolean isInMemoryIndex() {
        return this.indexService.indexDirectory == null;
    }
}
