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

import static java.util.stream.Collectors.toSet;

import static com.vmware.xenon.services.common.LuceneDocumentIndexService.QUERY_THREAD_COUNT;
import static com.vmware.xenon.services.common.LuceneDocumentIndexService.UPDATE_THREAD_COUNT;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.Query;

import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.BackupRequest;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.BackupResponse;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.BackupType;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.RestoreRequest;

/**
 * Handle backup and restore of lucene index files.
 *
 * @see LuceneDocumentIndexService
 * @see ServiceHostManagementService
 */
public class LuceneDocumentIndexBackupService extends StatelessService {

    public static final String SELF_LINK = ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP;

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


        ServiceDocument sd = (ServiceDocument) patch.getBodyRaw();
        if (sd.documentKind != null) {
            if (sd.documentKind.equals(BackupRequest.KIND)) {
                try {
                    handleBackup(patch);
                } catch (IOException e) {
                    patch.fail(e);
                }
            } else if (sd.documentKind.equals(RestoreRequest.KIND)) {
                RestoreRequest restoreRequest = (RestoreRequest) patch.getBodyRaw();
                handleRestore(patch, restoreRequest);
            }
            return;
        }
        Operation.failActionNotSupported(patch);
    }

    /**
     * Generate a lucene index snapshot to the destination file(zip) or directory(incremental).
     */
    private void handleBackup(Operation op) throws IOException {
        IndexWriter writer = this.indexService.writer;
        if (writer == null) {
            op.fail(new CancellationException());
            return;
        }

        String indexDirectoryName = this.indexService.indexDirectory;

        BackupRequest backupRequest = op.getBody(BackupRequest.class);

        // special handling to keep existing behavior. generate backup zip file name.
        if (backupRequest.type == BackupType.ZIP && backupRequest.destination == null) {
            String outFileName = indexDirectoryName + "-" + Utils.getNowMicrosUtc();
            File zipFile = File.createTempFile(outFileName, ".zip", null);
            backupRequest.destination = zipFile.toURI();
        }


        // validate request
        Exception validationError = validateBackupRequest(backupRequest);
        if (validationError != null) {
            op.fail(validationError);
            return;
        }

        URI destinationUri = backupRequest.destination;
        Path destinationPath = Paths.get(destinationUri);

        URI storageSandbox = getHost().getStorageSandbox();
        Path indexDirectoryPath = Paths.get(storageSandbox).resolve(indexDirectoryName);


        SnapshotDeletionPolicy snapshotter = null;
        IndexCommit commit = null;
        try {
            // Create a snapshot so the index files won't be deleted.
            snapshotter = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
            commit = snapshotter.snapshot();

            if (backupRequest.type == BackupType.ZIP) {
                // Add the files in the commit to a zip file.
                List<URI> fileList = FileUtils.filesToUris(indexDirectoryPath.toString(), commit.getFileNames());
                FileUtils.zipFiles(fileList, destinationPath.toFile());
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

                logInfo("Incremental backup performed. dir=%s, added=%d, deleted=%d",
                        destinationPath, toAdd.size(), toDelete.size());
            }

            BackupResponse response = new BackupResponse();
            response.backupFile = destinationUri;

            op.setBody(response).complete();
        } catch (Exception e) {
            logSevere(e);
            op.fail(e);
        } finally {
            if (snapshotter != null && commit != null) {
                snapshotter.release(commit);
            }
            writer.deleteUnusedFiles();
        }
    }

    private Exception validateBackupRequest(LuceneDocumentIndexService.BackupRequest backupRequest) {
        URI destinationUri = backupRequest.destination;
        if (destinationUri == null) {
            return new IllegalStateException("local file or directory must be specified in destination.");
        }
        if (!UriUtils.FILE_SCHEME.equals(destinationUri.getScheme())) {
            String message = String.format("destination %s must be a local file or directory(file scheme).", destinationUri);
            return new IllegalStateException(message);
        }
        Path destinationPath = Paths.get(destinationUri);
        if (backupRequest.type == BackupType.ZIP) {
            if (Files.isDirectory(destinationPath)) {
                String message = String.format("destination %s must be a local file for zip backup.", destinationPath);
                return new IllegalStateException(message);
            }
        } else {
            if (Files.isRegularFile(destinationPath)) {
                String message = String.format("destination %s must be a local directory for incremental backup.", destinationPath);
                return new IllegalStateException(message);
            }
        }

        return null;
    }


    private void handleRestore(Operation op, RestoreRequest req) {
        IndexWriter w = this.indexService.writer;
        if (w == null) {
            op.fail(new CancellationException());
            return;
        }


        String indexDirectoryName = this.indexService.indexDirectory;
        URI storageSandbox = getHost().getStorageSandbox();
        Path indexDirectoryPath = Paths.get(storageSandbox).resolve(indexDirectoryName);


        // We already have a slot in the semaphore.  Acquire the rest.
        final int semaphoreCount = QUERY_THREAD_COUNT + UPDATE_THREAD_COUNT - 1;
        try {

            this.indexService.writerSync.acquire(semaphoreCount);
            this.indexService.close(w);

            File indexDirectoryFile = indexDirectoryPath.toFile();

            // Copy whatever was there out just in case.
            if (Files.isDirectory(indexDirectoryPath)) {
                // We know the file list won't be null because directory.exists() returned true,
                // but Findbugs doesn't know that, so we make it happy.
                File[] files = indexDirectoryPath.toFile().listFiles();
                if (files != null && files.length > 0) {
                    logInfo("archiving existing index %s", indexDirectoryFile);
                    this.indexService.archiveCorruptIndexFiles(indexDirectoryFile);
                }
            }

            Path backupPath = Paths.get(req.backupFile);
            if (Files.isDirectory(backupPath)) {
                // perform restore from directory
                logInfo("restoring index %s from directory %s", indexDirectoryFile, backupPath);
                FileUtils.copyFiles(backupPath.toFile(), indexDirectoryFile);
            } else {
                // perform restore from zip file (original behavior)
                logInfo("restoring index %s from %s md5sum(%s)", indexDirectoryFile, req.backupFile,
                        FileUtils.md5sum(backupPath.toFile()));
                FileUtils.extractZipArchive(backupPath.toFile(), indexDirectoryFile.toPath());
            }

            IndexWriter writer = this.indexService.createWriter(indexDirectoryFile, true);


            // perform time snapshot recovery which means deleting all docs updated after given time
            if (req.timeSnapshotBoundaryMicros != null) {
                Query luceneQuery = LongPoint.newRangeQuery(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                        req.timeSnapshotBoundaryMicros + 1, Long.MAX_VALUE);
                writer.deleteDocuments(luceneQuery);
            }

            op.complete();
            logInfo("restore complete");
        } catch (Throwable e) {
            logSevere(e);
            op.fail(e);
        } finally {
            this.indexService.writerSync.release(semaphoreCount);
        }
    }
}
