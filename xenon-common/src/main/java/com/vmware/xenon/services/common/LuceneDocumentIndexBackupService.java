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
import static com.vmware.xenon.services.common.ServiceHostManagementService.BackupType.DIRECTORY;
import static com.vmware.xenon.services.common.ServiceHostManagementService.BackupType.STREAM;
import static com.vmware.xenon.services.common.ServiceHostManagementService.BackupType.ZIP;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
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
import com.vmware.xenon.services.common.LuceneDocumentIndexService.MaintenanceRequest;
import com.vmware.xenon.services.common.ServiceHostManagementService.BackupRequest;
import com.vmware.xenon.services.common.ServiceHostManagementService.RestoreRequest;

/**
 * Handle backup and restore of lucene index files.
 *
 * @see ServiceHostManagementService
 * @see LuceneDocumentIndexService
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
            op.fail(new CancellationException());
            return;
        }

        // call maintenance request, then perform backup
        MaintenanceRequest maintenanceRequest = new MaintenanceRequest();
        Operation post = Operation.createPost(this, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(maintenanceRequest);
        sendWithDeferredResult(post)
                .whenComplete((o, x) -> {
                    if (x != null) {
                        op.fail(x);
                        return;
                    }

                    // perform backup
                    try {
                        handleBackupInternal(op, backupRequest);
                    } catch (IOException e) {
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
        URI storageSandbox = getHost().getStorageSandbox();
        Path indexDirectoryPath = Paths.get(storageSandbox).resolve(indexDirectoryName);
        try {
            takeSnapshot(localDestinationPath, isZipBackup, indexDirectoryPath);
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
                        if (oox != null) {
                            originalOp.fail(oox);
                            return;
                        }
                        // delete temp backup file
                        try {
                            Files.deleteIfExists(localDestinationPath);
                        } catch (IOException e) {
                            originalOp.fail(new IllegalStateException("failed to delete backup file "
                                    + localDestinationPath));
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


    private void takeSnapshot(Path destinationPath, boolean isZipBackup, Path indexDirectoryPath) throws IOException {
        IndexWriter writer = this.indexService.writer;

        SnapshotDeletionPolicy snapshotter = null;
        IndexCommit commit = null;
        try {
            // Create a snapshot so the index files won't be deleted.
            snapshotter = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
            commit = snapshotter.snapshot();

            if (isZipBackup) {
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
        } finally {
            if (snapshotter != null && commit != null) {
                snapshotter.release(commit);
            }
            writer.deleteUnusedFiles();
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
                });


        // perform download
        if (isFromRemote) {
            FileUtils.getFile(getHost().getClient(), downloadFileOp, backupFilePath.toFile());
        } else {
            // skip download
            downloadFileOp.complete();
        }
    }


    private void restoreFromLocal(Operation op, Path backupFilePath, Long timeSnapshotBoundaryMicros) {

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

            if (Files.isDirectory(backupFilePath)) {
                // perform restore from directory
                logInfo("restoring index %s from directory %s", indexDirectoryFile, backupFilePath);
                FileUtils.copyFiles(backupFilePath.toFile(), indexDirectoryFile);
            } else {
                // perform restore from zip file (original behavior)
                logInfo("restoring index %s from %s md5sum(%s)", indexDirectoryFile, backupFilePath,
                        FileUtils.md5sum(backupFilePath.toFile()));
                FileUtils.extractZipArchive(backupFilePath.toFile(), indexDirectoryFile.toPath());
            }

            // perform time snapshot recovery which means deleting all docs updated after given time
            IndexWriter writer = this.indexService.createWriter(indexDirectoryFile, true);
            if (timeSnapshotBoundaryMicros != null) {
                Query luceneQuery = LongPoint.newRangeQuery(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                        timeSnapshotBoundaryMicros + 1, Long.MAX_VALUE);
                writer.deleteDocuments(luceneQuery);
            }

            this.indexService.setWriterUpdateTimeMicros(Utils.getNowMicrosUtc());

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
