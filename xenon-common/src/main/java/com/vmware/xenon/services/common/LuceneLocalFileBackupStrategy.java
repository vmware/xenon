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

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import static com.vmware.xenon.common.Utils.logWarning;
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
import java.util.logging.Logger;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;

import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.LuceneDocumentIndexBackupService.LuceneDocumentIndexBackupStrategy;
import com.vmware.xenon.services.common.ServiceHostManagementService.BackupRequest;

/**
 * Perform backup on local file system.
 */
public class LuceneLocalFileBackupStrategy implements LuceneDocumentIndexBackupStrategy {

    @Override
    public Exception validateRequest(BackupRequest backupRequest) {
        URI destinationUri = backupRequest.destination;
        if (destinationUri == null) {
            return new IllegalStateException("backup destination must be specified.");
        }
        if (backupRequest.backupType == ZIP) {
            Path destinationPath = Paths.get(destinationUri);
            if (Files.isDirectory(destinationPath)) {
                String message = format("destination %s must be a local file for zip backup.", destinationPath);
                return new IllegalStateException(message);
            }
        } else if (backupRequest.backupType == DIRECTORY) {
            Path destinationPath = Paths.get(destinationUri);
            if (Files.isRegularFile(destinationPath)) {
                String message = format("destination %s must be a local directory for incremental backup.", destinationPath);
                return new IllegalStateException(message);
            }
        }

        return null;
    }


    @Override
    public void perform(Operation originalOp, BackupRequest backupRequest, LuceneDocumentIndexService indexService,
            String indexDirectory, IndexWriter writer) throws IOException {

        ServiceHost host = indexService.getHost();
        boolean isStreamBackup = STREAM == backupRequest.backupType;

        // indexDirectory is null for in-memory index
        Path indexDirectoryPath = null;
        if (indexDirectory != null) {
            URI storageSandbox = host.getStorageSandbox();
            indexDirectoryPath = Paths.get(storageSandbox).resolve(indexDirectory);
        }


        Path localDestinationPath;
        if (isStreamBackup) {
            // create a temp file to store the backup and stream from it
            String outFileName = indexDirectory + "-" + Utils.getNowMicrosUtc();
            localDestinationPath = Files.createTempFile(outFileName, ".zip");
        } else {
            localDestinationPath = Paths.get(backupRequest.destination);
        }

        // take snapshot
        boolean isZipBackup = EnumSet.of(ZIP, STREAM).contains(backupRequest.backupType);
        try {
            takeSnapshot(localDestinationPath, isZipBackup, indexDirectoryPath, writer);
        } catch (IOException e) {
            Logger.getAnonymousLogger().severe(Utils.toString(e));
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
            FileUtils.putFile(host.getClient(), uploadOp, localDestinationPath.toFile());
        } else {
            originalOp.complete();
        }
    }

    private void takeSnapshot(Path destinationPath, boolean isZipBackup, Path indexDirectoryPath, IndexWriter writer) throws IOException {

        boolean isInMemoryIndex = indexDirectoryPath == null;

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
                    if (isInMemoryIndex) {
                        // copy files into temp directory and point index directory path to temp dir
                        tempDir = Files.createTempDirectory("lucene-in-memory-backup");
                        copyInMemoryLuceneIndexToDirectory(commit, tempDir);
                        indexDirectoryPath = tempDir;
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
                    Logger.getAnonymousLogger().info(format(
                            "Incremental backup performed. dir=%s, added=%d, deleted=%d, took=%dms",
                            destinationPath, toAdd.size(), toDelete.size(), backupEndTime - backupStartTime));
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

    private void copyInMemoryLuceneIndexToDirectory(IndexCommit commit, Path directoryPath) throws IOException {
        Directory from = commit.getDirectory();
        try (Directory to = new NIOFSDirectory(directoryPath)) {
            for (String filename : commit.getFileNames()) {
                to.copyFrom(from, filename, filename, IOContext.DEFAULT);
            }
        }
    }
}
