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
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.logging.Level;

import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.LocalFileService.LocalFileServiceState;

/**
 * Provides host information and allows for host configuration. It can also be used to determine
 * host health and availability
 */
public class ServiceHostManagementService extends StatefulService {
    public static final String SELF_LINK = UriUtils.buildUriPath(ServiceUriPaths.CORE_MANAGEMENT);

    public static final String STAT_NAME_AVAILABLE_MEMORY_BYTES_PREFIX = "availableMemoryBytes";
    public static final String STAT_NAME_AVAILABLE_MEMORY_BYTES_PER_DAY = STAT_NAME_AVAILABLE_MEMORY_BYTES_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
    public static final String STAT_NAME_AVAILABLE_MEMORY_BYTES_PER_HOUR = STAT_NAME_AVAILABLE_MEMORY_BYTES_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;
    public static final String STAT_NAME_AVAILABLE_DISK_BYTES_PREFIX = "availableDiskByte";
    public static final String STAT_NAME_AVAILABLE_DISK_BYTES_PER_DAY = STAT_NAME_AVAILABLE_DISK_BYTES_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
    public static final String STAT_NAME_AVAILABLE_DISK_BYTES_PER_HOUR = STAT_NAME_AVAILABLE_DISK_BYTES_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;
    public static final String STAT_NAME_CPU_USAGE_PCT_PREFIX = "cpuUsagePercent";
    public static final String STAT_NAME_CPU_USAGE_PCT_PER_DAY = STAT_NAME_CPU_USAGE_PCT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
    public static final String STAT_NAME_CPU_USAGE_PCT_PER_HOUR = STAT_NAME_CPU_USAGE_PCT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;

    public static final String STAT_NAME_THREAD_COUNT = "threadCount";
    public static final String STAT_NAME_JVM_THREAD_COUNT_PREFIX = "jvmThreadCount";
    public static final String STAT_NAME_JVM_THREAD_COUNT_PER_DAY = STAT_NAME_JVM_THREAD_COUNT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
    public static final String STAT_NAME_JVM_THREAD_COUNT_PER_HOUR = STAT_NAME_JVM_THREAD_COUNT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;

    public static final String STAT_NAME_HTTP11_CONNECTION_COUNT_PREFIX = "http11ConnectionCount";
    public static final String STAT_NAME_HTTP11_CONNECTION_COUNT_PER_DAY = STAT_NAME_HTTP11_CONNECTION_COUNT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
    public static final String STAT_NAME_HTTP11_CONNECTION_COUNT_PER_HOUR = STAT_NAME_HTTP11_CONNECTION_COUNT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;

    public static final String STAT_NAME_HTTP2_CONNECTION_COUNT_PREFIX = "http2ConnectionCount";
    public static final String STAT_NAME_HTTP2_CONNECTION_COUNT_PER_DAY = STAT_NAME_HTTP2_CONNECTION_COUNT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
    public static final String STAT_NAME_HTTP2_CONNECTION_COUNT_PER_HOUR = STAT_NAME_HTTP2_CONNECTION_COUNT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;

    public static final String STAT_NAME_HTTP11_PENDING_OP_COUNT = "http11PendingOperationCount";
    public static final String STAT_NAME_HTTP2_PENDING_OP_COUNT = "http2PendingOperationCount";

    public static final String STAT_NAME_SERVICE_COUNT = "serviceCount";
    public static final String STAT_NAME_SERVICE_PAUSE_COUNT = "servicePauseCount";
    public static final String STAT_NAME_SERVICE_RESUME_COUNT = "serviceResumeCount";
    public static final String STAT_NAME_SERVICE_CACHE_CLEAR_COUNT = "serviceCacheClearCount";
    public static final String STAT_NAME_ODL_CACHE_CLEAR_COUNT = "onDemandLoadCacheClearCount";
    public static final String STAT_NAME_ODL_STOP_COUNT = "onDemandLoadStopCount";
    public static final String STAT_NAME_ODL_STOP_CONFLICT_COUNT = "onDemandLoadStopConflictCount";
    public static final String STAT_NAME_PAUSE_RESUME_CONFLICT_COUNT = "pauseResumeConflictCount";
    public static final String STAT_NAME_RATE_LIMITED_OP_COUNT = "rateLimitedOperationCount";
    public static final String STAT_NAME_PENDING_SERVICE_DELETION_COUNT = "pendingServiceDeletionCount";

    public ServiceHostManagementService() {
        super(ServiceHostState.class);
        super.toggleOption(ServiceOption.CORE, true);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
    }

    public enum OperationTracingEnable {
        START,
        STOP
    }

    public static class SynchronizeWithPeersRequest {
        public static final String KIND = Utils.buildKind(SynchronizeWithPeersRequest.class);

        public static SynchronizeWithPeersRequest create(String path) {
            SynchronizeWithPeersRequest r = new SynchronizeWithPeersRequest();
            r.kind = SynchronizeWithPeersRequest.KIND;
            r.nodeSelectorPath = path;
            return r;
        }

        public String nodeSelectorPath;

        /** Request kind **/
        public String kind;
    }

    public static class ConfigureOperationTracingRequest {
        public static final String KIND = Utils.buildKind(ConfigureOperationTracingRequest.class);

        public OperationTracingEnable enable = OperationTracingEnable.START;
        public String level;
        /** Request kind **/
        public String kind;
    }

    /**
     * Request to snapshot the index, create an archive of it, and upload it to the given URL with the given
     * credentials.
     */
    public static class BackupRequest {
        public static final String KIND = Utils.buildKind(BackupRequest.class);

        /** Auth token for upload, if any **/
        public String bearer;

        /**
         * Where the file should go
         *
         * Supported UIR scheme: http, https, file with local file
         * When http/https is specified, destination is expected to accept put request with range header.
         *
         * @see LocalFileService
         **/
        public URI destination;

        /** Request kind **/
        public String kind;
    }

    /**
     * Request to snapshot the index, create an archive of it, and upload it to the given URL with the given
     * credentials.
     */
    public static class RestoreRequest {
        public static final String KIND = Utils.buildKind(RestoreRequest.class);

        /** Auth token for upload, if any **/
        public String bearer;

        /**
         * Where the file to download exists
         *
         * Supported UIR scheme: http, https, file with local file
         * When http/https scheme is specified, destination is expected to accept get with range header.
         *
         * @see LocalFileService
         **/
        public URI destination;

        /** Request kind **/
        public String kind;

        /** Recover the data to the specified point in time */
        public Long timeSnapshotBoundaryMicros;
    }

    @Override
    public void handleGet(Operation get) {
        getHost().updateSystemInfo(false);
        ServiceHostState s = getHost().getState();
        s.documentSelfLink = getSelfLink();
        s.documentKind = Utils.buildKind(ServiceHostState.class);
        s.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        get.setBody(s).complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        try {
            if (!patch.hasBody()) {
                throw new IllegalArgumentException("empty body");
            }

            patch.setStatusCode(Operation.STATUS_CODE_NOT_MODIFIED);
            ConfigureOperationTracingRequest tr = patch
                    .getBody(ConfigureOperationTracingRequest.class);

            if (tr.kind.equals(ConfigureOperationTracingRequest.KIND)) {
                // Actuating the operation tracing service doesn't modify the index.
                handleOperationTracingRequest(tr, patch);
                return;
            }

            BackupRequest br = patch.getBody(BackupRequest.class);
            if (br.kind.equals(BackupRequest.KIND)) {
                handleBackupRequest(br, patch);
                return;
            }

            RestoreRequest rr = patch.getBody(RestoreRequest.class);
            if (rr.kind.equals(RestoreRequest.KIND)) {
                handleRestoreRequest(rr, patch);
                return;
            }

            SynchronizeWithPeersRequest sr = patch.getBody(SynchronizeWithPeersRequest.class);
            if (sr.kind.equals(SynchronizeWithPeersRequest.KIND)) {
                handleSynchronizeWithPeersRequest(sr, patch);
                return;
            }

            throw new IllegalArgumentException("unknown request");
        } catch (Throwable t) {
            patch.fail(t);
        }
    }

    @Override
    public void handleDelete(Operation delete) {
        logInfo("Received shutdown request from %s", delete.getReferer());
        boolean isProcessOwner = getHost().isProcessOwner();
        // DELETE to this service causes a graceful host shutdown.
        // Because shut down can take several seconds on an active system
        // we complete the operation right away, and the remote client relies
        // on polling to determine when we really went down. That is the safest
        // option anyway, for clients that really do care
        delete.setStatusCode(Operation.STATUS_CODE_ACCEPTED);
        delete.complete();
        getHost().stop();

        if (isProcessOwner) {
            System.exit(0);
        }
    }

    private void handleSynchronizeWithPeersRequest(SynchronizeWithPeersRequest rr,
            Operation patch) {
        if (rr.nodeSelectorPath == null) {
            patch.fail(new IllegalArgumentException("nodeSelectorPath is required"));
            return;
        }

        if (getHost().getServiceStage(rr.nodeSelectorPath) == null) {
            patch.fail(new IllegalArgumentException(rr.nodeSelectorPath
                    + " is not started on this host"));
            return;
        }

        getHost().scheduleNodeGroupChangeMaintenance(rr.nodeSelectorPath);
        patch.complete();
    }

    private void handleOperationTracingRequest(ConfigureOperationTracingRequest req, Operation op)
            throws Throwable {
        URI operationTracingServiceUri = UriUtils.buildUri(this.getHost(),
                ServiceUriPaths.CORE_OPERATION_INDEX);

        CompletionHandler serviceCompletion = (o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }

            boolean start = req.enable == OperationTracingEnable.START;
            this.logInfo("%s %s", start ? "Started" : "Stopped",
                    operationTracingServiceUri.toString());

            Level level = start ? Level.ALL : Level.OFF;
            try {
                if (req.level != null) {
                    level = Level.parse(req.level);
                }
            } catch (Throwable ex) {
                logSevere(ex);
            }
            this.getHost().setOperationTracingLevel(level);
            op.complete();
        };

        if ((req.enable == OperationTracingEnable.START)) {
            OperationIndexService operationService = new OperationIndexService();
            this.getHost().startService(Operation.createPost(operationTracingServiceUri)
                    .setCompletion(serviceCompletion),
                    operationService);
        } else {
            sendRequest(Operation.createDelete(operationTracingServiceUri).setCompletion(
                    serviceCompletion));
        }
    }

    private void handleBackupRequest(BackupRequest req, Operation op) {
        URI indexServiceUri = UriUtils
                .buildUri(this.getHost(), ServiceUriPaths.CORE_DOCUMENT_INDEX);

        // when local file is specified to the destination, create a LocalFileService and make it to the destination
        boolean createLocalFileService = isLocalFileUri(req.destination);
        URI backupServiceUri = createLocalFileService ? createLocalFileServiceUri() : req.destination;

        LuceneDocumentIndexService.BackupRequest luceneBackup = new LuceneDocumentIndexService.BackupRequest();
        luceneBackup.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;

        // Lucene gave us a zip file.  Now upload it.
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }

            // upload the result.
            LuceneDocumentIndexService.BackupResponse r = o
                    .getBody(LuceneDocumentIndexService.BackupResponse.class);

            if (createLocalFileService) {
                // delete local file service when finished
                op.nestCompletion((ox, ex) -> {
                    Operation.createDelete(backupServiceUri)
                            .setCompletion((deleteOp, deleteOx) -> {
                                if (deleteOx != null) {
                                    logInfo("Failed to delete local file service %s for backup. %s",
                                            backupServiceUri, Utils.toString(deleteOx));
                                    // we will not fail entire backup operation if deletion of local file failed
                                }
                                op.complete();
                            })
                            .sendWith(this);
                });
            }

            op.nestCompletion((ox, ex) -> {
                if (ex != null) {
                    op.fail(ex);
                    return;
                }
                File f = new File(r.backupFile);
                if (!f.delete()) {
                    ox.fail(new IllegalStateException("failed to delete backup file"
                            + r.backupFile.toString()));
                }
                ox.complete();
            });

            try {
                uploadFile(op, r.backupFile, backupServiceUri);
            } catch (Exception ex) {
                op.fail(ex);
            }
        };

        Operation initialPost = Operation.createPost(getHost(), backupServiceUri.getPath())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    sendRequest(Operation.createPatch(indexServiceUri).setBody(luceneBackup).setCompletion(c));
                });

        if (createLocalFileService) {
            LocalFileServiceState body = new LocalFileServiceState();
            body.fileOptions = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            body.localFileUri = req.destination;
            body.documentSelfLink = UriUtils.buildUriPath(LocalFileService.SERVICE_PREFIX, getHost().nextUUID());

            initialPost.setBody(body);

            getHost().startService(initialPost, new LocalFileService());
        } else {
            // skip LocalFileService creation
            initialPost.complete();
        }

    }

    private boolean isLocalFileUri(URI uri) {
        return UriUtils.FILE_SCHEME.equals(uri.getScheme());
    }

    private URI createLocalFileServiceUri() {
        return UriUtils.buildUri(getHost().getUri(), LocalFileService.SERVICE_PREFIX, getHost().nextUUID());
    }

    private void uploadFile(Operation op, URI file, URI destination) throws Exception {
        File f = new File(file);

        Operation post = Operation.createPost(destination)
                .transferRefererFrom(op).setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    op.complete();
                });
        FileUtils.putFile(this.getHost().getClient(), post, f);
    }

    private void handleRestoreRequest(RestoreRequest req, Operation op) {
        URI indexServiceUri = UriUtils
                .buildUri(this.getHost(), ServiceUriPaths.CORE_DOCUMENT_INDEX);

        boolean isLocalRestoreFile = isLocalFileUri(req.destination);

        try {

            File fileToDownload;
            if (isLocalRestoreFile) {
                fileToDownload = new File(req.destination);
            } else {
                fileToDownload = File.createTempFile("restore-" + Utils.getNowMicrosUtc(), ".zip");
            }

            // download complete.  now restore the zip
            CompletionHandler c = (o, e) -> {
                if (e != null) {
                    op.fail(e);
                    return;
                }

                LuceneDocumentIndexService.RestoreRequest luceneRestore = new LuceneDocumentIndexService.RestoreRequest();
                luceneRestore.documentKind = LuceneDocumentIndexService.RestoreRequest.KIND;
                luceneRestore.backupFile = fileToDownload.toURI();
                luceneRestore.timeSnapshotBoundaryMicros = req.timeSnapshotBoundaryMicros;

                op.nestCompletion((ox, ex) -> {
                    if (ex != null) {
                        op.fail(ex);
                        return;
                    }
                    File f = new File(luceneRestore.backupFile);
                    if (!f.delete()) {
                        ox.fail(new IllegalStateException("failed to delete backup file"
                                + luceneRestore.backupFile.toString()));
                    }

                    ox.complete();
                });

                // restore the downloaded zip.
                sendRequest(Operation.createPatch(indexServiceUri).setBody(luceneRestore)
                        .setCompletion((ox, ex) -> {
                            if (ex != null) {
                                op.fail(ex);
                                return;
                            }
                            op.complete();
                        }));
            };

            Operation downloadFileOp = Operation.createGet(req.destination).transferRefererFrom(op).setCompletion(c);

            if (isLocalRestoreFile) {
                // skip download
                downloadFileOp.complete();
            } else {
                FileUtils.getFile(this.getHost().getClient(), downloadFileOp, fileToDownload);
            }

        } catch (IOException e) {
            op.fail(e);
        }
    }

}
