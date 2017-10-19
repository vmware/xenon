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

import java.net.URI;
import java.util.EnumSet;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.RequestRouter.Route.RouteDocumentation;
import com.vmware.xenon.common.RequestRouter.Route.RouteDocumentation.ApiResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceStatUtils;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

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

    public static final String STAT_NAME_HTTP11_PENDING_OP_COUNT_PREFIX = "http11PendingOperationCount";
    public static final String STAT_NAME_HTTP11_PENDING_OP_COUNT_PER_DAY = STAT_NAME_HTTP11_PENDING_OP_COUNT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
    public static final String STAT_NAME_HTTP11_PENDING_OP_COUNT_PER_HOUR = STAT_NAME_HTTP11_PENDING_OP_COUNT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;

    public static final String STAT_NAME_HTTP2_PENDING_OP_COUNT_PREFIX = "http2PendingOperationCount";
    public static final String STAT_NAME_HTTP2_PENDING_OP_COUNT_PER_DAY = STAT_NAME_HTTP2_PENDING_OP_COUNT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
    public static final String STAT_NAME_HTTP2_PENDING_OP_COUNT_PER_HOUR = STAT_NAME_HTTP2_PENDING_OP_COUNT_PREFIX
            + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;

    public static final String STAT_NAME_EXECUTOR_QUEUE_DEPTH = "executorQueueDepth";
    public static final String STAT_NAME_SCHEDULED_EXECUTOR_QUEUE_DEPTH = "scheduledExecutorQueueDepth";

    public static final String STAT_NAME_SERVICE_COUNT = "serviceCount";
    public static final String STAT_NAME_SERVICE_CACHE_CLEAR_COUNT = "serviceCacheClearCount";
    public static final String STAT_NAME_SERVICE_CACHE_MISS_COUNT = "serviceCacheMissCount";
    public static final String STAT_NAME_SERVICE_CACHE_HIT_COUNT = "serviceCacheHitCount";
    public static final String STAT_NAME_RATE_LIMITED_OP_COUNT = "rateLimitedOperationCount";
    public static final String STAT_NAME_PENDING_SERVICE_DELETION_COUNT = "pendingServiceDeletionCount";

    public static final String STAT_NAME_AUTO_BACKUP_SKIPPED_COUNT = "autoBackupSkippedCount";
    public static final String STAT_NAME_AUTO_BACKUP_PERFORMED_COUNT = "autoBackupPerformedCount";
    public static final String STAT_NAME_AUTO_BACKUP_FAILED_COUNT = "autoBackupFailedCount";

    public static final String STAT_NAME_AUTHORIZATION_CACHE_SIZE = "authorizationCacheSize";
    public static final String STAT_NAME_AUTHORIZATION_CACHE_INSERT_COUNT = "authorizationCacheInsertCount";

    public ServiceHostManagementService() {
        super(ServiceHostState.class);
        super.toggleOption(ServiceOption.CORE, true);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
    }

    public enum OperationTracingEnable {
        START,
        STOP
    }

    private static class BaseManagementServiceRequest {
        public String kind;
    }

    public static class SynchronizeWithPeersRequest extends BaseManagementServiceRequest {
        public static final String KIND = Utils.buildKind(SynchronizeWithPeersRequest.class);

        public static SynchronizeWithPeersRequest create(String path) {
            SynchronizeWithPeersRequest r = new SynchronizeWithPeersRequest();
            r.kind = SynchronizeWithPeersRequest.KIND;
            r.nodeSelectorPath = path;
            return r;
        }

        public String nodeSelectorPath;
    }

    public static class ConfigureOperationTracingRequest extends BaseManagementServiceRequest {
        public static final String KIND = Utils.buildKind(ConfigureOperationTracingRequest.class);

        public OperationTracingEnable enable = OperationTracingEnable.START;
        public String level;
    }

    public static class ConfigureInboundRequestLogging extends BaseManagementServiceRequest {
        public static final String KIND = Utils.buildKind(ConfigureInboundRequestLogging.class);
        public ServiceHost.RequestLoggingInfo loggingInfo;
    }


    public enum BackupType {

        /**
         * Create a zipped index snapshot file to specified local file.
         * If destination file already exists, it will be overridden.
         */
        ZIP,

        /**
         * Create index snapshot(consists of multiple files) to the specified local directory.
         * If previous snapshot exists in destination directory, incremental backup will be performed.
         */
        DIRECTORY,

        /**
         * Upload zipped index snapshot to specified http/https destination
         */
        STREAM
    }

    /**
     * Request to snapshot the index, create an archive of it, and upload it to the given URL with the given
     * credentials.
     */
    public static class BackupRequest extends BaseManagementServiceRequest {
        public static final String KIND = Utils.buildKind(BackupRequest.class);

        /** Auth token for upload, if any **/
        public String bearer;

        /**
         * Where the file should go
         *
         * Supported URI scheme: http, https, file with local file or directory
         * When http/https is specified, destination is expected to accept put request with range header.
         *
         * @see LocalFileService
         **/
        public URI destination;

        /**
         * Default is set to zip
         */
        public BackupType backupType = BackupType.ZIP;

        /**
         * Link to the backup target document index service
         */
        public String indexServiceLink = ServiceUriPaths.CORE_DOCUMENT_INDEX;

        /**
         * Link to the document index backup/restore service
         */
        public String backupServiceLink = ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP;
    }

    /**
     * Request to snapshot the index, create an archive of it, and upload it to the given URL with the given
     * credentials.
     */
    public static class RestoreRequest extends BaseManagementServiceRequest {
        public static final String KIND = Utils.buildKind(RestoreRequest.class);

        /** Auth token for upload, if any **/
        public String bearer;

        /**
         * Where the file to download exists
         *
         * Supported URI scheme: http, https, file with local file or directory
         * When http/https scheme is specified, destination is expected to accept get with range header.
         *
         * @see LocalFileService
         **/
        public URI destination;

        /** Recover the data to the specified point in time */
        public Long timeSnapshotBoundaryMicros;

        /**
         * Link to the document index backup/restore service
         */
        public String backupServiceLink = ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP;

        /**
         * Link to the restore target document index service
         */
        public String indexServiceLink = ServiceUriPaths.CORE_DOCUMENT_INDEX;

    }

    public static class AutoBackupConfiguration extends BaseManagementServiceRequest {
        public static final String KIND = Utils.buildKind(AutoBackupConfiguration.class);

        /**
         * Enable/Disable auto backup.
         */
        public boolean enable;

    }

    @Override
    public void handleStart(Operation post) {
        initializeStats();
        post.complete();
    }

    private void initializeStats() {
        if (!this.hasOption(ServiceOption.INSTRUMENTATION)) {
            return;
        }

        ServiceStatUtils.getOrCreateHourlyTimeSeriesStat(this, STAT_NAME_AUTO_BACKUP_PERFORMED_COUNT, EnumSet.of(AggregationType.SUM));
        ServiceStatUtils.getOrCreateDailyTimeSeriesStat(this, STAT_NAME_AUTO_BACKUP_PERFORMED_COUNT, EnumSet.of(AggregationType.SUM));

        ServiceStatUtils.getOrCreateHourlyTimeSeriesStat(this, STAT_NAME_AUTO_BACKUP_SKIPPED_COUNT, EnumSet.of(AggregationType.SUM));
        ServiceStatUtils.getOrCreateDailyTimeSeriesStat(this, STAT_NAME_AUTO_BACKUP_SKIPPED_COUNT, EnumSet.of(AggregationType.SUM));
    }

    @RouteDocumentation(
            description = "Describe this host.",
            responses = {
                    @ApiResponse(statusCode = 200, description = "OK", response = ServiceHostState.class)
            })
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

            // HTTP-304 spec doesn't define behavior for PATCH
            // setting this status code here is relying on current xenon behavior and may change in future.
            patch.setStatusCode(Operation.STATUS_CODE_NOT_MODIFIED);
            BaseManagementServiceRequest request = patch.getBody(BaseManagementServiceRequest.class);
            if (request.kind == null || request.kind.equals("")) {
                throw new IllegalArgumentException("kind is required");
            }


            if (request.kind.equals(ConfigureOperationTracingRequest.KIND)) {
                // Actuating the operation tracing service doesn't modify the index.
                ConfigureOperationTracingRequest tr = patch.getBody(ConfigureOperationTracingRequest.class);
                handleOperationTracingRequest(tr, patch);
                return;
            }

            if (request.kind.equals(ConfigureInboundRequestLogging.KIND)) {
                ConfigureInboundRequestLogging lr = patch.getBody(ConfigureInboundRequestLogging.class);
                handleConfigureInboundRequestLogging(lr, patch);
                return;
            }

            if (request.kind.equals(BackupRequest.KIND)) {
                BackupRequest br = patch.getBody(BackupRequest.class);
                handleBackupRequest(br, patch);
                return;
            }

            if (request.kind.equals(RestoreRequest.KIND)) {
                RestoreRequest rr = patch.getBody(RestoreRequest.class);
                handleRestoreRequest(rr, patch);
                return;
            }

            if (request.kind.equals(SynchronizeWithPeersRequest.KIND)) {
                SynchronizeWithPeersRequest sr = patch.getBody(SynchronizeWithPeersRequest.class);
                handleSynchronizeWithPeersRequest(sr, patch);
                return;
            }

            if (request.kind.equals(AutoBackupConfiguration.KIND)) {
                AutoBackupConfiguration autoBackupConfigRequest = patch.getBody(AutoBackupConfiguration.class);
                handleAutoBackupConfigRequest(autoBackupConfigRequest, patch);
                return;
            }

            throw new IllegalArgumentException("unknown request");
        } catch (Exception e) {
            patch.fail(e);
        }
    }

    /**
     * Shutdown the host.
     *
     * When host is the process owner, {@code System.exit(0);} is called at the end.
     */
    @RouteDocumentation(description = "Shuts down this host. If the host is the process owner then the process"
            + " is terminated with exit code 0.")
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
        } else {
            logInfo("This host is NOT the process owner. Skipping System.exit()");
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
            throws Exception {
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
            } catch (Exception ex) {
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

    private void handleConfigureInboundRequestLogging(ConfigureInboundRequestLogging request, Operation op) {
        if (request.loggingInfo == null) {
            op.fail(new IllegalArgumentException("loggingInfo is missing"));
            return;
        }
        getHost().setRequestLoggingInfo(request.loggingInfo);
        op.complete();
    }

    private void handleBackupRequest(BackupRequest req, Operation op) {

        // when http/https is specified as destination, appropriately update backupType since old code may not set it.
        if (req.destination != null) {
            String scheme = req.destination.getScheme();
            if (UriUtils.HTTP_SCHEME.equals(scheme) || UriUtils.HTTPS_SCHEME.equals(scheme)) {
                req.backupType = BackupType.STREAM;
            }
        }

        // delegate backup to backup service
        sendPatchToBackupService(op, req.backupServiceLink, req);
    }

    private void handleRestoreRequest(RestoreRequest req, Operation op) {
        // delegate restore to backup service
        sendPatchToBackupService(op, req.backupServiceLink, req);
    }

    private void handleAutoBackupConfigRequest(AutoBackupConfiguration req, Operation op) {
        getHost().setAutoBackupEnabled(req.enable);
        logInfo("Auto Backup is %s", req.enable ? "enabled" : "disabled");
        op.complete();
    }

    private void sendPatchToBackupService(Operation op, String backupServiceLink, Object body) {
        Operation patch = Operation.createPatch(this, backupServiceLink)
                .transferRequestHeadersFrom(op)
                .transferRefererFrom(op)
                .setExpiration(op.getExpirationMicrosUtc())
                .setBody(body)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    op.transferResponseHeadersFrom(o);
                    op.setBodyNoCloning(o.getBodyRaw());
                    op.complete();
                });
        sendRequest(patch);
    }

}
