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
import java.util.logging.Level;

import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Provides host information and allows for host configuration. It can also be used to determine
 * host health and availability
 */
public class ServiceHostManagementService extends StatefulService {
    public static final String SELF_LINK = UriUtils.buildUriPath(ServiceUriPaths.CORE_MANAGEMENT);

    public ServiceHostManagementService() {
        super(ServiceHostState.class);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
    }

    public static enum OperationTracingEnable {
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

        /** Where the file should go **/
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

        /** Where the file to download exists **/
        public URI destination;

        /** Request kind **/
        public String kind;
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

            this.getHost().setOperationTracingLevel(start ? Level.ALL : Level.OFF);
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

        LuceneDocumentIndexService.BackupRequest luceneBackup = new LuceneDocumentIndexService.BackupRequest();
        luceneBackup.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;

        // Lucene gave us a zip file.  Now upload it.
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }

            // upload the result.
            LuceneDocumentIndexService.BackupRequest r = o
                    .getBody(LuceneDocumentIndexService.BackupRequest.class);

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
                uploadFile(op, r.backupFile, req);
            } catch (Exception ex) {
                op.fail(ex);
            }
        };

        sendRequest(Operation.createPatch(indexServiceUri).setBody(luceneBackup).setCompletion(c));
    }

    private void uploadFile(Operation op, URI file, BackupRequest req) throws Exception {
        File f = new File(file);

        Operation post = Operation.createPost(req.destination)
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

        try {
            File fileToDownload = File.createTempFile("restore-" + Utils.getNowMicrosUtc(), ".zip",
                    null);
            final URI backupFileUri = fileToDownload.toURI();

            // download complete.  now restore the zip
            CompletionHandler c = (o, e) -> {
                if (e != null) {
                    op.fail(e);
                    return;
                }

                LuceneDocumentIndexService.RestoreRequest luceneRestore = new LuceneDocumentIndexService.RestoreRequest();
                luceneRestore.documentKind = LuceneDocumentIndexService.RestoreRequest.KIND;
                luceneRestore.backupFile = backupFileUri;

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

            Operation downloadFileOp = Operation.createGet(req.destination)
                    .transferRefererFrom(op).setCompletion(c);
            FileUtils.getFile(this.getHost().getClient(), downloadFileOp, fileToDownload);

        } catch (IOException e) {
            op.fail(e);
        }
    }

}
