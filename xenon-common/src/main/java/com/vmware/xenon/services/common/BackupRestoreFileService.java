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

import static com.vmware.xenon.common.Operation.HEADER_FIELD_VALUE_SEPARATOR;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;

/**
 *
 */
public class BackupRestoreFileService extends StatefulService {

    public static final String FACTORY_LINK = "/backup-restore-files";

    public enum BackupRestoreType {
        BACKUP, RESTORE
    }

    public enum BackupProgress {
        INITIALIZED, STARTED, FINISHED, FAILED;
    }

    public static class BackupRestoreFileServiceState extends ServiceDocument {
        public BackupRestoreType type;
        public String localFilePath;
        // Allow target file to be overridden
        public boolean override = false;
        public int backupProcessedBytes = 0;
        public BackupProgress backupProgress;
    }

    public BackupRestoreFileService() {
        super(BackupRestoreFileServiceState.class);
    }

    @Override
    public void handleStart(Operation start) {
        if (!start.hasBody()) {
            start.fail(new IllegalStateException("no body given"));
            return;
        }

        BackupRestoreFileServiceState state = start.getBody(BackupRestoreFileServiceState.class);
        if (state.type == null) {
            start.fail(new IllegalStateException("type needs to be BACKUP or RESTORE"));
            return;
        }

        if (state.type == BackupRestoreType.BACKUP) {
            state.backupProgress = BackupProgress.INITIALIZED;
        }
        start.complete();
    }

    /**
     * @param put
     */
    @Override
    public void handlePut(Operation put) {
        if (!put.hasBody()) {
            put.fail(new IllegalStateException("no data to write"));
            return;
        }

        BackupRestoreFileServiceState state = getState(put);
        if (state.type != BackupRestoreType.BACKUP) {
            put.fail(new IllegalStateException("Invalid service type for backup"));
        }

        String rangeString = put.getRequestHeader(Operation.CONTENT_RANGE_HEADER);

        Set<StandardOpenOption> mode = new HashSet<>();
        mode.add(StandardOpenOption.CREATE);
        mode.add(StandardOpenOption.WRITE);
        if (state.override) {
            mode.add(StandardOpenOption.TRUNCATE_EXISTING);
        }

        Path path = Paths.get(state.localFilePath);

        // open async channel
        AsynchronousFileChannel channel;
        try {
            channel = AsynchronousFileChannel.open(path, mode, null);
        } catch (IOException e) {
            put.fail(e);
            return;
        }


        try {
            FileUtils.ContentRange r = new FileUtils.ContentRange(rangeString);
            ByteBuffer b = ByteBuffer.wrap((byte[]) put.getBodyRaw());

            state.backupProgress = BackupProgress.STARTED;
            state.backupProcessedBytes = 0;

            channel.write(b, r.start, put, new CompletionHandler<Integer, Operation>() {
                @Override
                public void completed(Integer bytesWritten, Operation op) {
                    BackupRestoreFileServiceState s = getState(op);
                    s.backupProcessedBytes += bytesWritten;
                    if (s.backupProcessedBytes >= r.fileSize) {
                        try {
                            logInfo("%s complete (bytes:%d md5:%s)",
                                    path, s.backupProcessedBytes, FileUtils.md5sum(path.toFile()));
                            channel.close();
                            s.backupProgress = BackupProgress.FINISHED;
                        } catch (Exception e) {
                            s.backupProgress = BackupProgress.FAILED;
                            return;
                        }
                    }
                }

                @Override
                public void failed(Throwable ex, Operation op) {
                    logWarning("Backup Failed %s", Utils.toString(ex));
                    state.backupProgress = BackupProgress.FAILED;

                    try {
                        channel.close();
                    } catch (IOException e) {
                    }
                }
            });
        } catch (Exception e) {
            put.fail(e);
            try {
                channel.close();
            } catch (IOException e1) {
            }
        }

        put.complete();
    }

    @Override
    public void handleGet(Operation get) {

        String rangeHeader = get.getRequestHeader(Operation.RANGE_HEADER);

        if (rangeHeader == null) {
            // not a restore request
            super.handleGet(get);
            return;
        }

        BackupRestoreFileServiceState state = getState(get);
        if (state.type != BackupRestoreType.RESTORE) {
            get.fail(new IllegalStateException("Invalid service type for restore"));
        }

        Path path = Paths.get(state.localFilePath);
        AsynchronousFileChannel channel;
        try {
            channel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
        } catch (Exception e) {
            get.fail(e);
            return;
        }

        try {
            FileUtils.ContentRange r = FileUtils.ContentRange.fromRangeHeader(rangeHeader, path.toFile().length());

            String contentRangeHeader = r.toContentRangeHeader();
            int idx = contentRangeHeader.indexOf(HEADER_FIELD_VALUE_SEPARATOR);
            String name = contentRangeHeader.substring(0, idx);
            String value = contentRangeHeader.substring(idx + 1);
            get.addResponseHeader(name, value);

            String contentType = FileUtils.getContentType(URI.create(state.localFilePath));
            ByteBuffer b = ByteBuffer.allocate((int) (r.end - r.start));
            channel.read(b, r.start, null, new CompletionHandler<Integer, Void>() {

                @Override
                public void completed(Integer result, Void v) {
                    if (contentType != null) {
                        get.setContentType(contentType);
                    }
                    b.flip();
                    get.setContentLength(b.limit());
                    get.setBodyNoCloning(b.array());

                    try {
                        channel.close();
                    } catch (Exception e) {
                        get.fail(e);
                        return;
                    }
                    get.complete();
                }

                @Override
                public void failed(Throwable exe, Void v) {
                    get.fail(exe);
                }
            });
        } catch (Exception e) {
            get.fail(e);

            try {
                channel.close();
            } catch (IOException ex) {
            }
        }
    }
}
