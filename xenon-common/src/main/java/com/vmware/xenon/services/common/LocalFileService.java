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
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;

/**
 * Map to a file in local filesystem for reading and writing.
 *
 * @see ServiceHostManagementService
 */
public class LocalFileService extends StatefulService {

    public static final String SERVICE_PREFIX = "/local-files";

    public enum FileType {
        READ, WRITE
    }

    public static class LocalFileServiceState extends ServiceDocument {
        // indicate the doc is for read or write
        public FileType type;

        // local file path to read or write
        public String localFilePath;

        // Allow target file to be overridden (Only for write)
        public boolean writeOverride = false;

        // (Only for write)
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public int writeProcessedBytes = 0;

        // (Only for write) Optional value to indicate file writing is completed.
        // Caller(file uploader) can patch this flag to indicate upload has finished or not.
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public boolean writeFinished;
    }

    public LocalFileService() {
        super(LocalFileServiceState.class);
    }

    @Override
    public void handleStart(Operation start) {
        if (!start.hasBody()) {
            start.fail(new IllegalStateException("no body given"));
            return;
        }

        LocalFileServiceState state = start.getBody(LocalFileServiceState.class);
        if (state.type == null) {
            start.fail(new IllegalStateException("type needs to be WRITE or READ"));
            return;
        }

        start.complete();
    }

    /**
     * Store body on received put operation into filesystem.
     *
     * @see ServiceHostManagementService#handleBackupRequest
     */
    @Override
    public void handlePut(Operation put) {
        if (!put.hasBody()) {
            put.fail(new IllegalStateException("no data to write"));
            return;
        }

        LocalFileServiceState state = getState(put);
        if (state.type != FileType.WRITE) {
            put.fail(new IllegalStateException("Invalid service type for backup"));
        }

        String rangeString = put.getRequestHeader(Operation.CONTENT_RANGE_HEADER);

        Set<StandardOpenOption> mode = new HashSet<>();
        mode.add(StandardOpenOption.CREATE);
        mode.add(StandardOpenOption.WRITE);
        if (state.writeOverride) {
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

            channel.write(b, r.start, put, new CompletionHandler<Integer, Operation>() {
                @Override
                public void completed(Integer bytesWritten, Operation op) {
                    LocalFileServiceState s = new LocalFileServiceState();
                    s.writeProcessedBytes += bytesWritten;

                    try {
                        logInfo("%s complete (bytes:%d md5:%s)",
                                path, s.writeProcessedBytes, FileUtils.md5sum(path.toFile()));
                        channel.close();
                    } catch (Exception e) {
                    }
                    // update processed bytes
                    sendSelfPatch(s);
                    put.complete();
                }

                @Override
                public void failed(Throwable ex, Operation op) {
                    logWarning("Backup Failed %s", Utils.toString(ex));
                    try {
                        channel.close();
                    } catch (IOException e) {
                    }
                    put.fail(ex);
                }
            });
        } catch (Exception e) {
            put.fail(e);
            try {
                channel.close();
            } catch (IOException e1) {
            }
        }
    }

    /**
     * Read the file from filesystem when range header is presented.
     * Otherwise handle as a default GET request to the service.
     *
     * @see ServiceHostManagementService#handleRestoreRequest
     */
    @Override
    public void handleGet(Operation get) {

        String rangeHeader = get.getRequestHeader(Operation.RANGE_HEADER);

        if (rangeHeader == null) {
            // not a read request
            super.handleGet(get);
            return;
        }

        LocalFileServiceState state = getState(get);
        if (state.type != FileType.READ) {
            get.fail(new IllegalStateException("FileType should be READ."));
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

    private void sendSelfPatch(LocalFileServiceState state) {
        Operation.createPatch(getUri()).setBody(state).sendWith(this);
    }

    @Override
    public void handlePatch(Operation patch) {
        LocalFileServiceState currentTask = getState(patch);
        LocalFileServiceState patchBody = getBody(patch);
        Utils.mergeWithState(getStateDescription(), currentTask, patchBody);
        patch.complete();
    }
}
