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
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Map to a file in local filesystem for reading and writing.
 *
 * @see ServiceHostManagementService
 */
public class LocalFileService extends StatefulService {

    public static final String SERVICE_PREFIX = "/local-files";

    public static class LocalFileServiceState extends ServiceDocument {
        // file scheme uri for local file to read or write (ex: "file:/var/backup/backup.zip")
        public URI localFileUri;

        // (Only for write) mode options to open the file for write
        public Set<StandardOpenOption> fileOptions = new HashSet<>();

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
        if (state.localFileUri == null) {
            start.fail(new IllegalStateException("file scheme uri is required"));
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
        if (state.writeFinished) {
            put.fail(new IllegalStateException("writeFinished is set to true"));
            return;
        }

        if (!UriUtils.FILE_SCHEME.equals(state.localFileUri.getScheme())) {
            put.fail(new IllegalStateException("file scheme uri is required"));
            return;
        }

        Path localFilePath;
        try {
            localFilePath = Paths.get(state.localFileUri);
        } catch (Exception e) {
            put.fail(e);
            return;
        }
        Path path = localFilePath;

        String rangeString = put.getRequestHeader(Operation.CONTENT_RANGE_HEADER);

        // open async channel
        AsynchronousFileChannel channel;
        try {
            channel = AsynchronousFileChannel.open(path, state.fileOptions, null);
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
                    try {
                        channel.close();
                        logInfo("%s complete (bytes:%d range:%s-%s md5:%s)",
                                path, bytesWritten, r.start, r.end, FileUtils.md5sum(path.toFile()));
                    } catch (Exception e) {
                        put.fail(e);
                        return;
                    }
                    put.complete();
                }

                @Override
                public void failed(Throwable ex, Operation op) {
                    logWarning("Backup Failed %s", Utils.toString(ex));
                    try {
                        channel.close();
                    } catch (IOException e) {
                    }
                    // fail the put op, sender can retry sending the same range
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
     * For reading a large file, it is recommended to issue multiple get requests with each request reading part of
     * the file specified in range header.
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

        if (!UriUtils.FILE_SCHEME.equals(state.localFileUri.getScheme())) {
            get.fail(new IllegalStateException("file scheme uri is required"));
            return;
        }

        Path localFilePath;
        try {
            localFilePath = Paths.get(state.localFileUri);
        } catch (Exception e) {
            get.fail(e);
            return;
        }
        Path path = localFilePath;

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

            String contentType = FileUtils.getContentType(path.toUri());
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
                    try {
                        channel.close();
                    } catch (IOException e) {
                    }
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

    @Override
    public void handlePatch(Operation patch) {
        LocalFileServiceState currentTask = getState(patch);
        LocalFileServiceState patchBody = getBody(patch);
        Utils.mergeWithState(getStateDescription(), currentTask, patchBody);
        patch.complete();
    }
}
