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

package com.vmware.dcp.common;

import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;

public class MinimalFileStore extends StatefulService {

    public File outFile = null;
    private AsynchronousFileChannel file = null;

    public static class MinimalFileState extends ServiceDocument {
        public URI fileUri;
        // lets assume no one is going to POST a hole (which is valid, btw).
        public boolean fileComplete = false;
        public int accumulatedBytes = 0;
    }

    public MinimalFileStore() {
        super(MinimalFileState.class);
    }

    @Override
    public void handleStart(Operation start) {
        if (!start.hasBody()) {
            start.fail(new IllegalStateException("no body given"));
            return;
        }

        MinimalFileState state = start.getBody(MinimalFileState.class);

        this.outFile = new File(state.fileUri);
        try {
            this.file = AsynchronousFileChannel.open(this.outFile.toPath(),
                    StandardOpenOption.WRITE);
        } catch (Exception e) {
            start.fail(e);
            return;
        }

        start.complete();
    }

    @Override
    public void handlePut(Operation put) {
        if (!put.hasBody()) {
            put.fail(new IllegalStateException("no data to write"));
            return;
        }

        MinimalFileState state = this.getState(put);

        if (this.file == null) {
            if (state.fileComplete) {
                put.fail(new IllegalArgumentException("writing to closed file"));
                return;
            }

            try {

            } catch (Exception e) {
                put.fail(e);
                return;
            }
        }

        String rangeString = put.getRequestHeader(Operation.CONTENT_RANGE_HEADER);

        try {
            FileUtils.ContentRange r = new FileUtils.ContentRange(rangeString);
            ByteBuffer b = ByteBuffer.wrap((byte[]) put.getBodyRaw());
            AsynchronousFileChannel ch = this.file;
            File outFile = this.outFile;

            ch.write(b, r.start, put,
                    new CompletionHandler<Integer, Operation>() {

                        @Override
                        public void completed(Integer bytesWritten, Operation op) {
                            MinimalFileState s = getState(op);
                            s.accumulatedBytes += bytesWritten;
                            if (s.accumulatedBytes >= r.fileSize) {
                                s.fileComplete = true;
                                try {
                                    logInfo("%s complete (bytes:%d md5:%s)", outFile,
                                            s.accumulatedBytes, FileUtils.md5sum(outFile));
                                    ch.close();
                                } catch (Exception e) {
                                    op.fail(e);
                                    return;
                                }
                            }

                            op.complete();
                        }

                        @Override
                        public void failed(Throwable ex, Operation dc) {
                            put.fail(ex);
                        }
                    });

        } catch (Exception e) {
            put.fail(e);
            return;
        }
    }

    @Override
    public void handleGet(Operation get) {

        final AsynchronousFileChannel readFileCh;

        MinimalFileState state = this.getState(get);
        try {
            readFileCh = AsynchronousFileChannel.open(this.outFile.toPath(),
                    StandardOpenOption.READ);
        } catch (Exception e) {
            get.fail(e);
            return;
        }

        if (!state.fileComplete) {
            get.fail(new IllegalStateException("file is incomplete"));
            return;
        }

        if (state.fileUri == null) {
            get.fail(new IllegalStateException("no file"));
            return;
        }

        try {
            String requestHeader = get.getRequestHeader(Operation.RANGE_HEADER);
            FileUtils.ContentRange r = null;

            // first xfer
            if (requestHeader == null) {
                r = new FileUtils.ContentRange((int) this.outFile.length());
            } else {
                r = FileUtils.ContentRange.fromRangeHeader(requestHeader, this.outFile.length());
            }
            get.addHeader(r.toContentRangeHeader(), true);

            ByteBuffer b = ByteBuffer.allocate((int) (r.end - r.start));
            readFileCh.read(b, r.start, null, new CompletionHandler<Integer, Void>() {

                @Override
                public void completed(Integer arg0, Void v) {
                    b.flip();
                    String contentType = FileUtils.getContentType(state.fileUri);
                    if (contentType != null) {
                        get.setContentType(contentType);
                    }

                    get.setContentLength(b.limit());
                    get.setBodyNoCloning(b.array());

                    try {
                        readFileCh.close();
                    } catch (Exception e) {
                        get.fail(e);
                        return;
                    }
                    get.complete();
                }

                @Override
                public void failed(Throwable arg0, Void v) {
                    get.fail(arg0);
                }
            });
        } catch (Exception e) {
            get.fail(e);
            return;
        }
    }
}
