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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ServiceHostLogService.LogServiceState;

/**
 * Read log file lines in reverse (tail), asynchronously.
 */
class AsyncLogFileReader {
    private static final int CHUNK_SIZE = 4096;

    private AsynchronousFileChannel channel;
    private long chunks;
    private int maxLines;
    private int bufferPosition;
    private boolean skippedFirst;
    private ByteBuffer buffer;
    private byte[] remaining;
    private LinkedList<String> lines = new LinkedList<>();
    private String parentSelfLink;

    public static AsyncLogFileReader create(String parentSelfLink) {
        AsyncLogFileReader r = new AsyncLogFileReader();
        r.parentSelfLink = parentSelfLink;
        return r;
    }

    public void start(Operation op, String file, int maxLines) throws IOException {
        this.channel = AsynchronousFileChannel.open(Paths.get(file), StandardOpenOption.READ);
        long size = this.channel.size();
        this.chunks = size / CHUNK_SIZE;
        this.maxLines = maxLines;

        int firstChunkSize = (int) (size % CHUNK_SIZE);

        if (firstChunkSize > 0) {
            this.chunks++;
        } else {
            firstChunkSize = CHUNK_SIZE;
        }

        read(op, firstChunkSize);
    }

    private void close() {
        if (this.channel != null) {
            try {
                this.channel.close();
                this.channel = null;
            } catch (IOException e) {
            }
        }
    }

    private void fail(Throwable e, Operation op) {
        close();
        op.fail(e);
    }

    private void complete(Operation op) {
        close();

        ServiceHostLogService.LogServiceState state = new ServiceHostLogService.LogServiceState();
        state.documentKind = Utils.buildKind(LogServiceState.class);
        state.documentSelfLink = this.parentSelfLink;
        state.items = this.lines;
        op.setBody(state).complete();
    }

    private void handleReadCompletion(Operation op) {
        try {
            this.buffer.limit(this.buffer.capacity());
            if (this.remaining != null) {
                this.buffer.put(this.remaining);
                this.remaining = null;
            }

            this.bufferPosition = this.buffer.capacity() - 1;
            String line;

            while ((line = readLine()) != null) {
                this.lines.addFirst(line);
                if (--this.maxLines <= 0) {
                    complete(op);
                    return;
                }
            }

            read(op, CHUNK_SIZE);
        } catch (Exception e) {
            fail(e, op);
        }
    }

    private void read(Operation op, int length) {
        if (this.chunks-- > 0) {
            int capacity = length + (this.remaining != null ? this.remaining.length : 0);
            long offset = this.chunks * CHUNK_SIZE;

            this.buffer = ByteBuffer.allocate(capacity);
            this.buffer.limit(length);

            try {
                this.channel.read(this.buffer, offset, null,
                        new CompletionHandler<Integer, Void>() {
                            @Override
                            public void completed(Integer result, Void notUsed) {
                                handleReadCompletion(op);
                            }

                            @Override
                            public void failed(Throwable exc, Void notUsed) {
                                fail(exc, op);
                            }
                        });
            } catch (Exception e) {
                fail(e, op);
            }
        } else {
            complete(op);
        }
    }

    private String readLine() throws UnsupportedEncodingException {
        String line = null;
        int i = this.bufferPosition;
        byte[] data = this.buffer.array();

        while (i > -1) {
            if (data[i] == '\n') {
                int offset = i + 1;
                int len = this.bufferPosition - offset + 1;

                this.bufferPosition = i - 1;

                if (!this.skippedFirst) {
                    this.skippedFirst = true;
                } else {
                    line = new String(data, offset, len, Utils.CHARSET);
                    break;
                }
            }

            if (--i < 0) {
                int len = this.bufferPosition + 1;
                if (len > 0) {
                    this.remaining = new byte[len];
                    System.arraycopy(data, 0, this.remaining, 0, len);
                } else {
                    this.remaining = null;
                }
                this.bufferPosition = -1;
                break; // end of file chunk
            }
        }

        if (this.chunks == 0 && this.remaining != null) {
            line = new String(this.remaining, Utils.CHARSET);
            this.remaining = null;
        }

        return line;
    }
}
