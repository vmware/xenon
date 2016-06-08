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

package com.vmware.xenon.common;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Level;

import org.junit.Test;

public class TestFileUtils extends BasicReusableHostTestCase {

    /**
     * Command line argument
     */
    public long fileSize = ((2 * FileUtils.ContentRange.MAX_IN_FLIGHT_CHUNKS) + 1)
            * FileUtils.ContentRange.CHUNK_SIZE;

    @Test
    public void testFileUpload() throws Throwable {
        File inFile = randomFile();

        MinimalFileStore mfs = new MinimalFileStore();
        MinimalFileStore.MinimalFileState mfsState = new MinimalFileStore.MinimalFileState();
        File outFile = File.createTempFile("randomOutput", ".bin", null);
        outFile.deleteOnExit();
        mfsState.fileUri = outFile.toURI();

        mfs = (MinimalFileStore) this.host.startServiceAndWait(mfs, UUID.randomUUID().toString(),
                mfsState);

        Operation post = Operation.createPut(mfs.getUri())
                .setReferer(this.host.getUri())
                .setCompletion(this.host.getCompletion());
        this.host.testStart(1);
        FileUtils.putFile(this.host.getClient(), post, inFile);
        this.host.testWait();

        String inMd5 = FileUtils.md5sum(inFile);
        String outMd5 = FileUtils.md5sum(mfs.outFile);

        assertTrue(inMd5.equals(outMd5));
    }

    @Test
    public void testFileDownload() throws Throwable {
        MinimalFileStore mfs = new MinimalFileStore();

        File inFile = randomFile(this.fileSize);
        inFile.deleteOnExit();

        MinimalFileStore.MinimalFileState mfsState = new MinimalFileStore.MinimalFileState();
        mfsState.fileComplete = true;
        mfsState.fileUri = inFile.toURI();

        mfs = (MinimalFileStore) this.host.startServiceAndWait(mfs, UUID.randomUUID().toString(),
                mfsState);

        File downloadFile = File.createTempFile("randomOutput", ".bin", null);
        downloadFile.deleteOnExit();

        Operation get = Operation.createGet(mfs.getUri())
                .setReferer(this.host.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    if (downloadFile.length() == 0) {
                        this.host.failIteration(new IllegalStateException(
                                "File is still zero length after completion"));
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.testStart(1);
        FileUtils.getFile(this.host.getClient(), get, downloadFile);
        this.host.testWait();

        String inMd5 = FileUtils.md5sum(inFile);
        String outMd5 = FileUtils.md5sum(downloadFile);

        assertTrue(inFile.length() == downloadFile.length());
        assertTrue(inMd5.equals(outMd5));
    }

    @Test
    public void testUpDown() throws Throwable {
        MinimalFileStore mfs = new MinimalFileStore();
        mfs.outFile = File.createTempFile("intermediate", ".bin", null);
        mfs.outFile.deleteOnExit();

        MinimalFileStore.MinimalFileState mfsState = new MinimalFileStore.MinimalFileState();
        mfsState.fileComplete = false;
        mfsState.fileUri = mfs.outFile.toURI();

        mfs = (MinimalFileStore) this.host.startServiceAndWait(mfs, UUID.randomUUID().toString(),
                mfsState);

        File uploadFile = randomFile();
        uploadFile.deleteOnExit();

        Operation post = Operation.createPut(mfs.getUri())
                .setReferer(this.host.getUri())
                .setCompletion(this.host.getCompletion());
        this.host.testStart(1);
        FileUtils.putFile(this.host.getClient(), post, uploadFile);
        this.host.testWait();

        File downloadFile = File.createTempFile("download", ".bin", null);
        downloadFile.deleteOnExit();

        Operation get = Operation.createGet(mfs.getUri())
                .setReferer(this.host.getUri())
                .setCompletion(this.host.getCompletion());
        this.host.testStart(1);
        FileUtils.getFile(this.host.getClient(), get, downloadFile);
        this.host.testWait();

        String inMd5 = FileUtils.md5sum(uploadFile);
        String outMd5 = FileUtils.md5sum(downloadFile);

        assertTrue(uploadFile.length() == downloadFile.length());
        assertTrue(inMd5.equals(outMd5));
    }

    private File randomFile() throws Throwable {
        return randomFile(FileUtils.ContentRange.CHUNK_SIZE * 3);
    }

    private File randomFile(long length) throws Throwable {
        File f = File.createTempFile("randomInput", ".bin", null);

        RandomAccessFile w = new RandomAccessFile(f, "rw");

        byte[] buf = new byte[FileUtils.ContentRange.CHUNK_SIZE];
        long chunkCount = (long) Math.ceil(length / FileUtils.ContentRange.CHUNK_SIZE);
        Random r = new Random();
        for (int i = 0; i <= chunkCount; i++) {
            r.nextBytes(buf);
            w.write(buf);
        }
        w.close();
        this.host.log(Level.INFO, "Created %s (bytes:%d md5:%s)", f, f.length(),
                FileUtils.md5sum(f));

        return f;
    }

}
