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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.services.common.LocalFileService.LocalFileServiceState;

@Ignore("https://www.pivotaltracker.com/story/show/140758667")
public class TestLocalFileService extends BasicReusableHostTestCase {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void writeFile() throws Throwable {
        File localFile = this.tmpDir.newFile();

        // create local file service
        String serviceLink = LocalFileService.SERVICE_PREFIX + "/write";

        LocalFileServiceState initialState = new LocalFileServiceState();
        initialState.fileOptions = EnumSet.of(StandardOpenOption.WRITE);
        initialState.localFilePath = localFile.getPath();

        Operation post = Operation.createPost(this.host, serviceLink).setBody(initialState);
        this.host.startService(post, new LocalFileService());
        this.host.waitForServiceAvailable(serviceLink);


        File fileToUpload = new File(getClass().getResource("example_bodies.json").getFile());

        // upload file
        TestContext testCtx = this.host.testCreate(1);
        Operation uploadOp = Operation.createPut(UriUtils.buildUri(this.host, serviceLink))
                .setReferer(this.host.getUri())
                .setCompletion(testCtx.getCompletion());
        FileUtils.putFile(this.host.getClient(), uploadOp, fileToUpload);
        testCtx.await();

        String contentToUpload = new String(Files.readAllBytes(Paths.get(fileToUpload.toURI())));
        String contentUploaded = new String(Files.readAllBytes(Paths.get(localFile.toURI())));

        assertEquals("File should be uploaded", contentToUpload, contentUploaded);
    }

    @Test
    public void readFile() throws Throwable {
        File fileToRead = new File(getClass().getResource("example_bodies.json").getFile());
        String content = new String(Files.readAllBytes(Paths.get(fileToRead.toURI())));

        // create local file service
        String serviceLink = LocalFileService.SERVICE_PREFIX + "/read";

        LocalFileServiceState initialState = new LocalFileServiceState();
        initialState.localFilePath = fileToRead.getPath();

        Operation post = Operation.createPost(this.host, serviceLink).setBody(initialState);
        this.host.startService(post, new LocalFileService());
        this.host.waitForServiceAvailable(serviceLink);

        // get file
        Operation get = Operation.createGet(UriUtils.buildUri(this.host, serviceLink))
                .addRequestHeader(Operation.RANGE_HEADER, String.format("bytes=%d-%d", 0, fileToRead.length()));
        TestRequestSender sender = this.host.getTestRequestSender();
        Operation op = sender.sendAndWait(get);

        String result = new String((byte[]) op.getBodyRaw());
        assertEquals(content, result);
    }
}
