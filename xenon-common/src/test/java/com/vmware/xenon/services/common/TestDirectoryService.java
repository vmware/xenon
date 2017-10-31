/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;

public class TestDirectoryService extends BasicReusableHostTestCase {

    public static final String SELF_LINK = "/sandbox-dir";

    @Before
    public void setup() {
        URI sandbox = this.host.getStorageSandbox();
        this.host.log("Sandbox in %s", sandbox);
        DirectoryContentService dirService = new DirectoryContentService(new File(sandbox).toPath());
        this.host.startService(Operation.createPost(UriUtils.buildUri(this.host, SELF_LINK)), dirService);
        this.host.waitForServiceAvailable(SELF_LINK);
    }

    @Test
    public void testSimpleServe() {
        String existingFile = "/serviceHostState.json";

        Operation re = this.host.getTestRequestSender()
                .sendAndWait(Operation.createGet(this.host, SELF_LINK + existingFile));

        ServiceHostState hs = re.getBody(ServiceHostState.class);
        assertEquals(this.host.getId(), hs.id);
    }

    @Test
    public void testNonExistentFile() {
        String badFile = "/wikipedia.docx";

        FailureResponse response = this.host.getTestRequestSender()
                .sendAndWaitFailure(Operation.createGet(this.host, SELF_LINK + badFile));

        assertEquals(Operation.STATUS_CODE_NOT_FOUND, response.op.getStatusCode());
    }

    @Test
    public void dontServeDirectory() {
        String dirName = "/lucene";
        FailureResponse response = this.host.getTestRequestSender()
                .sendAndWaitFailure(Operation.createGet(this.host, SELF_LINK + dirName));

        assertEquals(Operation.STATUS_CODE_NOT_FOUND, response.op.getStatusCode());
    }

    @Test
    public void dontEscapeGivenRoot() throws IOException {
        // create a file in the parent of the sandbox
        Path fileUnderAttack = new File(this.host.getStorageSandbox()).toPath().getParent().resolve("file.txt");
        Files.write(fileUnderAttack, "top secret document".getBytes(Utils.CHARSET));

        FailureResponse response = this.host.getTestRequestSender()
                .sendAndWaitFailure(Operation.createGet(this.host, SELF_LINK + "/../file.txt"));

        assertEquals(Operation.STATUS_CODE_NOT_FOUND, response.op.getStatusCode());
    }
}
