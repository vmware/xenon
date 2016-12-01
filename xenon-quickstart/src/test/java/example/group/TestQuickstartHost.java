/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

package example.group;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URI;
import java.time.Duration;
import java.util.UUID;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.services.common.RootNamespaceService;

/**
 * Validate that QuickstartHost runs correctly with command line arguments and responds to the root URI
 *
 */
public class TestQuickstartHost {

    private TemporaryFolder tmpFolder = new TemporaryFolder();
    private QuickstartHost quickstartHost;

    @Test
    public void testState() throws Throwable {
        try {
            this.tmpFolder.create();
            startXenonHost();
            verifyXenonHost();
        } finally {
            stopXenonHost();
            this.tmpFolder.delete();
        }
    }

    /**
     * Starts the host and does some validation that it started correctly.
     *
     * @throws Throwable - a failure during startup
     */
    private void startXenonHost() throws Throwable {

        this.quickstartHost = new QuickstartHost();
        String bindAddress = "127.0.0.1";
        String hostId = UUID.randomUUID().toString();

        String[] args = {
                "--port=0",
                "--bindAddress=" + bindAddress,
                "--sandbox=" + this.tmpFolder.getRoot().getAbsolutePath(),
                "--id=" + hostId
        };

        this.quickstartHost.initialize(args);
        this.quickstartHost.start();

        assertEquals(bindAddress, this.quickstartHost.getPreferredAddress());
        assertEquals(bindAddress, this.quickstartHost.getUri().getHost());
        assertEquals(hostId, this.quickstartHost.getId());
        assertEquals(this.quickstartHost.getUri(), this.quickstartHost.getPublicUri());
    }

    /**
     * Query the root URI (/) and validate the response. We're making sure that the Root Namespace
     * Service is running and that it returns at least one selfLink.
     */
    private void verifyXenonHost() {
        TestRequestSender sender = new TestRequestSender(this.quickstartHost);
        URI rootUri = UriUtils.buildUri(this.quickstartHost, RootNamespaceService.class);
        TestContext.waitFor(Duration.ofMillis(10000), () -> {
            Operation get = Operation.createGet(rootUri);
            ServiceDocumentQueryResult result = sender.sendAndWait(get, ServiceDocumentQueryResult.class);
            return result.documentLinks.size() > 0;
        }, () -> {
                fail("wait timed out");
                return "wait timed out";
            });
    }

    /**
     * Stops the host and cleans up.
     */
    private void stopXenonHost() {
        this.quickstartHost.stop();
    }
}
