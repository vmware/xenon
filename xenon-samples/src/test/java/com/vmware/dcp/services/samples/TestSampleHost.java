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

package com.vmware.dcp.services.samples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.net.URI;
import java.util.UUID;

import org.junit.Test;

import com.vmware.xenon.common.BasicReportTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.samples.SampleHost;
import com.vmware.xenon.services.common.RootNamespaceService;

/**
 * Validate that Sample Host runs correctly and responds to the root URI
 *
 * Note that most tests (including this one) start a VerificationHost. This test also
 * starts a SampleHost. The code is slightly weird because we use the VerificationHost
 * to test the SampleHost. As you read the code, note that this.host is the
 * VerificationHost.
 *
 */
public class TestSampleHost extends BasicReportTestCase {

    private SampleHost sampleHost;

    @Test
    public void testState() throws Throwable {

        try {
            startSampleHost();
            verifySampleHost();
        } finally {
            stopSampleHost();
        }
    }

    /**
     * Starts the sample host and does some validation that it started correctly.
     *
     * @throws Throwable
     */
    private void startSampleHost() throws Throwable {

        this.sampleHost = new SampleHost();
        String bindAddress = "127.0.0.1";
        String hostId = UUID.randomUUID().toString();

        String[] args = {
                "--port=0",
                "--bindAddress=" + bindAddress,
                "--id=" + hostId
        };

        this.sampleHost.initialize(args);
        this.sampleHost.start();

        assertEquals(bindAddress, this.sampleHost.getPreferredAddress());
        assertEquals(bindAddress, this.sampleHost.getUri().getHost());
        assertEquals(hostId, this.sampleHost.getId());
        assertEquals(this.sampleHost.getUri(), this.sampleHost.getPublicUri());
    }

    /**
     * Query the root URI (/) and validate the response. We're making sure that the Root Namespace
     * Service is running and that it returns at least one selfLink.
     *
     * @throws Throwable
     */
    private void verifySampleHost() throws Throwable {

        this.host.waitForServiceAvailable(RootNamespaceService.SELF_LINK);

        URI rootUri = UriUtils.buildUri(this.sampleHost, RootNamespaceService.class);
        this.host.testStart(1);

        // GET the root URI and validate the response
        Operation get = Operation
                .createGet(rootUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    validateRootResponse(o);
                    this.host.completeIteration();
                });
        this.host.send(get);
        this.host.testWait();
    }

    /**
     * The validation of the response from the RootNamespaceService: verifies that we
     * can parse the response and that there is at least one document link.
     *
     * @param response
     */
    private void validateRootResponse(Operation response) {
        ServiceDocumentQueryResult body = response.getBody(ServiceDocumentQueryResult.class);
        assertNotEquals(body, null);
        assertNotEquals(body.documentLinks, null);
        assertNotEquals(body.documentLinks.size(), 0);
    }

    /**
     * Stops the sample host and cleans up.
     */
    private void stopSampleHost() {
        this.sampleHost.stop();
        VerificationHost.cleanupStorage(this.sampleHost.getStorageSandbox());
    }
}
