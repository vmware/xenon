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

package com.vmware.xenon.host;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.host.DecentralizedControlPlaneHost;

public class TestDecentralizedControlPlaneHost {

    @Test
    public void startUpWithArguments() throws Throwable {
        DecentralizedControlPlaneHost h = new DecentralizedControlPlaneHost();
        try {
            String bindAddress = "127.0.0.1";
            String hostId = UUID.randomUUID().toString();

            String[] args = {
                    "--port=0",
                    "--bindAddress=" + bindAddress,
                    "--id=" + hostId
            };

            h.initialize(args);

            h.start();

            assertEquals(bindAddress, h.getPreferredAddress());
            assertEquals(bindAddress, h.getUri().getHost());
            assertEquals(hostId, h.getId());
            assertEquals(h.getUri(), h.getPublicUri());

        } finally {
            h.stop();
            VerificationHost.cleanupStorage(h.getStorageSandbox());
        }

    }

}
