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

package com.vmware.xenon.services.samples;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.HashMap;
import java.util.logging.Level;

import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.config.TestXenonConfiguration;
import com.vmware.xenon.common.test.VerificationHost;


public class TestIdGeneratorService extends BasicReusableHostTestCase {

    public int nodeCount = 3;

    @BeforeClass
    public static void setUpClass() throws Exception {
        TestXenonConfiguration.override(
                IdRangeService.class,
                "RANGE_INTERVAL",
                "10"
        );
    }

    private void setUpMultiNode() throws Throwable {
        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            h.startFactory(new IdRangeService());
            h.startService(new IdGeneratorService());
        }

        URI idRangeServiceUri = UriUtils.buildUri(this.host.getPeerHost(), IdRangeService.FACTORY_LINK);
        this.host.waitForReplicatedFactoryServiceAvailable(idRangeServiceUri);
    }

    @Test
    public void idGeneratorService() throws Throwable {
        HashMap<Long, Long> idCountMap = new HashMap<>();
        setUpMultiNode();
        int totalIds = 200;
        int idCount = 0;

        for (int i = 0; i < totalIds / 2; i++) {
            VerificationHost peer = this.host.getPeerHost();
            Operation get = Operation.createGet(peer, IdGeneratorService.SELF_LINK);
            IdGeneratorService.IdGeneratorResponse state = sender.sendAndWait(
                    get, IdGeneratorService.IdGeneratorResponse.class);
            this.host.log(Level.INFO, "ID Response: %d - %s", state.uniqueId, state.documentOwner);
            if (state.uniqueId >= 0) {
                assertFalse(idCountMap.containsKey(state.uniqueId));
                idCountMap.put(state.uniqueId, 0L);
                idCount++;
            }
        }

        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            h.startService(new IdGeneratorService());
        }

        this.host.stopHost(this.host.getPeerHost());
        VerificationHost host = VerificationHost.create(0);
        host.start();
        host.startFactory(new IdRangeService());
        host.startService(new IdGeneratorService());
        this.host.addPeerNode(host );
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        for (int i = 0; i < totalIds / 2; i++) {
            VerificationHost peer1 = this.host.getPeerHost();
            Operation get = Operation.createGet(peer1, IdGeneratorService.SELF_LINK);
            IdGeneratorService.IdGeneratorResponse state = sender.sendAndWait(
                    get, IdGeneratorService.IdGeneratorResponse.class);
            this.host.log(Level.INFO, "ID Response: %d - %s", state.uniqueId, state.documentOwner);
            if (state.uniqueId >= 0) {
                assertFalse(idCountMap.containsKey(state.uniqueId));
                idCountMap.put(state.uniqueId, 0L);
                idCount++;
            }
        }

        // Verify that we had enough valid Id generated.
        assertTrue(idCount >= totalIds / 2);

        // Verify new ranges
        for (int i = 0; i < 10; i++) {
            URI uri = UriUtils.buildUri(this.host.getPeerHost(), IdRangeService.FACTORY_LINK);
            uri = UriUtils.extendUri(uri, UriUtils.convertPathCharsFromLink(IdGeneratorService.SELF_LINK));
            Operation get = Operation.createGet(uri);
            IdRangeService.State state = sender.sendAndWait(get, IdRangeService.State.class);
            this.host.log(Level.INFO, "Range: %d - %d", state.minId, state.maxId);
            if (state.minId >= 0) {
                assertFalse(idCountMap.containsKey(state.minId));
                idCountMap.put(state.minId, 0L);
            }
        }
    }
}
