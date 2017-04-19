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
import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleSharedNameTwoService.ExampleSharedNameTwoState;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

public class TestMultiNodeQueryTask extends BasicTestCase {
    public int numNodes = 3;

    private URI peerUri;

    @Before
    public void setUp() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);

        host.setUpPeerHosts(numNodes);
        host.joinNodesAndVerifyConvergence(numNodes, numNodes, true);
        host.setNodeGroupQuorum(numNodes);
        this.peerUri = this.host.getPeerHostUri();

        for (VerificationHost h : host.getInProcessHostMap().values()) {
            h.startFactory(new ExampleSharedNameOneService());
            h.startFactory(new ExampleSharedNameTwoService());
        }
        host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(this.peerUri, ExampleSharedNameOneService.FACTORY_LINK));
        host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(this.peerUri, ExampleSharedNameTwoService.FACTORY_LINK));
    }

    @After
    public void tearDown() throws Throwable {
        if (this.host == null) {
            return;
        }
        this.host.tearDownInProcessPeers();
        this.host.toggleNegativeTestMode(false);
        this.host.tearDown();
    }

    @Test
    public void sortOnSharedNameNoResults() throws Throwable {
        Query kindClause = Query.Builder.create()
                .addKindFieldClause(ExampleSharedNameTwoState.class)
                .build();

        QueryTask task = QueryTask.Builder.createDirectTask()
                .setQuery(kindClause)
                .addOption(QueryOption.EXPAND_CONTENT)
                .orderAscending("name", ServiceDocumentDescription.TypeName.STRING)
                .setResultLimit(50)
                .build();

        URI queryTaskURI = UriUtils.buildUri(this.peerUri, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        this.host.log(Level.INFO, Utils.toJson(task));
        this.host.createQueryTaskService(queryTaskURI, task, false,true, task, null);
        assertNotNull(task.results);
        assertEquals(Long.valueOf(0), task.results.documentCount);
    }

}
