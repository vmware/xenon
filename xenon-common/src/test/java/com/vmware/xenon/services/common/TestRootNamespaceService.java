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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;

public class TestRootNamespaceService extends BasicTestCase {

    @Before
    public void prepare() throws Throwable {
        this.host.waitForServiceAvailable(RootNamespaceService.SELF_LINK);
    }

    @Test
    public void getRootNamespaceService() throws Throwable {
        this.host.waitForServiceAvailable(RootNamespaceService.SELF_LINK);
        URI serviceUri = UriUtils.buildUri(this.host, RootNamespaceService.class);

        this.host.testStart(1);

        // create a root service
        Operation get = Operation
                .createGet(serviceUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.send(get);
        this.host.testWait();
    }

    @Test
    public void getRootNamespaceServiceWithFilter() throws Throwable {
        this.host.waitForServiceAvailable(RootNamespaceService.SELF_LINK);
        URI serviceUri = UriUtils.buildUri(this.host, RootNamespaceService.class);

        // create a root service
        Operation get = Operation
                .createGet(UriUtils.extendUriWithQuery(serviceUri, "$filter", "options eq 'STATELESS'"));

        get = this.host.waitForResponse(get);
        ServiceDocumentQueryResult res = get.getBody(ServiceDocumentQueryResult.class);

        // at least one stateless service
        assertTrue(res.documentLinks.contains("/"));

        // links must be sorted
        ArrayList<String> copy = new ArrayList<>(res.documentLinks);
        Collections.sort(copy);
        assertEquals(copy, res.documentLinks);
    }

    @Test
    public void getRootNamespaceServiceWithIncludesExcludes() throws Throwable {
        this.host.waitForServiceAvailable(RootNamespaceService.SELF_LINK);
        URI serviceUri = UriUtils.buildUri(this.host, RootNamespaceService.class);

        // create a root service
        Operation get = Operation
                .createGet(UriUtils.extendUriWithQuery(serviceUri, "includes", "STATELESS", "excludes", "FACTORY"));

        get = this.host.waitForResponse(get);
        ServiceDocumentQueryResult res = get.getBody(ServiceDocumentQueryResult.class);

        // at least one stateless service
        assertTrue(res.documentLinks.contains("/"));

        // no factory service
        assertFalse(res.documentLinks.contains(ServiceUriPaths.CORE_LOCAL_QUERY_TASKS));

        // links must be sorted
        ArrayList<String> copy = new ArrayList<>(res.documentLinks);
        Collections.sort(copy);
        assertEquals(copy, res.documentLinks);
    }
}