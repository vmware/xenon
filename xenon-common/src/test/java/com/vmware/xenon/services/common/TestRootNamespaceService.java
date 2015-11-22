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

import java.net.URI;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReportTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;

public class TestRootNamespaceService extends BasicReportTestCase {

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
                .createGet(serviceUri).setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.send(get);
        this.host.testWait();
    }
}
