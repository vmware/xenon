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

import java.net.URI;

import org.junit.Test;

import com.vmware.xenon.services.common.QueryTask;

public class TestODataUtils {

    @Test
    public void testTenantLinksInODataQuery() {
        Operation inputOp = new Operation();
        URI queryUri  = UriUtils.buildUri("foo.com", 80, "/", "$tenantLinks=/links/foo, bar");
        //URI queryUri = UriUtils.appendQueryParam(this.host.getUri(), "$tenantLinks", "foo, bar");
        inputOp.setUri(queryUri);
        QueryTask resultTask = ODataUtils.toQuery(inputOp, true);
        assertTrue(resultTask.tenantLinks.contains("/links/foo"));
        assertTrue(resultTask.tenantLinks.contains("bar"));
    }
}
