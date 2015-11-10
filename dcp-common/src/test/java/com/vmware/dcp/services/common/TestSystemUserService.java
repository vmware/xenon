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

package com.vmware.dcp.services.common;

import java.net.URI;

import org.junit.Test;

import com.vmware.dcp.common.BasicTestCase;
import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.UriUtils;

public class TestSystemUserService extends BasicTestCase {

    @Test
    public void systemUser() throws Throwable {
        URI uri = UriUtils.buildUri(this.host, SystemUserService.SELF_LINK);
        Operation get = Operation
                .createGet(uri)
                .setCompletion(this.host.getCompletion());

        this.host.testStart(1);
        this.host.send(get);
        this.host.testWait();
    }
}
