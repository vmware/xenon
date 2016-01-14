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

package com.vmware.xenon.dns.services;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;

/**
 * Helper class to start all DNS services
 */
public class DNSServices {

    public static void startServices(ServiceHost host, String[] args) throws Throwable {

        try {
            // start the dns factory service and dns query service
            host.startService(
                    Operation.createPost(UriUtils.buildUri(host, DNSFactoryService.class)),
                    new DNSFactoryService());
            host.startService(
                    Operation.createPost(UriUtils.buildUri(host, DNSQueryService.class)),
                    new DNSQueryService());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
