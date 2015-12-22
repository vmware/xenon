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

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;

/**
 * Factory service for dns records
 */
public class DNSFactoryService extends FactoryService {
    public static String SELF_LINK = DNSUriPaths.DNS + "/service-records";

    public DNSFactoryService() {
        super(DNSService.DNSServiceState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new DNSService();
    }
}