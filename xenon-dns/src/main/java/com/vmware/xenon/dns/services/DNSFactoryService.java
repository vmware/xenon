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

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Factory service for dns records
 */
public class DNSFactoryService extends FactoryService {
    public static final String SELF_LINK = ServiceUriPaths.DNS + "/service-records";

    public DNSFactoryService() {
        super(DNSService.DNSServiceState.class);
        super.toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new DNSService();
    }

    public static Operation createPost(URI dnsServerURI, ServiceHost currentHost,
                                       String serviceLink, String serviceKind, Set<String> tags,
                                       String healthCheckLink, long healthCheckIntervalSeconds) {
        DNSService.DNSServiceState dnsServiceState = new DNSService.DNSServiceState();
        dnsServiceState.nodeReferences = new HashSet<>();
        dnsServiceState.nodeReferences.add(currentHost.getUri());
        dnsServiceState.serviceLink = serviceLink;
        dnsServiceState.serviceName = serviceKind;
        dnsServiceState.tags = tags;
        dnsServiceState.healthCheckLink = healthCheckLink;
        dnsServiceState.healthCheckIntervalSeconds = healthCheckIntervalSeconds;
        dnsServiceState.documentSelfLink = serviceKind;

        Operation operation = Operation.createPost(
                UriUtils.extendUri(dnsServerURI, DNSFactoryService.SELF_LINK))
                .setBody(dnsServiceState);
        operation.setReferer(currentHost.getUri());
        return operation;
    }
}
