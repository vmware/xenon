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
import java.util.ArrayList;
import java.util.List;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;

public class DNSUtils {

    public static Operation registerServiceOp(URI dnsServerURI, ServiceHost currentHost,
            String serviceLink, String serviceKind, List<String> tags,
            String healthCheckLink, long healthCheckIntervalSeconds) {
        DNSService.DNSServiceState dnsServiceState = new DNSService.DNSServiceState();
        dnsServiceState.serviceReferences = new ArrayList<>();
        dnsServiceState.serviceReferences.add(currentHost.getUri());
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
