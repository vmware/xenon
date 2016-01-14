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
import java.util.List;
import java.util.StringJoiner;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;

public class DNSUtils {
    public static Operation fetchDNSRecordsOp(
            URI dnsServerURI, String serviceName, List<String> tags, boolean available) {

        URI queryURI = UriUtils.extendUri(dnsServerURI,DNSQueryService.SELF_LINK);

        if (serviceName != null) {
            queryURI = UriUtils.appendQueryParam(queryURI,
                    DNSService.DNSServiceState.FIELD_NAME_SERVICE_NAME, serviceName);
        }

        if (available) {
            queryURI = UriUtils.appendQueryParam(queryURI,
                    DNSService.DNSServiceState.FIELD_NAME_SERVICE_AVAILABLE, "");
        }

        if (tags != null && tags.size() > 0) {
            StringJoiner tagString = new StringJoiner(",");
            tags.forEach(tagString::add);
            queryURI = UriUtils.appendQueryParam(queryURI,
                    DNSService.DNSServiceState.FIELD_NAME_SERVICE_TAGS,tags.toString());
        }

        return Operation.createGet(queryURI);
    }

    public static Operation registerServiceOp(URI dnsServerURI, ServiceHost currentHost,
            String serviceLink, String serviceKind, List<String> tags, DNSService.DNSServiceState.Check check) {
        DNSService.DNSServiceState dnsServiceState = new DNSService.DNSServiceState();
        dnsServiceState.ipAddress = currentHost.getPreferredAddress();

        dnsServiceState.id = serviceLink;
        dnsServiceState.serviceName = serviceKind;
        dnsServiceState.port = new Long(currentHost.getPort());
        dnsServiceState.tags = tags;
        dnsServiceState.check = check;

        Operation operation = Operation.createPost(
                UriUtils.extendUri(dnsServerURI, DNSFactoryService.SELF_LINK))
                .setBody(dnsServiceState);
        operation.setReferer(currentHost.getUri());
        return operation;
    }

}
