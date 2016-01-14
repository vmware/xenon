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
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;

/**
 * Service that represents DNS records
 */

public class DNSService extends StatefulService {

    public static class DNSServiceState extends ServiceDocument {
        public static final String FIELD_NAME_SERVICE_NAME = "serviceName";
        public static final String FIELD_NAME_SERVICE_TAGS = "tags";

        public enum ServiceStatus {
          AVAILABLE, UNKNOWN, UNAVAILABLE
        }

        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String serviceLink;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String serviceName;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.OPTIONAL)
        public List<String> tags;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public URI healthReference;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long healthCheckIntervalSecs;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String ipAddress;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String hostName;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long port;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public ServiceStatus serviceStatus;
    }

    public DNSService() {
        super(DNSServiceState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
        super.toggleOption(ServiceOption.ENFORCE_QUORUM, true);

    }

    @Override
    public void handleStart(Operation start) {
        DNSServiceState dnsServiceState = start.getBody(DNSServiceState.class);

        if ( dnsServiceState.healthReference != null && dnsServiceState.healthCheckIntervalSecs > 0) {
            super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
            setMaintenanceIntervalMicros(TimeUnit.SECONDS.toMicros(dnsServiceState.healthCheckIntervalSecs));
        }

        start.complete();
    }

    public void sendSelfPatch(DNSServiceState newState) {
        Operation patch = Operation.createPatch(getUri())
                .setBody(newState);
        sendRequest(patch);

    }

    @Override
    public void handleMaintenance(Operation post) {
        /*
            The maintenance call is used to validate service state
            based on Check url provided and the interval.
            1) Fetch the current state
            2) If a valid check url is provided, attempt a GET on url provided
            3) On HTTP_OK, mark the serviceStatus to the http response if passed, else mark based
            on HTTP response code.

            Maintenance is turned off if the health check information is invalid.
         */
        Operation.CompletionHandler completionHandler = (o, e) -> {
            if (e != null) {
                logWarning("Failure reading service lookup data: %s", e.toString());
                post.fail(e);
                return;
            }
            DNSServiceState currentState = o.getBody(DNSServiceState.class);

            Operation.CompletionHandler nestedCompletionHandler = (o1, e1) -> {
                DNSServiceState dnsServiceState = new DNSServiceState();
                if (e1 != null) {
                    dnsServiceState.serviceStatus = DNSServiceState.ServiceStatus.UNAVAILABLE;
                } else {
                    try {
                        dnsServiceState.serviceStatus = o1.getBody(
                                DNSServiceState.ServiceStatus.class);
                    } catch (Exception e2) {
                        if (o1.getStatusCode() == Operation.STATUS_CODE_OK) {
                            dnsServiceState.serviceStatus = DNSServiceState.ServiceStatus.AVAILABLE;
                        } else if (o1.getStatusCode() == Operation.STATUS_CODE_UNAVAILABLE) {
                            dnsServiceState.serviceStatus = DNSServiceState.ServiceStatus.UNAVAILABLE;
                        } else {
                            dnsServiceState.serviceStatus = DNSServiceState.ServiceStatus.UNKNOWN;
                        }
                    }
                }
                sendSelfPatch(dnsServiceState);
                post.complete();
            };

            URI checkURI = currentState.healthReference;
            Operation checkOp = Operation.createGet(checkURI)
                    .setCompletion(nestedCompletionHandler);
            sendRequest(checkOp);
        };
        sendRequest(Operation.createGet(getUri()).setCompletion(completionHandler));
    }

    @Override
    public void handlePatch(Operation patch) {
        DNSServiceState currentState = getState(patch);
        DNSServiceState body = patch.getBody(DNSServiceState.class);

        Utils.mergeWithState(getDocumentTemplate().documentDescription, currentState, body);

        // merge tags
        if (body.tags != null && !body.tags.isEmpty()) {
            body.tags.stream().filter(tag -> !currentState.tags.contains(tag))
                    .forEach(tag -> currentState.tags.add(tag));
        }
        // Make sure we turn on next maintenance if a valid check url interval is provided.
        if (currentState.healthReference != null && currentState.healthCheckIntervalSecs > 0) {
            super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
            setMaintenanceIntervalMicros(TimeUnit.SECONDS.toMicros(currentState.healthCheckIntervalSecs));
        } else {
            super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
        }

        patch.setBody(currentState).complete();
    }


    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument d = super.getDocumentTemplate();
        PropertyDescription pdTags = d.documentDescription.propertyDescriptions
                .get(DNSServiceState.FIELD_NAME_SERVICE_TAGS);
        pdTags.indexingOptions = EnumSet.of(PropertyIndexingOption.EXPAND);
        return d;
    }
}
