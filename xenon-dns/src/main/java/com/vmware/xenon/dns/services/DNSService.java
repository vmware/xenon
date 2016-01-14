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
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
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
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.ID)
        public String serviceName;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.OPTIONAL)
        public List<String> tags;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String healthCheckLink;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long healthCheckIntervalSeconds;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String hostName;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public List<URI> serviceReferences;
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

        if (dnsServiceState.healthCheckLink != null
                && dnsServiceState.healthCheckIntervalSeconds > 0) {
            super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
            setMaintenanceIntervalMicros(
                    TimeUnit.SECONDS.toMicros(dnsServiceState.healthCheckIntervalSeconds));
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
        Operation.CompletionHandler completionHandler = (o, e) -> {
            performAvailabilityCheck(post, o, e);
        };
        sendRequest(Operation.createGet(getUri()).setCompletion(completionHandler));
    }

    private void performAvailabilityCheck(Operation maint, Operation o, Throwable e) {
        /*
            The maintenance call is used to validate service availability
            based on Health Check Link provided and the interval.
            1) Attempt a GET on url provided on each service reference.
            2) If at least one health check returns true, mark the service available.
            3) If none of the attempts pass, then mark the service unavailable.
         */
        if (e != null) {
            maint.fail(e);
        }

        OperationJoin.JoinedCompletionHandler handler = (ops, failures) -> {
            DNSServiceState.ServiceStatus serviceStatus = DNSServiceState.ServiceStatus.UNKNOWN;
            if (failures == null || failures.isEmpty()) {
                serviceStatus = DNSServiceState.ServiceStatus.AVAILABLE;
            } else {
                serviceStatus = DNSServiceState.ServiceStatus.UNAVAILABLE;
                for (Operation op : ops.values()) {
                    try {
                        serviceStatus = op.getBody(
                                DNSServiceState.ServiceStatus.class);
                    } catch (Exception e2) {
                        if (op.getStatusCode() == Operation.STATUS_CODE_OK) {
                            serviceStatus = DNSServiceState.ServiceStatus.AVAILABLE;
                        } else if (op.getStatusCode() == Operation.STATUS_CODE_UNAVAILABLE) {
                            serviceStatus = DNSServiceState.ServiceStatus.UNAVAILABLE;
                        } else {
                            serviceStatus = DNSServiceState.ServiceStatus.UNKNOWN;
                        }
                    }
                    if (serviceStatus == DNSServiceState.ServiceStatus.AVAILABLE) {
                        break;
                    }
                }
            }
            populateServiceAvailability(maint, serviceStatus);
        };

        List<Operation> ops = new ArrayList<>();
        DNSServiceState currentState = o.getBody(DNSServiceState.class);
        ops.addAll(currentState.serviceReferences.stream().map(serviceReference -> Operation
                .createGet(UriUtils.extendUri(serviceReference, currentState.healthCheckLink)))
                .collect(Collectors.toList()));
        OperationJoin availabilityCheckJoin = OperationJoin.create();
        availabilityCheckJoin.setOperations(ops);
        availabilityCheckJoin.setCompletion(handler);
        availabilityCheckJoin.sendWith(this);
    }

    private void populateServiceAvailability(Operation maint,
            DNSServiceState.ServiceStatus serviceStatus) {
        DNSServiceState dnsServiceState = new DNSServiceState();
        dnsServiceState.serviceStatus = serviceStatus;
        sendSelfPatch(dnsServiceState);
        maint.complete();
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
        // merge service references
        if (body.serviceReferences != null && !body.serviceReferences.isEmpty()) {
            body.serviceReferences.stream()
                    .filter(serviceReference -> !currentState.serviceReferences
                            .contains(serviceReference))
                    .forEach(serviceReference -> currentState.serviceReferences
                            .add(serviceReference));
        }
        // Make sure we turn on next maintenance if a valid check url interval is provided.
        if (currentState.healthCheckLink != null && currentState.healthCheckIntervalSeconds > 0) {
            super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
            setMaintenanceIntervalMicros(
                    TimeUnit.SECONDS.toMicros(currentState.healthCheckIntervalSeconds));
        } else {
            super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
        }

        patch.setBody(currentState).complete();
    }

    @Override
    public void handlePut(Operation put) {
        if (!put.hasBody()) {
            put.fail(new IllegalArgumentException("body is required"));
            return;
        }

        DNSServiceState newState = put.getBody(DNSServiceState.class);
        DNSServiceState currentState = getState(put);

        if (!validate(currentState, newState)) {
            put.setStatusCode(Operation.STATUS_CODE_CONFLICT);
        } else {

            if (newState.tags != null && !newState.tags.isEmpty()) {
                newState.tags.stream().filter(tag -> !currentState.tags.contains(tag))
                        .forEach(tag -> currentState.tags.add(tag));
            }
            if (newState.serviceReferences != null && !newState.serviceReferences.isEmpty()) {
                newState.serviceReferences.stream()
                        .filter(serviceReference -> !currentState.serviceReferences
                                .contains(serviceReference))
                        .forEach(serviceReference -> currentState.serviceReferences
                                .add(serviceReference));
            }

            setState(put, currentState);
        }

        put.complete();
    }

    private boolean validate(DNSServiceState currentState, DNSServiceState newState) {
        if (!currentState.serviceName.equals(newState.serviceName)) {
            return false;
        }

        if (!currentState.serviceLink.equals(newState.serviceLink)) {
            return false;
        }

        if (currentState.serviceReferences.size() == 0) {
            return false;
        }

        if (!currentState.healthCheckLink.equals(newState.healthCheckLink)) {
            return false;
        }

        if (!currentState.healthCheckIntervalSeconds.equals(newState.healthCheckIntervalSeconds)) {
            return false;
        }

        return true;
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
