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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Operation;
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
        public Set<String> tags;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String healthCheckLink;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long healthCheckIntervalSeconds;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String hostName;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Set<URI> nodeReferences;
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
            if (e != null) {
                post.fail(e);
            }
            DNSServiceState currentState = o.getBody(DNSServiceState.class);
            URI[] nodeReferenceArray = currentState.nodeReferences.toArray(new URI[currentState.nodeReferences.size()]);
            performAvailabilityCheck(post, nodeReferenceArray, currentState.healthCheckLink, 0);
        };
        sendRequest(Operation.createGet(getUri()).setCompletion(completionHandler));
    }

    private void performAvailabilityCheck(Operation maint, URI[] nodeReferences, String healthCheckLink, int nodeIndex) {
        /*
            The maintenance call is used to validate service availability
            based on Health Check Link provided and the interval.
            1) Attempt a GET on the first nodeReference + healthCheckLink.
            2) If health check returns true, mark the service available.
            3) If health check returns false, attempt the same on next nodeReference + healthCheckLink.
            4) If we exhausted all the nodeReferences mark the service unavailable.
         */


        Operation.CompletionHandler completionHandler = (on, en) -> {
            DNSServiceState.ServiceStatus serviceStatus = DNSServiceState.ServiceStatus.UNKNOWN;
            if (en == null) {
                try {
                    serviceStatus = on.getBody(
                            DNSServiceState.ServiceStatus.class);
                } catch (Exception e2) {
                    if (on.getStatusCode() == Operation.STATUS_CODE_OK) {
                        serviceStatus = DNSServiceState.ServiceStatus.AVAILABLE;
                    } else if (on.getStatusCode() == Operation.STATUS_CODE_UNAVAILABLE) {
                        serviceStatus = DNSServiceState.ServiceStatus.UNAVAILABLE;
                    } else {
                        serviceStatus = DNSServiceState.ServiceStatus.UNKNOWN;
                    }
                }

                if (serviceStatus == DNSServiceState.ServiceStatus.AVAILABLE) {
                    populateServiceAvailability(maint, serviceStatus);
                } else {
                    performAvailabilityCheck(maint, nodeReferences, healthCheckLink, nodeIndex + 1);
                }

            } else {
                performAvailabilityCheck(maint, nodeReferences, healthCheckLink, nodeIndex + 1);
            }
        };



        if (nodeIndex >= nodeReferences.length) {
            populateServiceAvailability(maint, DNSServiceState.ServiceStatus.UNAVAILABLE);
        } else {
            Operation healthCheckOp = Operation
                    .createGet(UriUtils.extendUri(nodeReferences[nodeIndex], healthCheckLink))
                    .setCompletion(completionHandler);
            this.sendRequest(healthCheckOp);
        }
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
        boolean modified;
        modified = Utils.mergeWithState(getDocumentTemplate().documentDescription, currentState, body);
        modified = mergeCollectionState(currentState,body,modified);

        if (modified) {
            // Make sure we turn on next maintenance if a valid check url interval is provided.
            if (currentState.healthCheckLink != null && currentState.healthCheckIntervalSeconds > 0) {
                super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
                setMaintenanceIntervalMicros(
                        TimeUnit.SECONDS.toMicros(currentState.healthCheckIntervalSeconds));
            } else {
                super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
            }
            patch.setBody(currentState);
        }
        patch.complete();
    }

    @Override
    public void handlePut(Operation put) {
        if (!put.hasBody()) {
            put.fail(new IllegalArgumentException("body is required"));
            return;
        }

        DNSServiceState newState = put.getBody(DNSServiceState.class);
        DNSServiceState currentState = getState(put);
        boolean merged = false;

        if (!validate(currentState, newState)) {
            put.setStatusCode(Operation.STATUS_CODE_CONFLICT);
        } else {
            merged = mergeCollectionState(currentState, newState, merged);

            if (merged) {
                setState(put,currentState);
            }
        }

        put.complete();
    }

    private boolean mergeCollectionState(DNSServiceState currentState, DNSServiceState newState, boolean modified) {

        // merge tags
        if (newState.tags != null && !newState.tags.isEmpty()) {
            modified = currentState.tags.addAll(newState.tags);
        }

        // merge service references
        if (newState.nodeReferences != null && !newState.nodeReferences.isEmpty()) {
            modified = currentState.nodeReferences.addAll(newState.nodeReferences) || modified;
        }

        return modified;
    }

    private boolean validate(DNSServiceState currentState, DNSServiceState newState) {
        if (!currentState.serviceName.equals(newState.serviceName)) {
            return false;
        }

        if (!currentState.serviceLink.equals(newState.serviceLink)) {
            return false;
        }

        if (currentState.nodeReferences.size() == 0) {
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
