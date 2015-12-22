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
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.StatefulService;

/**
 * Service that represents DNS records
 */

public class DNSService extends StatefulService {

    public static class DNSServiceState extends ServiceDocument {
        public static final String FIELD_NAME_SERVICE_NAME = "serviceName";
        public static final String FIELD_NAME_SERVICE_TAGS = "tags";
        public static final String FIELD_NAME_SERVICE_AVAILABLE = "available";
        public String id;
        public String serviceName;
        public List<String> tags;

        public static class Check {
            public String url;
            public Long interval;
        }

        public Check check;
        public String ipAddress;
        public String hostName;
        public Long port;
        public Boolean available;
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

        if ( dnsServiceState.check != null && dnsServiceState.check.interval != null ) {
            super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
            setMaintenanceIntervalMicros(
                    TimeUnit.SECONDS.toMicros(dnsServiceState.check.interval));
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
                logWarning("Failure reading service lookup data: %s", e.toString());
                post.fail(e);
                return;
            } else {
                DNSServiceState currentState = o.getBody(DNSServiceState.class);
                if ( currentState.check == null ) {
                    super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
                } else {
                    Operation.CompletionHandler nestedCompletionHandler = (o1, e1) -> {
                        DNSServiceState dnsServiceState = new DNSServiceState();
                        if (e1 != null) {
                            dnsServiceState.available = false;
                        } else {
                            dnsServiceState.available = true;
                        }
                        sendSelfPatch(dnsServiceState);
                        post.complete();
                    };

                    try {
                        URI checkURI = new URI(currentState.check.url);
                        Operation checkOp = Operation.createGet(checkURI)
                                .setCompletion(nestedCompletionHandler);
                        sendRequest(checkOp);
                    } catch (URISyntaxException execption) {
                        post.fail(execption);
                    }
                }

            }
        };
        sendRequest(Operation.createGet(getUri()).setCompletion(completionHandler));
    }

    @Override
    public void handlePatch(Operation patch) {
        DNSServiceState currentState = getState(patch);
        DNSServiceState replacementState = patch
                .getBody(DNSServiceState.class);
        updateState(currentState, replacementState);

        if (currentState.check != null && currentState.check.interval != null) {
            super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
            setMaintenanceIntervalMicros(
                    TimeUnit.SECONDS.toMicros(currentState.check.interval));
        } else {
            super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
        }

        patch.setBody(currentState).complete();
    }

    private void updateState(DNSServiceState currentState, DNSServiceState body) {
        if (body.id != null) {
            currentState.id = body.id;
        }
        if (body.serviceName != null) {
            currentState.serviceName = body.serviceName;
        }
        if (body.tags != null) {
            currentState.tags = body.tags;
        }
        if (body.ipAddress != null) {
            currentState.ipAddress = body.ipAddress;
        }
        if (body.hostName != null) {
            currentState.hostName = body.hostName;
        }
        if (body.port != null) {
            currentState.port = body.port;
        }
        if (body.check != null) {
            currentState.check = body.check;
        }
        if (body.available != null) {
            currentState.available = body.available;
        }
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