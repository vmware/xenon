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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.dcp.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.dcp.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.Utils;

/**
 * Example service
 */
public class ExampleService extends StatefulService {

    public static class ExampleServiceState extends ServiceDocument {
        public static final String FIELD_NAME_KEY_VALUES = "keyValues";
        public static final String FIELD_NAME_COUNTER = "counter";
        public static final String FIELD_NAME_NAME = "name";
        public static final long VERSION_RETENTION_LIMIT = 100;

        @UsageOption(option = PropertyUsageOption.OPTIONAL)
        public Map<String, String> keyValues = new HashMap<>();
        public Long counter;
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String name;
    }

    public ExampleService() {
        super(ExampleServiceState.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        // Example of state validation on start:
        // 1) Require that an initial state is provided
        // 2) Require that the name field is not null
        // A service could also accept a POST with no body or invalid state and correct it

        if (!startPost.hasBody()) {
            startPost.fail(new IllegalArgumentException("initial state is required"));
            return;
        }

        ExampleServiceState s = startPost.getBody(ExampleServiceState.class);
        if (s.name == null) {
            startPost.fail(new IllegalArgumentException("name is required"));
            return;
        }
        logFine("Initial state is %s", Utils.toJsonHtml(s));

        startPost.complete();
    }

    @Override
    public void handlePut(Operation put) {
        ExampleServiceState newState = put.getBody(ExampleServiceState.class);
        ExampleServiceState currentState = getState(put);

        // example of structural validation: check if the new state is acceptable
        if (currentState.name != null && newState.name == null) {
            put.fail(new IllegalArgumentException("name must be set"));
            return;
        }

        updateCounter(newState, currentState, false);

        // replace current state, with the body of the request, in one step
        setState(put, newState);
        put.complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        ExampleServiceState updateState = updateState(patch);
        if (updateState == null) {
            return;
        }
        // updateState method already set the response body with the merged state
        patch.complete();
    }

    private ExampleServiceState updateState(Operation update) {
        // A DCP service handler is state-less: Everything it needs is provided as part of the
        // of the operation. The body and latest state associated with the service are retrieved
        // below.
        ExampleServiceState body = update.getBody(ExampleServiceState.class);
        ExampleServiceState currentState = getState(update);
        boolean hasStateChanged = Utils.mergeWithState(getDocumentTemplate().documentDescription,
                currentState, body);
        // update state

        updateCounter(body, currentState, hasStateChanged);

        if (body.keyValues != null && !body.keyValues.isEmpty()) {
            for (Entry<String, String> e : body.keyValues.entrySet()) {
                currentState.keyValues.put(e.getKey(), e.getValue());
            }
        }

        if (body.documentExpirationTimeMicros != currentState.documentExpirationTimeMicros) {
            currentState.documentExpirationTimeMicros = body.documentExpirationTimeMicros;
        }

        // response has latest, updated state
        update.setBody(currentState);
        return currentState;
    }

    private boolean updateCounter(ExampleServiceState body,
            ExampleServiceState currentState, boolean hasStateChanged) {
        if (body.counter != null) {
            if (currentState.counter == null) {
                currentState.counter = body.counter;
            }
            // deal with possible operation re-ordering by simply always
            // moving the counter up
            currentState.counter = Math.max(body.counter, currentState.counter);
            body.counter = currentState.counter;
            hasStateChanged = true;
        }
        return hasStateChanged;
    }

    /**
     * Provides a default instance of the service state and allows service author to specify
     * indexing and usage options, per service document property
     */
    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument template = super.getDocumentTemplate();
        PropertyDescription pd = template.documentDescription.propertyDescriptions.get(
                ExampleServiceState.FIELD_NAME_KEY_VALUES);

        // instruct the index to deeply index the map
        pd.indexingOptions.add(PropertyIndexingOption.EXPAND);

        PropertyDescription pdName = template.documentDescription.propertyDescriptions.get(
                ExampleServiceState.FIELD_NAME_NAME);

        // instruct the index to enable SORT on this field.
        pdName.indexingOptions.add(PropertyIndexingOption.SORT);

        // instruct the index to only keep the most recent N versions
        template.documentDescription.versionRetentionLimit = ExampleServiceState.VERSION_RETENTION_LIMIT;
        return template;
    }
}
