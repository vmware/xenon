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

package com.vmware.xenon.services.common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.esotericsoftware.kryo.serializers.VersionFieldSerializer.Since;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationProcessingChain;
import com.vmware.xenon.common.RequestRouter;
import com.vmware.xenon.common.RequestRouter.Route.RouteDocumentation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.serialization.ReleaseConstants;

/**
 * Example service
 */
public class ExampleService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/examples";


    public static class ExampleODLService extends ExampleService {
        public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/odl-examples";

        /**
         * Create a default factory service that starts instances of this service on POST.
         * This method is optional, {@code FactoryService.create} can be used directly
         */
        public static FactoryService createFactory() {
            return FactoryService.create(ExampleODLService.class);
        }

        public ExampleODLService() {
            super();
            super.toggleOption(ServiceOption.ON_DEMAND_LOAD, true);
            super.toggleOption(ServiceOption.INSTRUMENTATION, false);
        }
    }

    public static class ExampleImmutableService extends ExampleService {
        public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/immutable-examples";

        public static FactoryService createFactory() {
            return FactoryService.create(ExampleImmutableService.class);
        }

        public ExampleImmutableService() {
            super();
            super.toggleOption(ServiceOption.IMMUTABLE, true);
            super.toggleOption(ServiceOption.ON_DEMAND_LOAD, true);
            super.toggleOption(ServiceOption.INSTRUMENTATION, false);
        }
    }

    public static class ExampleNonPersistedService extends ExampleService {
        public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/nonpersist-examples";

        public static FactoryService createFactory() {
            return FactoryService.create(ExampleNonPersistedService.class);
        }

        public ExampleNonPersistedService() {
            super();
            toggleOption(ServiceOption.PERSISTENCE, false);
            super.toggleOption(ServiceOption.INSTRUMENTATION, false);
        }
    }

    /**
     * Request for strict update version check of a service.  This shows an example of implementing
     * compareAndSet operation in a service.  We implement handler that will check the supplied documentVersion
     * with documentVersion in current state and only apply the change of name if the two versions are equal.
     * See {@code getOperationProcessingChain} and {@code handlePatchForStrictUpdate} for implementation details.
     */
    public static class StrictUpdateRequest {
        public static final String KIND = Utils.buildKind(StrictUpdateRequest.class);

        // Field used by RequestRouter to route this 'kind' of request to
        // special handler. See {@code getOperationProcessingChain}.
        public String kind;

        // Field to be updated after version check.
        public String name;

        // Version to match before update.
        public long documentVersion;
    }

    /**
     * Create a default factory service that starts instances of this service on POST.
     * This method is optional, {@code FactoryService.create} can be used directly
     */
    public static FactoryService createFactory() {
        return FactoryService.create(ExampleService.class);
    }

    public static class ExampleServiceState extends ServiceDocument {
        public static final String FIELD_NAME_KEY_VALUES = "keyValues";
        public static final String FIELD_NAME_COUNTER = "counter";
        public static final String FIELD_NAME_SORTED_COUNTER = "sortedCounter";
        public static final String FIELD_NAME_NAME = "name";
        public static final String FIELD_NAME_TAGS = "tags";
        public static final String FIELD_NAME_ID = "id";
        public static final String FIELD_NAME_REQUIRED = "required";
        public static final String FIELD_NAME_IS_FROM_MIGRATION = "isFromMigration";
        public static final long VERSION_RETENTION_LIMIT = 100;
        public static final long VERSION_RETENTION_FLOOR = 20;

        @UsageOption(option = PropertyUsageOption.OPTIONAL)
        @PropertyOptions(indexing = { PropertyIndexingOption.EXPAND,
                PropertyIndexingOption.FIXED_ITEM_NAME })
        @Documentation(description = "@KEYVALUE", exampleString = "{ \"key1\" : \"value1\", \"key2\", \"value2\" }")
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Map<String, String> keyValues = new HashMap<>();
        @Documentation(description = "Version counter")
        public Long counter;
        @PropertyOptions(indexing = PropertyIndexingOption.SORT)
        public Long sortedCounter;
        @Documentation(description = "@NAME", exampleString = "myExample")
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        @PropertyOptions(indexing = PropertyIndexingOption.SORT)
        public String name;
        @Documentation(description = "@TAGS", exampleString = "{ \"tag1\" , \"tag2\" }")
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Set<String> tags = new HashSet<>();
        @UsageOption(option = PropertyUsageOption.ID)
        @UsageOption(option = PropertyUsageOption.REQUIRED)
        public String id;
        @UsageOption(option = PropertyUsageOption.REQUIRED)
        public String required;
        @PropertyOptions(
                usage = PropertyUsageOption.SERVICE_USE,
                indexing = PropertyIndexingOption.EXCLUDE_FROM_SIGNATURE)
        @Since(ReleaseConstants.RELEASE_VERSION_1_5_1)
        public Boolean isFromMigration;
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

        s.isFromMigration = startPost.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_FROM_MIGRATION_TASK);

        startPost.complete();
    }

    @Override
    @RouteDocumentation(description = "@PUT")
    public void handlePut(Operation put) {
        ExampleServiceState newState = getBody(put);
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
    @RouteDocumentation(description = "Update selected fields of example document")
    public void handlePatch(Operation patch) {
        updateState(patch);
        // updateState method already set the response body with the merged state
        patch.complete();
    }

    /**
     * A chain of filters, each of them is a {@link java.util.function.Predicate <Operation>}. When {@link #processRequest} is called
     * the filters are evaluated sequentially, where each filter's {@link java.util.function.Predicate <Operation>#test} can return
     * <code>true</code> to have the next filter in the chain continue process the request or
     * <code>false</code> to stop processing.
     */
    @Override
    public OperationProcessingChain getOperationProcessingChain() {
        if (super.getOperationProcessingChain() != null) {
            return super.getOperationProcessingChain();
        }

        RequestRouter myRouter = new RequestRouter();
        myRouter.register(
                Action.PATCH,
                new RequestRouter.RequestBodyMatcher<>(
                        StrictUpdateRequest.class, "kind",
                        StrictUpdateRequest.KIND),
                this::handlePatchForStrictUpdate, "Strict update version check");

        OperationProcessingChain opProcessingChain = new OperationProcessingChain(this);
        opProcessingChain.add(myRouter);
        setOperationProcessingChain(opProcessingChain);
        return opProcessingChain;
    }

    private void handlePatchForStrictUpdate(Operation patch) {
        ExampleServiceState currentState = getState(patch);
        StrictUpdateRequest body = patch.getBody(StrictUpdateRequest.class);

        if (body.kind == null || !body.kind.equals(Utils.buildKind(StrictUpdateRequest.class))) {
            patch.fail(new IllegalArgumentException("invalid kind: %s" + body.kind));
            return;
        }

        if (body.name == null) {
            patch.fail(new IllegalArgumentException("name is required"));
            return;
        }

        if (body.documentVersion != currentState.documentVersion) {
            String errorString = String
                    .format("Current version %d. Request version %d",
                            currentState.documentVersion,
                            body.documentVersion);
            patch.fail(new IllegalArgumentException(errorString));
            return;
        }

        currentState.name = body.name;
        patch.setBody(currentState);
        patch.complete();
    }

    private ExampleServiceState updateState(Operation update) {
        // A DCP service handler is state-less: Everything it needs is provided as part of the
        // of the operation. The body and latest state associated with the service are retrieved
        // below.
        ExampleServiceState body = getBody(update);
        ExampleServiceState currentState = getState(update);

        // use helper that will merge automatically current state, with state supplied in body.
        // Note the usage option PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL has been set on the
        // "name" field.
        boolean hasStateChanged = Utils.mergeWithState(getStateDescription(),
                currentState, body);

        updateCounter(body, currentState, hasStateChanged);

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

    @Override
    public void handleDelete(Operation delete) {
        if (!delete.hasBody()) {
            delete.complete();
            return;
        }

        // A DELETE can be used to both stop the service, mark it deleted in the index
        // so its excluded from queries, but it can also set its expiration so its state
        // history is permanently removed
        ExampleServiceState currentState = getState(delete);
        ExampleServiceState st = delete.getBody(ExampleServiceState.class);
        if (st.documentExpirationTimeMicros > 0) {
            currentState.documentExpirationTimeMicros = st.documentExpirationTimeMicros;
        }
        delete.complete();
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

        PropertyDescription pdTags = template.documentDescription.propertyDescriptions.get(
                ExampleServiceState.FIELD_NAME_TAGS);

        // instruct the index to deeply index the set of tags
        pdTags.indexingOptions.add(PropertyIndexingOption.EXPAND);

        PropertyDescription pdName = template.documentDescription.propertyDescriptions.get(
                ExampleServiceState.FIELD_NAME_NAME);

        // instruct the index to enable SORT on this field.
        pdName.indexingOptions.add(PropertyIndexingOption.SORT);

        // instruct the index to only keep the most recent N versions
        template.documentDescription.versionRetentionLimit = ExampleServiceState.VERSION_RETENTION_LIMIT;
        template.documentDescription.versionRetentionFloor = ExampleServiceState.VERSION_RETENTION_FLOOR;
        return template;
    }
}
