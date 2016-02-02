/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.OperationSequence;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;

public class MigrationTaskService extends StatefulService {

    public static class State extends ServiceDocument {
        /**
         * URI pointing to the source system.
         */
        public URI sourceNode;
        /**
         * Factory link of the source factory.
         */
        public String sourceFactory;

        /**
         * URI pointing to the destination system.
         */
        public URI destinationNode;
        /**
         * Factory link to post the new data to.
         */
        public String destinationFactory;

        /**
         * (Optional) Link to the service transforming migrated data on the destination system.
         */
        public String transformationService;

        /**
         * (Optional) Result limit for retrieving service documents from source system.
         * Default is set to {@link #DEFAULT_PAGE_SIZE}.
         */
        public Integer resultLimit;
        /**
         * (Optional) Limits query results to contain documents that changed after time in milliseconds.
         */
        public Long changedSinceEpocMillis;
        /**
         * (Optional) Additional query terms used when querying the source system.
         */
        public List<QueryTask.Query> additionalQueryTerms;

        /**
         * (Optional) Status of the migration task.
         */
        public TaskState taskInfo;

        /**
         * Current query page being processed.
         */
        public String currentPageLink;
        /**
         * Number of entities processed thus far.
         */
        public Long processedEntities;

        // output
        /**
         * Timestamp of the newest document migrated. This will only be accurate once the migration finished.
         */
        public Long lastDocumentChangedEpoc;

    }

    private static final Integer DEFAULT_PAGE_SIZE = 500;

    public MigrationTaskService() {
        super(MigrationTaskService.State.class);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        State initState = startPost.getBody(State.class);
        initState = initialize(initState);
        if (TaskState.isFinished(initState.taskInfo)) {
            startPost.complete();
            return;
        }
        if (!verifyState(initState, startPost)) {
            return;
        }
        State patchState = new State();
        if (initState.taskInfo == null) {
            patchState.taskInfo = new TaskState();
            patchState.taskInfo.stage = TaskStage.CREATED;
        }
        startPost.complete();
        Operation.createPatch(this, getSelfLink())
            .setBody(patchState)
            .sendWith(this);
    }

    private State initialize(State initState) {
        if (initState.resultLimit == null) {
            initState.resultLimit = DEFAULT_PAGE_SIZE;
        }
        if (initState.changedSinceEpocMillis == null) {
            initState.changedSinceEpocMillis = 0L;
        }
        if (initState.taskInfo == null) {
            initState.taskInfo = new TaskState();
        }
        if (initState.taskInfo.stage == null) {
            initState.taskInfo.stage = TaskStage.CREATED;
        }
        return initState;
    }

    private boolean verifyState(State state, Operation operation) {
        List<String> errMsgs = new ArrayList<>();
        if (state.sourceFactory == null) {
            errMsgs.add("sourceFactory cannot be null.");
        }
        if (state.sourceNode == null) {
            errMsgs.add("sourceNode cannot be null.");
        }
        if (state.destinationFactory == null) {
            errMsgs.add("destinationFactory cannot be null.");
        }
        if (state.destinationNode == null) {
            errMsgs.add("destinationNode cannot be null.");
        }
        if (!errMsgs.isEmpty()) {
            operation.fail(new IllegalArgumentException(String.join(" ", errMsgs)));
        }
        return errMsgs.isEmpty();
    }

    @Override
    public void handlePatch(Operation patchOperation) {
        State patchState = patchOperation.getBody(State.class);
        State currentState = getState(patchOperation);

        applyPatch(patchState, currentState);
        if (!verifyState(currentState, patchOperation)
                && !verifyPatchedState(currentState, patchOperation)) {
            return;
        }
        if (TaskState.isFinished(currentState.taskInfo)) {
            patchOperation.complete();
            return;
        }

        Operation retrieveData = null;
        if (currentState.taskInfo.stage == TaskStage.CREATED) {
            QuerySpecification query = buildQuerySpecification(currentState);
            retrieveData = Operation.createPost(UriUtils.buildUri(currentState.sourceNode,
                    ServiceUriPaths.CORE_LOCAL_QUERY_TASKS))
                    .setBody(QueryTask.create(query).setDirect(true));
            currentState.taskInfo.stage = TaskStage.STARTED;
        } else {
            retrieveData = Operation.createGet(
                    UriUtils.buildUri(currentState.sourceNode, patchState.currentPageLink));
        }

        patchOperation.complete();

        retrieveData.setCompletion((o, t) -> {
            if (t != null) {
                failTask(t);
                return;
            }
            ServiceDocumentQueryResult results = o.getBody(QueryTask.class).results;
            if (results.documentCount != 0) {
                transformResults(currentState, results);
            } else {
                State progressPatch = buildPatchState(patchState, results, 0);
                Operation.createPatch(this, getSelfLink())
                    .setBody(progressPatch)
                    .sendWith(this);
            }
        })
            .sendWith(this);
    }

    private boolean verifyPatchedState(State state, Operation operation) {
        List<String> errMsgs = new ArrayList<>();
        if (state.taskInfo.stage == TaskStage.STARTED) {
            if (state.currentPageLink != null) {
                errMsgs.add("nextPageLink cannot be null.");
            }
        }
        if (!errMsgs.isEmpty()) {
            operation.fail(new IllegalArgumentException(String.join(" ", errMsgs)));
        }
        return errMsgs.isEmpty();
    }

    private State applyPatch(State patchState, State currentState) {
        currentState.currentPageLink = patchState.currentPageLink;
        currentState.lastDocumentChangedEpoc = Math.max(
                Optional.ofNullable(currentState.lastDocumentChangedEpoc).orElse(0L),
                Optional.ofNullable(patchState.lastDocumentChangedEpoc).orElse(0L));
        currentState.processedEntities = patchState.processedEntities;
        currentState.taskInfo = Optional.ofNullable(patchState.taskInfo)
                .orElse(currentState.taskInfo);
        return currentState;
    }

    private void transformResults(State state, ServiceDocumentQueryResult results) {
        // clean document self link
        Collection<Object> cleanJson = results.documents.values().stream()
                .map(d -> {
                    return removeFactoryPathFromSelfLink(d, state.sourceFactory);
                }).collect(Collectors.toList());

        // post to transformation service
        final Collection<Object> transformedJson = new ArrayList<>();
        if (state.transformationService != null) {
            Collection<Operation> transformations = cleanJson.stream()
                    .map(d -> {
                        return Operation.createPost(UriUtils.buildUri(state.destinationNode,
                                state.transformationService))
                                .setBody(d)
                                .setCompletion((o, t) -> {
                                    if (t != null) {
                                        failTask(t);
                                        return;
                                    }
                                    transformedJson.add(o.getBodyRaw());
                                });
                    })
                    .collect(Collectors.toList());
            OperationJoin.create(transformations)
            .setCompletion((o, t) -> {
                if (t != null) {
                    failTask(t.values());
                    return;
                }
                migrateEntities(transformedJson, state, results);
            })
                .sendWith(this);
            ;
        } else {
            transformedJson.addAll(cleanJson);
            migrateEntities(transformedJson, state, results);
        }
    }

    private void migrateEntities(Collection<Object> json, State state,
            ServiceDocumentQueryResult results) {
        // remove objects that might have been previously migrated to destination
        Collection<Operation> deletes = json.stream()
                .map(d -> {
                    URI newDocumentUri = UriUtils.buildUri(
                            state.destinationNode,
                            state.destinationFactory,
                            Utils.getJsonMapValue(Utils.toJson(d),
                                    ServiceDocument.FIELD_NAME_SELF_LINK, String.class));
                    return Operation.createDelete(newDocumentUri);
                })
                .collect(Collectors.toList());
        OperationSequence sequence = OperationSequence.create(deletes.toArray(new Operation[0]));

        // create objects on destination
        Collection<Operation> posts = json.stream()
                .map(d -> {
                    return Operation.createPost(
                            UriUtils.buildUri(state.destinationNode, state.destinationFactory))
                            .setBody(d);
                })
                .collect(Collectors.toList());
        sequence.next(posts.toArray(new Operation[0]))
        .setCompletion((os, es) -> {
            if (es != null) {
                failTask(es.values());
            }
        })
        // patch current service
        .next(Operation.createPatch(this, getSelfLink())
            .setBody(buildPatchState(state, results, json.size())))
            .sendWith(this);
    }

    private State buildPatchState(
            State currentState,
            ServiceDocumentQueryResult results,
            int processedEntities) {
        State patchState = new State();
        patchState.currentPageLink = results.nextPageLink;
        if (patchState.currentPageLink == null) {
            patchState.taskInfo = new TaskState();
            patchState.taskInfo.stage = TaskStage.FINISHED;
        }
        patchState.lastDocumentChangedEpoc = results.documents.values().stream().mapToLong(d -> {
            return Utils.getJsonMapValue(d, ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                    Long.class);
        }).max().orElse(0);
        patchState.processedEntities = Optional.ofNullable(currentState.processedEntities)
                .orElse(0L) + processedEntities;
        return patchState;
    }

    private Object removeFactoryPathFromSelfLink(Object jsonObject, String factoryPath) {
        String selfLink = extractId(jsonObject, factoryPath);
        return Utils.toJson(
                Utils.setJsonProperty(jsonObject, ServiceDocument.FIELD_NAME_SELF_LINK, selfLink));
    }

    private String extractId(Object jsonObject, String factoryPath) {
        String selfLink = Utils.getJsonMapValue(jsonObject, ServiceDocument.FIELD_NAME_SELF_LINK,
                String.class);
        if (selfLink.startsWith(factoryPath)) {
            selfLink = selfLink.replaceFirst(factoryPath, "");
        }
        return selfLink;
    }

    @SuppressWarnings("unchecked")
    private QuerySpecification buildQuerySpecification(State state) {
        QueryTask.QuerySpecification querySpec = new QueryTask.QuerySpecification();
        querySpec.resultLimit = state.resultLimit;

        QueryTask.Query typeClause = buildWildCardQuery(ServiceDocument.FIELD_NAME_SELF_LINK,
                state.sourceFactory + "*");
        QueryTask.Query timeClause = buildTimeClause(state.changedSinceEpocMillis);

        querySpec.query.addBooleanClause(typeClause);
        querySpec.query.addBooleanClause(timeClause);
        Optional.ofNullable(state.additionalQueryTerms).orElse((List<QueryTask.Query>) Collections.EMPTY_LIST).stream()
            .forEach(q -> querySpec.query.addBooleanClause(q));
        querySpec.options = EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);

        return querySpec;
    }

    private QueryTask.Query buildTimeClause(Long startTimeEpoc) {
        return new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS)
                .setNumericRange(NumericRange.createGreaterThanRange(startTimeEpoc));
    }

    private QueryTask.Query buildWildCardQuery(String property, String value) {
        return buildBaseQuery(property, value)
                .setTermMatchType(QueryTask.QueryTerm.MatchType.WILDCARD);
    }

    private QueryTask.Query buildBaseQuery(String property, String value) {
        return new QueryTask.Query()
                .setTermPropertyName(property)
                .setTermMatchValue(value);
    }

    private void failTask(Throwable t) {
        State patch = new State();
        patch.taskInfo = new TaskState();
        patch.taskInfo.stage = TaskStage.FAILED;
        patch.taskInfo.failure = Utils.toServiceErrorResponse(t);
        Operation.createPost(getUri())
            .setBody(patch)
            .sendWith(this);
    }

    private void failTask(Collection<Throwable> t) {
        failTask(t.iterator().next());
    }
}
