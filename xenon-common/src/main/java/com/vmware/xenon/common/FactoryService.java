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

package com.vmware.xenon.common;

import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.UriUtils.ForwardingTarget;
import com.vmware.xenon.services.common.NodeGroupBroadcastResponse;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTaskUtils;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Implements a POST handler for creating new service instances. Derived implementations should
 * override handlePost, validate the initial state for the new service and complete the request.
 * This class will then create the new instance, store the new state (if service is durable) and
 * respond to the sender
 */
public abstract class FactoryService extends StatelessService {

    /**
     * Creates a factory service instance that starts the specified child service
     * on POST
     */
    public static FactoryService create(Class<? extends Service> childServiceType,
            ServiceOption... options) {
        try {
            Service s = childServiceType.newInstance();
            Class<? extends ServiceDocument> childServiceDocumentType = s.getStateType();
            FactoryService fs = create(childServiceType, childServiceDocumentType, options);
            return fs;
        } catch (Throwable e) {
            Utils.logWarning("Failure creating factory for %s: %s", childServiceType,
                    Utils.toString(e));
            return null;
        }
    }

    /**
     * Creates a factory service instance that starts the specified child service
     * on POST
     */
    public static FactoryService create(Class<? extends Service> childServiceType,
            Class<? extends ServiceDocument> childServiceDocumentType, ServiceOption... options) {
        FactoryService fs = new FactoryService(childServiceDocumentType) {
            @Override
            public Service createServiceInstance() throws Throwable {
                return childServiceType.newInstance();
            }
        };
        Arrays.stream(options).forEach(option -> fs.toggleOption(option, true));
        return fs;
    }

    /**
     * Creates a factory service instance that starts the specified child service
     * on POST. The factory service has {@link ServiceOption#IDEMPOTENT_POST} enabled
     */
    public static FactoryService createIdempotent(Class<? extends Service> childServiceType) {
        return create(childServiceType, ServiceOption.IDEMPOTENT_POST);
    }

    /**
     * Creates a factory service instance that starts the specified child service
     * on POST. The factory service has {@link ServiceOption#IDEMPOTENT_POST} enabled
     */
    public static FactoryService createIdempotent(Class<? extends Service> childServiceType,
            Class<? extends ServiceDocument> childServiceDocumentType) {
        return create(childServiceType, childServiceDocumentType, ServiceOption.IDEMPOTENT_POST);
    }

    public static final Integer SELF_QUERY_RESULT_LIMIT = Integer.getInteger(
            Utils.PROPERTY_NAME_PREFIX
                    + "FactoryService.SELF_QUERY_RESULT_LIMIT", 1000);
    private EnumSet<ServiceOption> childOptions;
    private String nodeSelectorLink = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
    private int selfQueryResultLimit = SELF_QUERY_RESULT_LIMIT;
    private ServiceDocument childTemplate;

    /**
     * Creates a default instance of a factory service that can create and start instances
     * of the supplied service type
     */
    public FactoryService(Class<? extends ServiceDocument> childServiceDocumentType) {
        super(childServiceDocumentType);
        // We accept child service options to be set at factory
        // Turn off STATELESS, so that child services do not inherit the same.
        super.toggleOption(ServiceOption.STATELESS, false);
        super.toggleOption(ServiceOption.FACTORY, true);
        setSelfLink("");
        Service s = createChildServiceSafe();
        if (s == null) {
            throw new IllegalStateException("Could not create service of type "
                    + childServiceDocumentType.toString());
        }
        setSelfLink(null);
        this.childOptions = s.getOptions();
    }

    /**
     * Sets the result limit for child services queries used on service start, synchronization. The
     * result limit throttles the amount of services we load from the index, and also control the
     * overlapping synchronization requests. Higher limits results in faster service restart, but
     * can cause network and memory issues.
     */
    public void setSelfQueryResultLimit(int limit) {
        this.selfQueryResultLimit = limit;
    }

    /**
     * Returns the self query result limit
     */
    public int getSelfQueryResultLimit() {
        return this.selfQueryResultLimit;
    }

    /**
     * Returns true if the option is supported by the child services
     * of the factory.
     */
    public boolean hasChildOption(ServiceOption option) {
        return this.childOptions.contains(option);
    }

    @Override
    public final void handleStart(Operation startPost) {

        try {
            setAvailable(false);
            // create a child service class instance and force generation of its document description
            Service s = createChildService();
            s.setHost(getHost());
            getHost().buildDocumentDescription(s);

            if (this.childOptions.contains(ServiceOption.PERSISTENCE)) {
                toggleOption(ServiceOption.PERSISTENCE, true);
            }

            Class<?> childStateTypeDeclaredInChild = s.getStateType();

            if (!getStateType().equals(childStateTypeDeclaredInChild)) {
                throw new IllegalArgumentException(
                        String.format("Child service state type %s does not match state type "
                                + "declared in child service class (%s)", getStateType(),
                                childStateTypeDeclaredInChild));
            }

            if (s.hasOption(ServiceOption.PERSISTENCE)) {
                byte[] buffer = new byte[Service.MAX_SERIALIZED_SIZE_BYTES];
                // make sure service can be serialized, so it can be paused under memory pressure
                Utils.toBytes(s, buffer, 0);
            }
        } catch (Throwable e) {
            logSevere(e);
            startPost.fail(e);
            return;
        }

        String path = UriUtils.buildUriPath(
                SynchronizationTaskService.FACTORY_LINK,
                UriUtils.convertPathCharsFromLink(this.getSelfLink()));

        // Create a place-holder Synchronization-Task for this factory service
        Operation post = Operation
                .createPost(UriUtils.buildUri(this.getHost(), path))
                .setBody(createSynchronizationTaskState(null))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logSevere(e);
                        startPost.fail(e);
                        return;
                    }

                    if (!ServiceHost.isServiceIndexed(this)) {
                        setAvailable(true);
                        startPost.complete();
                        return;
                    }

                    // complete factory start POST immediately. Asynchronously
                    // kick-off the synchronization-task to load all child
                    // services. Requests to a child not yet loaded will be
                    // queued by the framework.
                    Operation clonedOp = startPost.clone();
                    startPost.complete();

                    if (!this.childOptions.contains(ServiceOption.REPLICATION)) {
                        clonedOp.setCompletion((op, t) -> {
                            if (t != null && !getHost().isStopping()) {
                                logWarning("Failure in kicking-off synchronization-task: %s", t.getMessage());
                                return;
                            }
                        });

                        startFactorySynchronizationTask(clonedOp, null);
                        return;
                    }
                    // when the node group becomes available, the maintenance handler will initiate
                    // service start and synchronization
                });

        SynchronizationTaskService service = SynchronizationTaskService
                .create(() -> createChildServiceSafe());
        this.getHost().startService(post, service);
    }

    /**
     * Complete operation, skipping authorization checks. Default factory
     * implementation applies authorization on the child state during POST (child creation)
     * and during GET result processing (the runtime applies filters on query results).
     * If a service author needs to apply authorization on the factory link, it should override
     * this method and call {@code ServiceHost.isAuthorized()}
     */
    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handleRequest(Operation op) {
        handleRequest(op, OperationProcessingStage.PROCESSING_FILTERS);
    }

    @Override
    public void handleRequest(Operation op, OperationProcessingStage opProcessingStage) {

        if (op.getAction() == Action.POST) {
            if (opProcessingStage == OperationProcessingStage.PROCESSING_FILTERS) {
                OperationProcessingChain opProcessingChain = getOperationProcessingChain();
                if (opProcessingChain != null && !opProcessingChain.processRequest(op)) {
                    return;
                }
                opProcessingStage = OperationProcessingStage.EXECUTING_SERVICE_HANDLER;
            }
            if (opProcessingStage == OperationProcessingStage.EXECUTING_SERVICE_HANDLER) {
                op.nestCompletion((o, e) -> {
                    if (e != null) {
                        logWarning("Service start failed: %s", Utils.toString(e));
                        op.fail(e);
                        return;
                    }
                    handlePostCompletion(op);
                });
                handlePost(op);
            }
        } else if (op.getAction() == Action.GET) {
            if (this.getProcessingStage() != ProcessingStage.AVAILABLE) {
                op.setBody(new ServiceDocumentQueryResult()).complete();
                return;
            }
            op.nestCompletion(this::handleGetCompletion);
            handleGet(op);
        } else if (op.getAction() == Action.DELETE) {
            if (ServiceHost.isServiceStop(op)) {
                op.nestCompletion(o -> {
                    handleStopCompletion(op);
                });
                handleStop(op);
            } else {
                op.nestCompletion(o -> {
                    handleDeleteCompletion(op);
                });
                handleDelete(op);
            }
        } else if (op.getAction() == Action.OPTIONS) {
            op.nestCompletion(this::handleOptionsCompletion);
            handleOptions(op);
        } else {
            op.fail(new IllegalArgumentException("Action not supported"));
        }
    }

    private void handlePostCompletion(Operation o) {
        if (o.getStatusCode() == Operation.STATUS_CODE_ACCEPTED) {
            // the derived class dealt with this operation, do not create a new
            // service
            o.complete();
            return;
        }

        // Request directly from clients must be marked with appropriate PRAGMA, so
        // the runtime knows this is not a restart of a service, but an original, create request
        if (!o.isSynchronize()) {
            o.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_CREATED);
        }

        // create and start service instance. If service is durable, and a body
        // is attached to the POST, a document will be created
        Service childService;
        ServiceDocument initialState = null;
        try {
            childService = createChildService();

            if (o.hasBody()) {
                initialState = o.getBody(this.stateType);
                // before we modify the body, clone it, to isolate our changes from a local client
                // that decided to mutate the body and re-use, after it called
                // sendRequest(op.setBody()). Operation clones on setBody() only, not getBody() if
                // the body is already in native form (not serialized)
                initialState = Utils.clone(initialState);
            }
            String suffix;
            if (initialState == null) {
                // create a unique path that is prefixed by the URI path of the factory
                // We do not use UUID since its not a good primary key, given our index.
                suffix = buildDefaultChildSelfLink();
                initialState = new ServiceDocument();
            } else {
                if (initialState.documentSelfLink == null) {
                    suffix = buildDefaultChildSelfLink();
                } else {
                    // treat the supplied selfLink as a suffix
                    suffix = initialState.documentSelfLink;
                }
            }

            // check suffix does not already contain the prefix i.e. the factory's self link
            URI serviceUri;
            if (UriUtils.isChildPath(suffix, getSelfLink())) {
                serviceUri = UriUtils.buildUri(getHost(), suffix);
            } else {
                serviceUri = UriUtils.extendUri(getUri(), suffix);
            }

            o.addRequestHeader(Operation.REPLICATION_PARENT_HEADER, getSelfLink());
            o.setUri(serviceUri);

        } catch (Throwable e) {
            logSevere(e);
            o.fail(e);
            return;
        }

        initialState.documentSelfLink = o.getUri().getPath();
        initialState.documentKind = Utils.buildKind(this.stateType);
        initialState.documentTransactionId = o.getTransactionId();
        if (hasChildOption(ServiceOption.IMMUTABLE)) {
            // skip cloning since contract with service author is that state
            // can not change after post.complete() is called in handleStart()
            o.setBodyNoCloning(initialState);
        } else {
            o.setBody(initialState);
        }

        if (this.childOptions.contains(ServiceOption.REPLICATION) && !o.isFromReplication()
                && !o.isForwardingDisabled()) {
            // We forward requests even if OWNER_SELECTION is not set: It has a minor perf
            // impact and lets use reuse the synchronization algorithm to replicate the POST
            // across peers. It also helps with convergence and eventual consistency.
            forwardRequest(o, childService);
            return;
        }

        completePostRequest(o, childService);
    }

    protected String buildDefaultChildSelfLink() {
        return Utils.buildUUID(getHost().getIdHash());
    }

    @Override
    public void handleStop(Operation op) {
        if (!ServiceHost.isServiceStop(op)) {
            logWarning("Abrupt stop of factory %s", this.getSelfLink());
        }
        super.handleStop(op);
    }

    private void completePostRequest(Operation o, Service childService) {
        // A SYNCH_OWNER request has an empty ServiceDocument with just the
        // documentSelfLink property set to allow the FactoryService to
        // route the request to the owner node. Other than that the document
        // is empty. The owner node is expected to compute the best state
        // in the node-group and use that for starting the service locally.
        // NOTE: if the service is already started on the owner node,
        // Synchronization will update it.
        if (o.isSynchronizeOwner()) {
            o.setBody(null);
        }

        if (!o.isFromReplication() && !o.isReplicationDisabled()) {
            o.nestCompletion(startOp -> {
                publish(o);
                if (o.getAction() == Action.PUT || !hasOption(ServiceOption.REPLICATION)) {
                    // if the operation was converted to PUT, due to IDEMPOTENT_POST or
                    // the service does not support replication, complete and return
                    o.complete();
                    return;
                }
                o.linkState(null);
                o.complete();
            });

            if (o.isWithinTransaction() && this.getHost().getTransactionServiceUri() != null) {
                childService.sendRequest(TransactionServiceHelper.notifyTransactionCoordinatorOfNewServiceOp(this,
                        childService, o).setCompletion((notifyOp, notifyFailure) -> {
                            if (notifyFailure != null) {
                                o.fail(notifyFailure);
                                return;
                            }
                            startChildService(o, childService);
                        })) ;
                return;
            }
        }
        startChildService(o, childService);
    }

    private void startChildService(Operation o, Service childService) {
        o.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK);
        getHost().startService(o, childService);
    }

    private void forwardRequest(Operation o, Service childService) {
        Operation selectOp = Operation
                .createPost(null)
                .setExpiration(o.getExpirationMicrosUtc())
                .setCompletion(
                        (so, se) -> {
                            if (se != null) {
                                o.fail(se);
                                return;
                            }

                            if (!so.hasBody()) {
                                throw new IllegalStateException();
                            }
                            SelectOwnerResponse rsp = so.getBody(SelectOwnerResponse.class);
                            ServiceDocument initialState = (ServiceDocument) o.getBodyRaw();
                            initialState.documentOwner = rsp.ownerNodeId;
                            if (initialState.documentEpoch == null) {
                                initialState.documentEpoch = 0L;
                            }

                            if (rsp.isLocalHostOwner) {
                                completePostRequest(o, childService);
                                return;
                            }

                            URI remotePeerService = SelectOwnerResponse.buildUriToOwner(rsp,
                                    getSelfLink(), null);

                            CompletionHandler fc = (fo, fe) -> {
                                o.setBodyNoCloning(fo.getBodyRaw());
                                o.setStatusCode(fo.getStatusCode());
                                o.transferResponseHeadersFrom(fo);
                                if (fe != null) {
                                    o.fail(fe);
                                    return;
                                }
                                o.complete();
                            };

                            Operation forwardOp = o.clone().setUri(remotePeerService)
                                    .setCompletion(fc);

                            forwardOp.setConnectionTag(ServiceClient.CONNECTION_TAG_FORWARDING);
                            forwardOp.toggleOption(NodeSelectorService.FORWARDING_OPERATION_OPTION,
                                    true);

                            // fix up selfLink so it does not have factory prefix
                        initialState.documentSelfLink = initialState.documentSelfLink
                                .replace(getSelfLink(), "");

                        getHost().sendRequest(forwardOp);
                    });
        getHost().selectOwner(getPeerNodeSelectorPath(),
                o.getUri().getPath(), selectOp);
    }

    @Override
    public void handleGet(Operation get) {
        // work is done in the completion
        get.complete();
    }

    private void handleGetCompletion(Operation op) {
        String query = op.getUri().getQuery();
        String oDataFilter = UriUtils.getODataFilterParamValue(op.getUri());
        boolean expand = UriUtils.hasODataExpandParamValue(op.getUri());
        if (query == null || (oDataFilter == null && expand)) {
            completeGetWithQuery(op, this.childOptions);
        } else {
            handleGetOdataCompletion(op);
        }
    }

    private void handleGetOdataCompletion(Operation op) {
        String path = UriUtils.getPathParamValue(op.getUri());
        String peer = UriUtils.getPeerParamValue(op.getUri());
        if (path != null && peer != null) {
            handleNavigationRequest(op);
            return;
        }

        Set<String> expandedQueryPropertyNames = QueryTaskUtils
                .getExpandedQueryPropertyNames(getChildTemplate().documentDescription);

        QueryTask task = ODataUtils.toQuery(op, false, expandedQueryPropertyNames);
        if (task == null) {
            return;
        }
        task.setDirect(true);

        String kind = Utils.buildKind(getStateType());
        QueryTask.Query kindClause = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(kind);
        task.querySpec.query.addBooleanClause(kindClause);

        if (task.querySpec.sortTerm != null) {
            String propertyName = task.querySpec.sortTerm.propertyName;
            PropertyDescription propertyDescription = getChildTemplate()
                    .documentDescription.propertyDescriptions.get(propertyName);
            if (propertyDescription == null) {
                op.fail(new IllegalArgumentException("Sort term is not a valid property: "
                        + propertyName));
                return;
            }
            task.querySpec.sortTerm.propertyType = propertyDescription.typeName;
        }

        if (task.querySpec.resultLimit != null) {
            handleODataLimitRequest(op, task);
            return;
        }

        sendRequest(Operation.createPost(this, ServiceUriPaths.CORE_QUERY_TASKS).setBody(task)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    ServiceDocumentQueryResult result = o.getBody(QueryTask.class).results;
                    ODataFactoryQueryResult odataResult = new ODataFactoryQueryResult();
                    odataResult.totalCount = result.documentCount;
                    result.copyTo(odataResult);
                    op.setBodyNoCloning(odataResult).complete();
                }));
    }

    private void handleNavigationRequest(Operation op) {
        String path = UriUtils.buildUriPath(ServiceUriPaths.DEFAULT_NODE_SELECTOR,
                ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING);
        String query = UriUtils.buildUriQuery(
                UriUtils.FORWARDING_URI_PARAM_NAME_PATH, UriUtils.getPathParamValue(op.getUri()),
                UriUtils.FORWARDING_URI_PARAM_NAME_PEER, UriUtils.getPeerParamValue(op.getUri()),
                UriUtils.FORWARDING_URI_PARAM_NAME_TARGET, ForwardingTarget.PEER_ID.toString());
        sendRequest(Operation.createGet(UriUtils.buildUri(this.getHost(), path, query)).setCompletion((o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }

            ServiceDocumentQueryResult result = o.getBody(QueryTask.class).results;
            prepareNavigationResult(result);
            op.setBodyNoCloning(result).complete();
        }));
    }

    private void handleODataLimitRequest(Operation op, QueryTask task) {
        if (task.querySpec.options.contains(QueryOption.COUNT)) {
            task.querySpec.options.remove(QueryOption.COUNT);
            task.querySpec.options.add(QueryOption.EXPAND_CONTENT);

            Operation query = Operation
                    .createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                    .setBody(task);
            prepareRequest(query);

            QueryTask countTask = new QueryTask();
            countTask.setDirect(true);
            countTask.querySpec = new QueryTask.QuerySpecification();
            countTask.querySpec.options.add(QueryOption.COUNT);
            countTask.querySpec.query = task.querySpec.query;

            Operation count = Operation
                    .createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                    .setBody(countTask);
            prepareRequest(count);

            OperationJoin.create(count, query).setCompletion((os, es) -> {
                if (es != null && !es.isEmpty()) {
                    op.fail(es.values().iterator().next());
                    return;
                }

                ServiceDocumentQueryResult countResult = os.get(count.getId()).getBody(QueryTask.class).results;
                ServiceDocumentQueryResult queryResult = os.get(query.getId()).getBody(QueryTask.class).results;

                if (queryResult.nextPageLink == null) {
                    ODataFactoryQueryResult odataResult = new ODataFactoryQueryResult();
                    queryResult.copyTo(odataResult);
                    odataResult.totalCount = countResult.documentCount;
                    op.setBodyNoCloning(odataResult).complete();
                    return;
                }

                sendNextRequest(op, queryResult.nextPageLink, countResult.documentCount);
            }).sendWith(this.getHost());
            return;
        }

        sendRequest(Operation.createPost(this, ServiceUriPaths.CORE_QUERY_TASKS).setBody(task)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }

                    ServiceDocumentQueryResult result = o.getBody(QueryTask.class).results;
                    if (result.nextPageLink == null) {
                        ODataFactoryQueryResult odataResult = new ODataFactoryQueryResult();
                        result.copyTo(odataResult);
                        odataResult.totalCount = result.documentCount;
                        op.setBodyNoCloning(odataResult).complete();
                        return;
                    }

                    sendNextRequest(op, result.nextPageLink, null);
                }));
    }

    private void sendNextRequest(Operation op, String nextPageLink, Long totalCount) {
        sendRequest(Operation.createGet(this, nextPageLink).setCompletion((o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }

            ODataFactoryQueryResult odataResult = new ODataFactoryQueryResult();
            ServiceDocumentQueryResult result = o.getBody(QueryTask.class).results;
            result.copyTo(odataResult);
            odataResult.totalCount = totalCount == null ? result.documentCount : totalCount;
            prepareNavigationResult(odataResult);
            op.setBodyNoCloning(odataResult).complete();
        }));
    }

    private void prepareNavigationResult(ServiceDocumentQueryResult result) {
        if (result.nextPageLink != null) {
            result.nextPageLink = convertNavigationLink(result.nextPageLink);
        }
        if (result.prevPageLink != null) {
            result.prevPageLink = convertNavigationLink(result.prevPageLink);
        }
    }

    private String convertNavigationLink(String navigationLink) {
        URI uri = URI.create(navigationLink);
        return this.getSelfLink() + UriUtils.URI_QUERY_CHAR + UriUtils.buildUriQuery(
                UriUtils.FORWARDING_URI_PARAM_NAME_PATH, UriUtils.getPathParamValue(uri),
                UriUtils.FORWARDING_URI_PARAM_NAME_PEER, UriUtils.getPeerParamValue(uri));
    }

    public void completeGetWithQuery(Operation op, EnumSet<ServiceOption> caps) {
        boolean doExpand = false;
        if (op.getUri().getQuery() != null) {
            doExpand = UriUtils.hasODataExpandParamValue(op.getUri());
        }

        URI u = UriUtils.buildDocumentQueryUri(getHost(),
                UriUtils.buildUriPath(getSelfLink(), UriUtils.URI_WILDCARD_CHAR),
                doExpand,
                false,
                caps != null ? caps : EnumSet.of(ServiceOption.NONE));

        Operation query = Operation.createGet(u)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    op.setBodyNoCloning(o.getBodyRaw()).complete();
                });

        sendRequest(query);
    }

    public void handleOptions(Operation op) {
        op.setBody(null).complete();
    }

    public void handlePost(Operation op) {
        if (op.hasBody()) {
            ServiceDocument body = op.getBody(this.stateType);
            if (body == null) {
                op.fail(new IllegalArgumentException("structured body is required"));
                return;
            }
            if (body.documentSourceLink != null) {
                op.fail(new IllegalArgumentException("clone request not supported"));
                return;
            }
        }

        // the real work is done in the completion
        op.complete();
    }

    private Service createChildService() throws Throwable {
        Service childService;
        childService = createServiceInstance();
        this.childOptions = childService.getOptions();

        // apply replication settings from the child to the factory
        if (childService.hasOption(ServiceOption.REPLICATION)) {
            toggleOption(ServiceOption.REPLICATION, true);
            if (!ServiceUriPaths.DEFAULT_NODE_SELECTOR.equals(childService
                    .getPeerNodeSelectorPath())) {
                this.nodeSelectorLink = childService.getPeerNodeSelectorPath();
            } else if (!ServiceUriPaths.DEFAULT_NODE_SELECTOR.equals(this.nodeSelectorLink)) {
                childService.setPeerNodeSelectorPath(this.nodeSelectorLink);
            }
        }

        // apply on demand load to factory so service host can decide to start a service
        // if it receives a request and the service is not started
        if (childService.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
            toggleOption(ServiceOption.ON_DEMAND_LOAD, true);
        }

        // apply custom UI option to factory if child service has it to ensure ui consistency
        if (childService.hasOption(ServiceOption.HTML_USER_INTERFACE)) {
            toggleOption(ServiceOption.HTML_USER_INTERFACE, true);
        }

        if (childService.hasOption(ServiceOption.INSTRUMENTATION)) {
            toggleOption(ServiceOption.INSTRUMENTATION, true);
        }

        if (this.hasOption(ServiceOption.IDEMPOTENT_POST)) {
            childService.toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        }

        // set capability on child to indicate its created by a factory
        childService.toggleOption(ServiceOption.FACTORY_ITEM, true);

        return childService;
    }

    private Service createChildServiceSafe() {
        try {
            return createChildService();
        } catch (Throwable e) {
            logSevere(e);
            return null;
        }
    }

    @Override
    public void toggleOption(ServiceOption option, boolean enable) {
        if (enable) {
            this.options.add(option);
        } else {
            this.options.remove(option);
        }
    }

    @Override
    public String getPeerNodeSelectorPath() {
        return this.nodeSelectorLink;
    }

    @Override
    public void setPeerNodeSelectorPath(String link) {
        this.nodeSelectorLink = link;
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocumentQueryResult r = new ServiceDocumentQueryResult();
        ServiceDocument childTemplate = getChildTemplate();
        r.documents = new HashMap<>();
        childTemplate.documentSelfLink = UriUtils.buildUriPath(getSelfLink(), "child-template");
        r.documentLinks.add(childTemplate.documentSelfLink);
        r.documents.put(childTemplate.documentSelfLink, childTemplate);
        return r;
    }

    private ServiceDocument getChildTemplate() {
        if (this.childTemplate == null) {
            try {
                Service s = createServiceInstance();
                s.setHost(getHost());
                s.toggleOption(ServiceOption.FACTORY_ITEM, true);
                this.childTemplate = s.getDocumentTemplate();
            } catch (Throwable e) {
                logSevere(e);
                return null;
            }
        }
        return this.childTemplate;
    }

    @Override
    public void handleNodeGroupMaintenance(Operation maintOp) {
        if (hasOption(ServiceOption.ON_DEMAND_LOAD)) {
            // on demand load child services are synchronized on first use, or when an explicit
            // migration task runs
            setAvailable(true);
            maintOp.complete();
            return;
        }

        // Become unavailable until synchronization is complete.
        // If we are not the owner, we stay unavailable
        setAvailable(false);
        OperationContext opContext = OperationContext.getOperationContext();
        // Only one node is responsible for synchronizing the child services of a given factory.
        // Ask the runtime if this is the owner node, using the factory self link as the key.
        Operation selectOwnerOp = maintOp.clone().setExpiration(Utils.getNowMicrosUtc()
                + getHost().getOperationTimeoutMicros());
        selectOwnerOp.setCompletion((o, e) -> {
            OperationContext.restoreOperationContext(opContext);
            if (e != null) {
                logWarning("owner selection failed: %s", e.toString());
                maintOp.fail(e);
                return;
            }
            SelectOwnerResponse rsp = o.getBody(SelectOwnerResponse.class);
            if (!rsp.isLocalHostOwner) {
                // We do not need to do anything
                maintOp.complete();
                return;
            }

            if (rsp.availableNodeCount > 1) {
                verifyFactoryOwnership(maintOp, rsp);
                return;
            }

            synchronizeChildServicesAsOwner(maintOp, rsp.membershipUpdateTimeMicros);
        });

        getHost().selectOwner(this.nodeSelectorLink, this.getSelfLink(), selectOwnerOp);
    }

    private void synchronizeChildServicesAsOwner(Operation maintOp, long membershipUpdateTimeMicros) {
        maintOp.nestCompletion((o, e) -> {
            if (e != null) {
                logWarning("synch failed: %s", e.toString());
            }
            maintOp.complete();
        });
        startFactorySynchronizationTask(maintOp, membershipUpdateTimeMicros);
    }

    private void startFactorySynchronizationTask(Operation parentOp, Long membershipUpdateTimeMicros) {
        if (this.childOptions.contains(ServiceOption.ON_DEMAND_LOAD)) {
            setAvailable(true);
            parentOp.complete();
            return;
        }

        SynchronizationTaskService.State task = createSynchronizationTaskState(
                membershipUpdateTimeMicros);
        Operation post = Operation
                .createPost(this, ServiceUriPaths.SYNCHRONIZATION_TASKS)
                .setBody(task)
                .setCompletion((o, e) -> {
                    // Ignore if the request failed because the current synch-request
                    // was considered out-dated by the synchronization-task.
                    if (o.getStatusCode() == Operation.STATUS_CODE_BAD_REQUEST) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        logInfo("Failure on POST to synch task: %s", Utils.toJsonHtml(rsp));
                        if (rsp.getErrorCode() == ServiceErrorResponse.ERROR_CODE_OUTDATED_SYNCH_REQUEST) {
                            parentOp.complete();
                            return;
                        }
                    }
                    if (e != null) {
                        logInfo("Failure on POST to synch task: %s", e.getMessage());
                        parentOp.fail(e);
                        return;
                    }

                    parentOp.complete();
                });
        sendRequest(post);
    }

    private SynchronizationTaskService.State createSynchronizationTaskState(
            Long membershipUpdateTimeMicros) {
        SynchronizationTaskService.State task = new SynchronizationTaskService.State();
        task.documentSelfLink = UriUtils.convertPathCharsFromLink(this.getSelfLink());
        task.factorySelfLink = this.getSelfLink();
        task.factoryStateKind = Utils.buildKind(this.getStateType());
        task.membershipUpdateTimeMicros = membershipUpdateTimeMicros;
        task.nodeSelectorLink = this.nodeSelectorLink;
        task.queryResultLimit = SELF_QUERY_RESULT_LIMIT;
        task.taskInfo = TaskState.create();
        task.taskInfo.isDirect = true;
        return task;
    }

    private void verifyFactoryOwnership(Operation maintOp, SelectOwnerResponse ownerResponse) {
        // Local node thinks it's the owner. Let's confirm that
        // majority of the nodes in the node-group
        NodeSelectorService.SelectAndForwardRequest request =
                new NodeSelectorService.SelectAndForwardRequest();
        request.key = this.getSelfLink();

        Operation broadcastSelectOp = Operation
                .createPost(UriUtils.buildUri(this.getHost(), this.nodeSelectorLink))
                .setReferer(this.getHost().getUri())
                .setBody(request)
                .setCompletion((op, t) -> {
                    if (t != null) {
                        logWarning("owner selection failed: %s", t.toString());
                        maintOp.fail(t);
                        return;
                    }

                    NodeGroupBroadcastResponse response = op.getBody(NodeGroupBroadcastResponse.class);
                    for (Map.Entry<URI, String> r : response.jsonResponses.entrySet()) {
                        NodeSelectorService.SelectOwnerResponse rsp = null;
                        try {
                            rsp = Utils.fromJson(r.getValue(), NodeSelectorService.SelectOwnerResponse.class);
                        } catch (Throwable e) {
                            logWarning("Exception thrown in de-serializing json response. %s", e.toString());

                            // Ignore if the remote node returned a bad response. Most likely this is because
                            // the remote node is offline and if so, ownership check for the remote node is
                            // irrelevant.
                            continue;
                        }
                        if (!rsp.ownerNodeId.equals(this.getHost().getId())) {
                            logWarning("SelectOwner response from %s does not indicate that " +
                                            "local node %s is the owner for factory %s. JsonResponse: %s",
                                    r.getKey().toString(), this.getHost().getId(), this.getSelfLink(), r.getValue());
                            maintOp.complete();
                            return;
                        }
                    }

                    logInfo("%s elected as owner for factory %s. Starting synch ...",
                            getHost().getId(), this.getSelfLink());
                    synchronizeChildServicesAsOwner(maintOp, ownerResponse.membershipUpdateTimeMicros);
                });

        getHost().broadcastRequest(this.nodeSelectorLink, this.getSelfLink(), true, broadcastSelectOp);
    }

    public abstract Service createServiceInstance() throws Throwable;
}
