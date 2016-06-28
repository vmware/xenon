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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.UriUtils.ForwardingTarget;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
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

    static class SynchronizationContext {
        SelectOwnerResponse originalSelection;
        QueryTask queryTask;
        Operation maintOp;
        public URI nextPageReference;
    }

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

        if (!ServiceHost.isServiceIndexed(this)) {
            setAvailable(true);
            startPost.complete();
            return;
        }

        // complete factory start POST immediately. Asynchronously query the index and start
        // child services. Requests to a child not yet loaded will be queued by the framework.
        Operation clonedOp = startPost.clone();
        startPost.complete();

        clonedOp.setCompletion((o, e) -> {
            if (e != null && !getHost().isStopping()) {
                logWarning("Failure querying index for all child services: %s", e.getMessage());
                return;
            }
            setAvailable(true);
            logFine("Finished self query for child services");
        });

        if (!this.childOptions.contains(ServiceOption.REPLICATION)) {
            SynchronizationContext ctx = new SynchronizationContext();
            ctx.originalSelection = null;
            ctx.maintOp = clonedOp;
            startOrSynchronizeChildServices(ctx);
            return;
        }
        // when the node group becomes available, the maintenance handler will initiate
        // service start and synchronization
    }

    private void startOrSynchronizeChildServices(SynchronizationContext ctx) {
        if (this.childOptions.contains(ServiceOption.ON_DEMAND_LOAD)) {
            ctx.maintOp.complete();
            return;
        }

        QueryTask queryTask = buildChildQueryTask();
        ctx.queryTask = queryTask;
        queryForChildren(ctx);
    }

    protected void queryForChildren(SynchronizationContext ctx) {
        URI queryFactoryUri = UriUtils.buildUri(this.getHost(), ServiceUriPaths.CORE_QUERY_TASKS);
        // check with the document store if any documents exist for services
        // under our URI name space. If they do, we need to re-instantiate these
        // services by issuing self posts
        Operation queryPost = Operation
                .createPost(queryFactoryUri)
                .setBody(ctx.queryTask)
                .setCompletion((o, e) -> {
                    if (getHost().isStopping()) {
                        ctx.maintOp.fail(new CancellationException("host is stopping"));
                        return;
                    }

                    if (e != null) {
                        if (!getHost().isStopping()) {
                            logWarning("Query failed with %s", e.toString());
                        }
                        ctx.maintOp.fail(e);
                        return;
                    }

                    ServiceDocumentQueryResult rsp = o.getBody(QueryTask.class).results;

                    if (rsp.nextPageLink == null) {
                        ctx.maintOp.complete();
                        return;
                    }

                    ctx.nextPageReference = UriUtils.buildUri(queryFactoryUri, rsp.nextPageLink);
                    processChildQueryPage(ctx, true);
                });

        sendRequest(queryPost);
    }

    private QueryTask buildChildQueryTask() {
        /*
        Use QueryTask to compute all the documents that match
        1) documentSelfLink to <FactorySelfLink>/*
        2) documentKind to <stateType>
        */
        QueryTask queryTask = new QueryTask();
        queryTask.querySpec = new QueryTask.QuerySpecification();

        queryTask.taskInfo.isDirect = true;

        QueryTask.Query kindClause;
        QueryTask.Query uriPrefixClause;

        uriPrefixClause = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_SELF_LINK)
                .setTermMatchType(QueryTask.QueryTerm.MatchType.WILDCARD)
                .setTermMatchValue(
                        getSelfLink() + UriUtils.URI_PATH_CHAR + UriUtils.URI_WILDCARD_CHAR);
        queryTask.querySpec.query.addBooleanClause(uriPrefixClause);

        kindClause = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(this.getStateType()));

        queryTask.querySpec.query.addBooleanClause(kindClause);

        // set timeout based on peer synchronization upper limit
        long timeoutMicros = TimeUnit.SECONDS.toMicros(
                getHost().getPeerSynchronizationTimeLimitSeconds());
        timeoutMicros = Math.max(timeoutMicros, getHost().getOperationTimeoutMicros());
        queryTask.documentExpirationTimeMicros = Utils.getNowMicrosUtc() + timeoutMicros;

        // The factory instance on this host is owner, so its responsible for getting the child links
        // from all peers. Use the broadcast query task to achieve this, since it will join
        // results
        queryTask.querySpec.options = EnumSet.of(QueryOption.BROADCAST);

        // use the same selector as the one we are associated with so it goes to the
        // proper node group
        queryTask.nodeSelectorLink = getPeerNodeSelectorPath();

        // process child services in limited numbers, set query result limit
        queryTask.querySpec.resultLimit = this.selfQueryResultLimit;
        return queryTask;
    }

    /**
     * Retrieves a page worth of results for child service links and restarts them
     */
    private void processChildQueryPage(SynchronizationContext ctx, boolean verifyOwner) {
        if (ctx.nextPageReference == null) {
            ctx.maintOp.complete();
            return;
        }

        if (getHost().isStopping()) {
            ctx.maintOp.fail(new CancellationException());
            return;
        }

        if (verifyOwner && hasOption(ServiceOption.REPLICATION)) {
            verifySynchronizationOwner(ctx);
            return;
        }

        CompletionHandler c = (o, e) -> {
            if (e != null) {
                if (!getHost().isStopping()) {
                    logWarning("Failure retrieving query results from %s: %s",
                            ctx.nextPageReference,
                            e.toString());
                }
                ctx.maintOp.fail(new IllegalStateException(
                        "failure retrieving query page results"));
                return;
            }

            ServiceDocumentQueryResult rsp = o.getBody(QueryTask.class).results;
            if (rsp.documentCount == 0 || rsp.documentLinks.isEmpty()) {
                ctx.maintOp.complete();
                return;
            }
            synchronizeChildrenInQueryPage(ctx, rsp);
        };
        sendRequest(Operation.createGet(ctx.nextPageReference).setCompletion(c));
    }

    private void synchronizeChildrenInQueryPage(SynchronizationContext ctx,
            ServiceDocumentQueryResult rsp) {
        if (getProcessingStage() == ProcessingStage.STOPPED) {
            ctx.maintOp.fail(new CancellationException());
            return;
        }
        ServiceMaintenanceRequest smr = null;
        if (ctx.maintOp.hasBody()) {
            smr = ctx.maintOp.getBody(ServiceMaintenanceRequest.class);
        }
        AtomicInteger pendingStarts = new AtomicInteger(rsp.documentLinks.size());
        // track child service request in parallel, passing a single parent operation
        CompletionHandler c = (so, se) -> {
            int r = pendingStarts.decrementAndGet();
            if (se != null && !getHost().isStopping()) {
                logWarning("Restart for children failed: %s", se.getMessage());
            }

            if (getHost().isStopping()) {
                ctx.maintOp.fail(new CancellationException());
                return;
            }

            if (r != 0) {
                return;
            }

            ctx.nextPageReference = rsp.nextPageLink == null ? null : UriUtils.buildUri(
                    ctx.nextPageReference, rsp.nextPageLink);
            processChildQueryPage(ctx, true);
        };

        for (String link : rsp.documentLinks) {
            if (getHost().isStopping()) {
                ctx.maintOp.fail(new CancellationException());
                return;
            }
            Operation post = Operation.createPost(this, link)
                    .setCompletion(c)
                    .setReferer(getUri());
            startOrSynchChildService(link, post, smr);
        }
    }

    private void startOrSynchChildService(String link, Operation post, ServiceMaintenanceRequest smr) {
        try {
            Service child = createChildService();
            NodeGroupState ngs = smr != null ? smr.nodeGroupState : null;
            getHost().startOrSynchService(post, child, ngs);
        } catch (Throwable e1) {
            logSevere(e1);
            post.fail(e1);
        }
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
        if (!o.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH)) {
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
                // create a random URI that is prefixed by the URI of this service
                suffix = UUID.randomUUID().toString();
                initialState = new ServiceDocument();
            } else {
                if (initialState.documentSelfLink == null) {
                    suffix = UUID.randomUUID().toString();
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
            o.setUri(serviceUri);

        } catch (Throwable e) {
            logSevere(e);
            o.fail(e);
            return;
        }

        initialState.documentSelfLink = o.getUri().getPath();
        initialState.documentKind = Utils.buildKind(this.stateType);
        initialState.documentTransactionId = o.getTransactionId();
        o.setBody(initialState);

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

    private void completePostRequest(Operation o, Service childService) {
        if (!o.isFromReplication() && !o.isReplicationDisabled()) {
            o.nestCompletion(startOp -> {
                publish(o);
                if (o.getAction() == Action.PUT || !hasOption(ServiceOption.REPLICATION)) {
                    // if the operation was converted to PUT, due to IDEMPOTENT_POST or
                    // the service does not support replication, complete and return
                    o.complete();
                    return;
                }
                o.setReplicationDisabled(false);
                replicateRequest(o);
            });

            if (o.isWithinTransaction() && this.getHost().getTransactionServiceUri() != null) {
                TransactionServiceHelper.notifyTransactionCoordinatorOfNewService(this,
                        childService, o);
            }
        }

        o.setReplicationDisabled(true);
        o.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK);
        getHost().startService(o, childService);
    }

    private void replicateRequest(Operation op) {
        // set the URI to be of this service, the factory since we want
        // the POST going to the remote peer factory service, not the
        // yet-to-be-created child service
        op.setUri(getUri());

        ServiceDocument initialState = op.getBody(this.stateType);
        final ServiceDocument clonedInitState = Utils.clone(initialState);

        // The factory services on the remote nodes must see the request body as it was before it
        // was fixed up by this instance. Restore self link to be just the child suffix "hint", removing the
        // factory prefix added upstream.
        String originalLink = clonedInitState.documentSelfLink;
        clonedInitState.documentSelfLink = clonedInitState.documentSelfLink.replace(getSelfLink(),
                "");
        op.nestCompletion((replicatedOp) -> {
            clonedInitState.documentSelfLink = originalLink;
            op.linkState(null).setBodyNoCloning(clonedInitState).complete();
        });

        // if limited replication is used for this service, supply a selection key, the fully qualified service link
        // so the same set of nodes get selected for the POST to create the service, as the nodes chosen
        // for subsequence updates to the child service
        op.linkState(clonedInitState);
        getHost().replicateRequest(this.childOptions, clonedInitState, getPeerNodeSelectorPath(),
                originalLink, op);
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
        if (!hasOption(ServiceOption.REPLICATION)) {
            maintOp.complete();
            return;
        }

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
            if (rsp.isLocalHostOwner == false) {
                // We do not need to do anything
                maintOp.complete();
                return;
            }

            if (rsp.availableNodeCount > 1) {
                logInfo("Elected owner on %s, starting synch (%d)", getHost().getId(),
                        rsp.availableNodeCount);
            }

            SynchronizationContext ctx = new SynchronizationContext();
            ctx.originalSelection = rsp;
            ctx.maintOp = maintOp;
            synchronizeChildServicesAsOwner(ctx);
        });

        getHost().selectOwner(this.nodeSelectorLink, this.getSelfLink(), selectOwnerOp);
    }

    /**
     * Invoked per query result page to check if should continue with synchronization.
     * Node group might have changed and we might no longer be the owner.
     *
     * This check is mostly for optimization: By interrupting synchronization
     * when a new owner is selected for this factory self link we avoid collision of PUT/POSTs
     * on the child service, from two different factories synchronizing it. In theory, its benign, since
     * the child should reject anything with the wrong epoch.
     *
     * The correctness issue is that the stateless factory is not using the default consensus approach
     * of propose, learn, decide before it kicks of synchronization. It relies on the node group selection
     * to produce the same owner, across all nodes, which happens only when the group is stable
     * This should be replaced with a "synchronization" task and a POST to a dedicated synchronization
     * factory in the future
     *
     * https://www.pivotaltracker.com/story/show/116784577
     *
     */
    private void verifySynchronizationOwner(SynchronizationContext ctx) {
        OperationContext opContext = OperationContext.getOperationContext();
        Operation selectOwnerOp = ctx.maintOp.clone().setExpiration(Utils.getNowMicrosUtc()
                + getHost().getOperationTimeoutMicros());
        selectOwnerOp
                .setCompletion((o, e) -> {
                    OperationContext.restoreOperationContext(opContext);
                    if (e != null) {
                        ctx.maintOp.fail(e);
                        return;
                    }

                    SelectOwnerResponse rsp = o.getBody(SelectOwnerResponse.class);
                    if (rsp.availableNodeCount != ctx.originalSelection.availableNodeCount
                            || rsp.membershipUpdateTimeMicros != ctx.originalSelection.membershipUpdateTimeMicros) {
                        logWarning("Membership changed, aborting synch");
                        ctx.maintOp.fail(new CancellationException(
                                "aborted due to node group change"));
                        return;
                    }

                    if (rsp.isLocalHostOwner == false) {
                        logWarning("No longer owner, aborting synch. New owner %s", rsp.ownerNodeId);
                        ctx.maintOp.fail(new CancellationException("aborted due to owner change"));
                        return;
                    }
                    processChildQueryPage(ctx, false);
                });

        getHost().selectOwner(this.nodeSelectorLink, this.getSelfLink(), selectOwnerOp);
    }

    private void synchronizeChildServicesAsOwner(SynchronizationContext ctx) {
        ctx.maintOp.nestCompletion((o, e) -> {
            if (e != null) {
                logWarning("synch failed: %s", e.toString());
            } else {
                // Update stat and /available so any service or client that cares, can notice this factory
                // is done with synchronization and child restart. The status of /available and
                // the available stat does not prevent the runtime from routing requests to this service
                setAvailable(true);
            }
            ctx.maintOp.complete();
        });
        startOrSynchronizeChildServices(ctx);
    }

    public abstract Service createServiceInstance() throws Throwable;
}
