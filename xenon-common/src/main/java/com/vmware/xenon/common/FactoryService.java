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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceHost.ServiceAlreadyStartedException;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Implements a POST handler for creating new service instances. Derived implementations should
 * override handlePost, validate the initial state for the new service and complete the request.
 * This class will then create the new instance, store the new state (if service is durable) and
 * respond to the sender
 */
public abstract class FactoryService extends StatelessService {

    public static final int SELF_QUERY_RESULT_LIMIT = 1000;
    private static final long SELF_QUERY_TIMEOUT_MINUTES = 60;
    private EnumSet<ServiceOption> childOptions;
    private String nodeSelectorLink = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
    private int selfQueryResultLimit = SELF_QUERY_RESULT_LIMIT;

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
            startPost.complete();
            return;
        }

        // complete factory start POST immediately. Asynchronously query the index and start
        // child services. Requests to a child not yet loaded will be queued by the framework.
        Operation clonedOp = startPost.clone();
        startPost.complete();

        clonedOp.setCompletion((o, e) -> {
            if (e != null) {
                logWarning("Failure querying index for all child services: %s", e.getMessage());
                return;
            }
            logFine("Finished self query for child services");
        });

        if (this.childOptions.contains(ServiceOption.ON_DEMAND_LOAD)) {
            return;
        }
        startOrSynchronizeChildServices(clonedOp);
    }

    private void startOrSynchronizeChildServices(Operation op) {
        QueryTask queryTask = buildChildQueryTask();
        queryForChildren(queryTask,
                UriUtils.buildUri(this.getHost(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS),
                op);
    }

    protected void queryForChildren(QueryTask queryTask, URI queryFactoryUri,
            Operation parentOperation) {
        // check with the document store if any documents exist for services
        // under our URI name space. If they do, we need to re-instantiate these
        // services by issuing self posts
        Operation queryPost = Operation
                .createPost(queryFactoryUri)
                .setBody(queryTask)
                .setCompletion((o, e) -> {
                    if (getHost().isStopping()) {
                        parentOperation.fail(new CancellationException());
                        return;
                    }

                    if (e != null) {
                        if (!getHost().isStopping()) {
                            logSevere(e);
                        } else {
                            parentOperation.fail(e);
                            return;
                        }
                        // still complete start
                        parentOperation.complete();
                        return;
                    }

                    ServiceDocumentQueryResult rsp = o.getBody(QueryTask.class).results;

                    if (rsp.nextPageLink == null) {
                        parentOperation.complete();
                        return;
                    }
                    processChildQueryPage(UriUtils.buildUri(queryFactoryUri, rsp.nextPageLink),
                            queryTask, parentOperation);
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

        // The self query task might take a long time if we are loading millions of services. The
        // the user can either rely on ServiceOption.ON_DEMAND_LOAD, or use a custom
        // and longer operation timeout, during host start.
        long timeoutMicros = TimeUnit.MINUTES.toMicros(SELF_QUERY_TIMEOUT_MINUTES);
        timeoutMicros = Math.max(timeoutMicros, getHost().getOperationTimeoutMicros());
        queryTask.documentExpirationTimeMicros = Utils.getNowMicrosUtc() + timeoutMicros;

        // process child services in limited numbers, set query result limit
        queryTask.querySpec.resultLimit = this.selfQueryResultLimit;
        return queryTask;
    }

    /**
     * Retrieves a page worth of results for child service links and restarts them
     */
    private void processChildQueryPage(URI queryPage, QueryTask queryTask, Operation parentOp) {
        if (queryPage == null) {
            parentOp.complete();
            return;
        }

        if (getHost().isStopping()) {
            parentOp.fail(new CancellationException());
            return;
        }

        sendRequest(Operation.createGet(queryPage).setCompletion(
                (o, e) -> {
                    if (e != null) {
                        logWarning("Failure retrieving query results from %s: %s", queryPage,
                                e.toString());
                        parentOp.complete();
                        return;
                    }

                    ServiceDocumentQueryResult rsp = o.getBody(QueryTask.class).results;
                    if (rsp.documentCount == 0 || rsp.documentLinks.isEmpty()) {
                        parentOp.complete();
                        return;
                    }
                    synchronizeChildrenInQueryPage(queryPage, queryTask, parentOp,
                            rsp);
                }));
    }

    private void synchronizeChildrenInQueryPage(URI queryPage,
            QueryTask queryTask, Operation parentOp,
            ServiceDocumentQueryResult rsp) {

        AtomicInteger pendingStarts = new AtomicInteger(rsp.documentLinks.size());
        // track child service request in parallel, passing a single parent operation
        CompletionHandler c = (so, se) -> {
            int r = pendingStarts.decrementAndGet();
            if (se != null && getHost().isStopping()) {
                logWarning("Restart for children failed: %s", se.getMessage());
            }

            if (getHost().isStopping()) {
                parentOp.fail(new CancellationException());
                return;
            }

            if (r != 0) {
                return;
            }

            URI nextQueryPage = rsp.nextPageLink == null ? null : UriUtils.buildUri(
                    queryPage, rsp.nextPageLink);

            processChildQueryPage(nextQueryPage, queryTask, parentOp);
        };

        for (String link : rsp.documentLinks) {
            if (getHost().isStopping()) {
                parentOp.fail(new CancellationException());
                return;
            }
            Operation post = Operation.createPost(this, link)
                    .setCompletion(c)
                    .setReferer(getUri());
            startOrSynchChildService(link, post);
        }
    }

    private void startOrSynchChildService(String link, Operation post) {
        try {
            Service child = createChildService();
            post.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK);
            getHost().startOrSynchService(post, child);
        } catch (Throwable e1) {
            post.fail(e1);
        }
        return;
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
            op.nestCompletion(o -> handleGetCompletion(o));
            handleGet(op);
        } else if (op.getAction() == Action.DELETE) {
            op.nestCompletion(o -> handleDeleteCompletion(o));
            handleDelete(op);
        } else if (op.getAction() == Action.OPTIONS) {
            op.nestCompletion(o -> handleOptionsCompletion(o));
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
            String suffix = null;
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

        if (this.childOptions.contains(ServiceOption.OWNER_SELECTION) && !o.isFromReplication()
                && !o.isForwardingDisabled()) {
            forwardRequest(o, childService);
            return;
        }

        completePostRequest(o, childService);
    }

    private void completePostRequest(Operation o, Service childService) {
        if (getHost().getServiceStage(o.getUri().getPath()) != null) {
            handleServiceExistsPostCompletion(o);
            return;
        }

        if (!o.isFromReplication() && !o.isReplicationDisabled()) {
            o.nestCompletion(startOp -> {
                publish(o);
                if (!hasOption(ServiceOption.REPLICATION)) {
                    o.complete();
                    return;
                }
                o.setReplicationDisabled(false);
                replicateRequest(o);
            });
        }

        o.setReplicationDisabled(true);
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

                            // fix up selfLink so it does not have factory prefix
                            initialState.documentSelfLink = initialState.documentSelfLink
                                    .replace(getSelfLink(), "");

                            getHost().sendRequest(forwardOp);
                        });
        getHost().selectOwner(getPeerNodeSelectorPath(),
                o.getUri().getPath(), selectOp);
    }

    public void handleServiceExistsPostCompletion(Operation o) {
        if (!hasOption(ServiceOption.IDEMPOTENT_POST)) {
            o.setStatusCode(Operation.STATUS_CODE_CONFLICT)
                    .fail(new ServiceAlreadyStartedException(o.getUri().toString()));
            return;
        }

        logInfo("Converting POST to PUT, service already exists: %s", o.getUri());
        // implement UPSERT semantics: If a service exists, convert the POST to a PUT and issue
        // it to the service
        Operation put = o.clone().setAction(Action.PUT).setCompletion((op, ex) -> {
            if (ex != null) {
                o.fail(ex);
            } else {
                o.transferResponseHeadersFrom(op)
                        .setBodyNoCloning(op.getBodyRaw())
                        .setStatusCode(op.getStatusCode()).complete();
            }
        });
        sendRequest(put);
        return;
    }

    @Override
    public void handleGet(Operation get) {
        // work is done in the completion
        get.complete();
    }

    private void handleGetCompletion(Operation op) {
        String oDataFilter = UriUtils.getODataFilterParamValue(op.getUri());
        if (oDataFilter != null) {
            handleGetOdataCompletion(op, oDataFilter);
        } else {
            completeGetWithQuery(this, op, this.childOptions);
        }
    }

    private void handleGetOdataCompletion(Operation op, String oDataFilter) {
        QueryTask task = new QueryTask().setDirect(true);
        task.querySpec = new QueryTask.QuerySpecification();
        if (op.getUri().getQuery().contains(UriUtils.URI_PARAM_ODATA_EXPAND)) {
            task.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT);
        }

        String kind = Utils.buildKind(getStateType());
        QueryTask.Query kindClause = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(kind);
        task.querySpec.query.addBooleanClause(kindClause);

        Query oDataFilterClause = new ODataQueryVisitor().toQuery(oDataFilter);
        task.querySpec.query.addBooleanClause(oDataFilterClause);

        sendRequest(Operation.createPost(this, ServiceUriPaths.CORE_QUERY_TASKS).setBody(task)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    QueryTask qrt = o.getBody(QueryTask.class);
                    op.setBodyNoCloning(qrt.results).complete();
                }));
    }

    public static void completeGetWithQuery(Service s, Operation op,
            EnumSet<ServiceOption> caps) {
        boolean doExpand = false;
        if (op.getUri().getQuery() != null) {
            doExpand = op.getUri().getQuery().contains(UriUtils.URI_PARAM_ODATA_EXPAND);
        }

        URI u = UriUtils.buildDocumentQueryUri(s.getHost(),
                UriUtils.buildUriPath(s.getSelfLink(), UriUtils.URI_WILDCARD_CHAR),
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

        s.sendRequest(query);
    }

    public void handleOptions(Operation op) {
        op.setBody(null).complete();
    }

    public void handlePost(Operation op) {
        if (op.hasBody()) {
            ServiceDocument body = op.getBody(ServiceDocument.class);
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
        if (false == enable) {
            this.options.remove(option);
        } else {
            this.options.add(option);
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

    private void replicateRequest(Operation op) {
        // set the URI to be of this service, the factory since we want
        // the POSt going to the remote peer factory service, not the
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
        getHost().replicateRequest(this.options, clonedInitState, getPeerNodeSelectorPath(),
                originalLink, op);
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        try {
            ServiceDocumentQueryResult r = new ServiceDocumentQueryResult();
            Service s = createServiceInstance();
            s.setHost(getHost());
            ServiceDocument childTemplate = s.getDocumentTemplate();
            r.documents = new HashMap<>();
            childTemplate.documentSelfLink = UriUtils.buildUriPath(getSelfLink(), "child-template");
            r.documentLinks.add(childTemplate.documentSelfLink);
            r.documents.put(childTemplate.documentSelfLink, childTemplate);
            return r;
        } catch (Throwable e) {
            logSevere(e);
            return null;
        }
    }

    @Override
    public void handleMaintenance(Operation maintOp) {
        ServiceMaintenanceRequest body = maintOp.getBody(ServiceMaintenanceRequest.class);
        if (!body.reasons.contains(MaintenanceReason.NODE_GROUP_CHANGE)) {
            maintOp.complete();
            return;
        }

        if (!hasOption(ServiceOption.REPLICATION)) {
            maintOp.complete();
            return;
        }
        startOrSynchronizeChildServices(maintOp);
    }

    public abstract Service createServiceInstance() throws Throwable;
}
