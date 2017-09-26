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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;

import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.OperationJoin.JoinedCompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceConfiguration;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.SynchronizationTaskService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 *
 * This service provides one point API to get availability status of all factories.
 *
 * Availability of factory service is determined by these factors in this order.
 *    1.  Check node selector of factory is AVAILABLE? If not, then factory is declared as UNAVAILABLE,
 *        and we do not proceed to next step.
 *    2.  Check factory availability stats from the factory owner node after determining the owner.
 *    3.  Check factory synchronization task's status from the factory owner node for determining if it is SYNCHRONIZING.
 *
 * Node Selector of the factory is critical to find factory's availability status. Factory's config
 * provides the node selector link, which is then first queried to find its own status, and then used
 * to find the owner of the factory. If node-selector is not AVAILABLE then owner cannot be found
 * and hence we call factory to be UNAVAILABLE in that case.
 *
 */
public class SynchronizationManagementService extends StatelessService {
    public static final String SELF_LINK = ServiceUriPaths.CORE_SYNCHRONIZATION_MANAGEMENT;
    public static final EnumSet<ServiceOption> FACTORY_SERVICE_OPTION = EnumSet.of(ServiceOption.FACTORY);

    public static class SynchronizationManagementState {
        public enum Status {
            AVAILABLE,
            UNAVAILABLE,
            SYNCHRONIZING
        }
        /**
         * Availability status of the factory.
         */
        public Status status = Status.UNAVAILABLE;

        /**
         * Factory owner's Id, which will be null if factory owner could not be determined.
         */
        public String owner;
    }

    @Override
    public void handlePatch(Operation op) {
        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }

        SynchronizationRequest synchRequest = op.getBody(SynchronizationRequest.class);
        if (synchRequest.kind == null || !synchRequest.kind.equals(Utils.buildKind(SynchronizationRequest.class))) {
            op.fail(new IllegalArgumentException(String.format(
                    "Invalid 'kind' in the request body")));
            return;
        }

        if (synchRequest.documentSelfLink == null || synchRequest.documentSelfLink.isEmpty()) {
            op.fail(new IllegalArgumentException(String.format(
                    "%s cannot be null or empty", ServiceDocument.FIELD_NAME_SELF_LINK)));
            return;
        }

        URI uri = UriUtils.buildUri(this.getHost(), synchRequest.documentSelfLink);
        uri = UriUtils.buildConfigUri(uri);
        Operation.createGet(uri).setCompletion((o, e) -> {
            if (e != null) {
                op.setBody(o.getBodyRaw());
                op.fail(e);
                return;
            }

            String peerNodeSelectorPath = o.getBody(ServiceConfiguration.class).peerNodeSelectorPath;
            sendSynchronizationRequest(synchRequest.documentSelfLink, peerNodeSelectorPath, synchRequest, op);
        }).sendWith(this);
    }

    @Override
    public void handleGet(Operation get) {
        // Get the list of all factories and fan-out their status retrieval operations.
        Operation operation = Operation.createGet(null).setCompletion((o, e) -> {
            if (e != null) {
                get.setBody(o.getBodyRaw());
                get.complete();
                return;
            }

            String queryFactory = null;
            Map<String,String> queryParams = UriUtils.parseUriQueryParams(get.getUri());
            if (queryParams.size() > 0) {
                queryFactory = queryParams.get(ServiceDocument.FIELD_NAME_SELF_LINK);
            }

            List<Operation> configGets = new ArrayList<>();
            ServiceDocumentQueryResult result = new ServiceDocumentQueryResult();
            result.documents = new HashMap<>();

            // Create config GETs for all factories to send through OperationJoin.
            ServiceDocumentQueryResult factories = o.getBody(ServiceDocumentQueryResult.class);
            for (String factorySelfLink : factories.documentLinks) {
                if (queryFactory != null && !queryFactory.equals(factorySelfLink)) {
                    continue;
                }
                URI configUri = UriUtils.buildConfigUri(this.getHost(), factorySelfLink);

                // We create an entry for each factory here and that entry will only be updated by
                // relevant factory operation and hence we do not need to protect this list from concurrent accesses.
                result.documents.put(factorySelfLink, new SynchronizationManagementState());
                Operation configGet = Operation.createGet(configUri);
                configGets.add(configGet);
            }

            if (configGets.isEmpty()) {
                get.fail(new IllegalArgumentException(String.format(
                        "Factory %s cannot be found", queryFactory)));
                return;
            }

            JoinedCompletionHandler joinedCompletion = (os, fs) -> {
                List<DeferredResult<Object>> factoryStatuses = new ArrayList<>();

                // Fan-out status retrieval operations for each factory and collect deferred results later when completed.
                for (Operation op : os.values()) {
                    if (op.getErrorResponseBody() == null) {
                        String factorySelfLink = UriUtils.getParentPath(op.getUri().getPath());
                        String peerNodeSelectorPath = op.getBody(ServiceConfiguration.class).peerNodeSelectorPath;

                        // Factory config completion handler will perform all subsequent operations to get the
                        // status of this factory.
                        DeferredResult<Object> factoryStatus = getFactoryStatus(factorySelfLink, result, peerNodeSelectorPath);
                        factoryStatuses.add(factoryStatus);
                    }
                }

                // Collect all factory results, pack them and return them to the caller.
                DeferredResult.allOf(factoryStatuses)
                        .thenApply(packAndReturnResults(get, result));
            };

            OperationJoin.create(configGets)
                    .setCompletion(joinedCompletion)
                    .sendWith(this);
        });

        this.getHost().queryServiceUris(FACTORY_SERVICE_OPTION, true, operation, null);
    }

    private DeferredResult<Object> sendSynchronizationRequest(String factoryLink, String selectorPath, SynchronizationRequest synchRequest, Operation op) {
        DeferredResult<Object> factoryStatus = new DeferredResult<>();
        URI nodeSelectorUri = UriUtils.buildUri(getHost(), selectorPath);
        NodeSelectorService.SelectAndForwardRequest req = new NodeSelectorService.SelectAndForwardRequest();
        req.key = factoryLink;
        Operation selectorPost = Operation.createPost(nodeSelectorUri)
                .setReferer(getUri())
                .setBodyNoCloning(req);

        // URI will be filled after factory owner is determined from the Node Selector.
        Operation factorySynchPost  = Operation.createPatch(null)
                .setBody(synchRequest)
                .setReferer(getUri());

        // Chain of operations executed in sequence to start factory's synchronization.
        DeferredResult
                .allOf(getNodeSelectorAvailability(selectorPost, factoryStatus))
                .thenCompose(a -> getFactoryOwnerFromNodeSelector(selectorPost, factorySynchPost, factorySynchPost))
                .thenAccept(a -> sendSynchronizationRequest(factoryLink, factorySynchPost, op));

        return factoryStatus;
    }

    private void sendSynchronizationRequest(
            String factoryLink, Operation factorySynchPost, Operation op) {
        URI uri = UriUtils.buildUri(factorySynchPost.getUri(), factoryLink);
        uri = UriUtils.extendUri(uri, ServiceHost.SERVICE_URI_SUFFIX_SYNCHRONIZATION);
        factorySynchPost.setUri(uri);
        factorySynchPost.setCompletion((o, e) -> {
            if (e != null) {
                this.log(Level.WARNING, "Failed to start factory synchronization: %s", e);
                op.fail(e);
                return;
            }
            op.setBody(o.getBodyRaw());
            op.complete();
        }).sendWith(this);
    }

    private DeferredResult<Object> getFactoryStatus(String factoryLink, ServiceDocumentQueryResult result, String selectorPath) {
        DeferredResult<Object> factoryStatus = new DeferredResult<>();
        URI nodeSelectorUri = UriUtils.buildUri(getHost(), selectorPath);
        NodeSelectorService.SelectAndForwardRequest req = new NodeSelectorService.SelectAndForwardRequest();
        req.key = factoryLink;
        Operation selectorPost = Operation.createPost(nodeSelectorUri)
                .setReferer(getUri())
                .setBodyNoCloning(req);

        // These GET operation get filled with URIs after factory owner is determined from the Node Selector.
        Operation synchGets = Operation.createGet(null).setReferer(getUri());
        Operation factoryStatGets  = Operation.createGet(null).setReferer(getUri());

        // Chain of operations executed in sequence to fill in one factory's status.
        // Completion of factoryStatus in the future will notify the caller that factory's status is updated.
        // Caller will be waiting on the completion of all factoryStatuses.
        DeferredResult
                .allOf(getNodeSelectorAvailability(selectorPost, factoryStatus))
                .thenCompose(a -> getFactoryOwnerFromNodeSelector(selectorPost, synchGets, factoryStatGets))
                .thenCompose(a -> getFactoryAvailability(result, factoryStatGets))
                .thenCompose(a -> getSynchronizationTaskStatus(result, synchGets, factoryStatus));

        return factoryStatus;
    }

    private Function<? super List<Object>, Object> packAndReturnResults(Operation get, ServiceDocumentQueryResult result) {
        return a -> {
            result.documentOwner = this.getHost().getId();
            result.documentCount = (long) result.documents.size();
            result.documentLinks = new ArrayList<>(result.documents.keySet());
            Collections.sort(result.documentLinks);
            get.setBodyNoCloning(result);
            get.complete();
            return null;
        };
    }

    private DeferredResult<Object> getSynchronizationTaskStatus(
            ServiceDocumentQueryResult result, Operation synchTaskGet, DeferredResult<Object> factoryStatus) {
        DeferredResult<Object> synchronizationTask = new DeferredResult<>();
        synchTaskGet.setCompletion((o, e) -> {
            if (e != null) {
                this.log(Level.WARNING, "Failed to GET synchronization task status: %s", e);
                synchronizationTask.complete(null);

                // Complete factoryStatus because we will not further proceed with this factory's status retrieval
                // and result collector should be notified now.
                factoryStatus.complete(null);
                return;
            }

            SynchronizationTaskService.State s = o.getBody(SynchronizationTaskService.State.class);
            SynchronizationManagementState factoryState =
                    (SynchronizationManagementState) result.documents.get(s.factorySelfLink);
            if (s.taskInfo.stage == TaskState.TaskStage.STARTED &&
                    factoryState.status.equals(SynchronizationManagementState.Status.UNAVAILABLE)) {
                factoryState.status = SynchronizationManagementState.Status.SYNCHRONIZING;
            }
            synchronizationTask.complete(null);

            // Complete factoryStatus now as we have completed retrieval of all information for this factory
            // and result collector should be notified now.
            factoryStatus.complete(null);
        }).sendWith(this);
        return synchronizationTask;
    }

    private DeferredResult<Object> getFactoryAvailability(
            ServiceDocumentQueryResult result, Operation factoryStatGet) {
        DeferredResult<Object> factoryStats = new DeferredResult<>();
        factoryStatGet.setCompletion((o, e) -> {
            if (e != null) {
                this.log(Level.WARNING, "Failed to GET factory stats: %s", e);
                factoryStats.complete(null);
                return;
            }

            ServiceStats s = o.getBody(ServiceStats.class);
            SynchronizationManagementState factoryState =
                    (SynchronizationManagementState) result.documents.get(UriUtils.getParentPath(s.documentSelfLink));
            ServiceStats.ServiceStat availableStat = s.entries.get(Service.STAT_NAME_AVAILABLE);

            factoryState.owner = s.documentOwner;
            factoryState.status = SynchronizationManagementState.Status.UNAVAILABLE;

            if (availableStat != null && availableStat.latestValue == STAT_VALUE_TRUE) {
                factoryState.status = SynchronizationManagementState.Status.AVAILABLE;
            }
            factoryStats.complete(null);
        }).sendWith(this);
        return factoryStats;
    }

    private DeferredResult<Object> getFactoryOwnerFromNodeSelector(
            Operation selectPost, Operation synchGet, Operation factoryStatGet) {
        DeferredResult<Object> findSelector = new DeferredResult<>();
        selectPost.setCompletion((o, e) -> {
            if (e != null) {
                this.log(Level.WARNING, "Failed to GET node selector: %s", e);
                findSelector.complete(null);
                return;
            }

            NodeSelectorService.SelectOwnerResponse selectRsp = o.getBody(NodeSelectorService.SelectOwnerResponse.class);

            String synchTaskLink = UriUtils.buildUriPath(
                    SynchronizationTaskService.FACTORY_LINK,
                    UriUtils.convertPathCharsFromLink(selectRsp.key));

            URI synchTaskOwnerUri = UriUtils.buildUri(selectRsp.ownerNodeGroupReference, synchTaskLink);
            URI factoryStatOwnerUri = UriUtils.buildStatsUri(UriUtils.buildUri(selectRsp.ownerNodeGroupReference, selectRsp.key));

            synchGet.setUri(synchTaskOwnerUri);
            factoryStatGet.setUri(factoryStatOwnerUri);
            findSelector.complete(null);
        }).sendWith(this);
        return findSelector;
    }

    private DeferredResult<Object> getNodeSelectorAvailability(Operation selectPost, DeferredResult<Object> factoryStatus) {
        DeferredResult<Object> findSelector = new DeferredResult<>();
        Operation selectorGet = Operation.createGet(selectPost.getUri())
                .setReferer(getUri());
        selectorGet.setCompletion((o, e) -> {
            if (e != null) {
                String message = "Failed to GET node selector: " + e;
                this.log(Level.WARNING, message);
                findSelector.fail(new Throwable(message));

                // Complete factoryStatus because we will not further proceed with this factory's status retrieval
                // and result collector should be notified now.
                factoryStatus.complete(null);
                return;
            }

            NodeSelectorState selectorRsp = o.getBody(NodeSelectorState.class);
            if (selectorRsp.status != NodeSelectorState.Status.AVAILABLE) {
                findSelector.fail(new Throwable("Node selector status: " + selectorRsp.status));

                // Complete factoryStatus because we will not further proceed with this factory's status retrieval
                // and result collector should be notified now.
                factoryStatus.complete(null);
                return;
            }

            findSelector.complete(null);
        }).sendWith(this);
        return findSelector;
    }
}
