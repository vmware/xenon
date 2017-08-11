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
import java.util.function.Function;
import java.util.logging.Level;

import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.OperationJoin.JoinedCompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceConfiguration;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.SynchronizationTaskService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;

/**
 *
 * This service provides one point API to get availability status all factories.
 *
 * Availability of Factory service is determined by these factors in this particular order.
 *    1.  Node selector of factory is AVAILABLE? If it is not available then factory is declared as UNAVAILABLE.
 *        if node selector of factory is unavailable then we do not proceed to next step.
 *    2.  Factory availability stats from the factory owner node.
 *    3.  Factory synchronization task's status from the factory owner node for determining if it is SYNCHING.
 *
 * Node Selector of the factory is critical to find factory's availability status. Factory's config
 * provides the node selector link, which is then first queried to find its own status, and then used
 * to find the owner of the factory. If node-selector is not AVAILABLE then owner cannot be found
 * and hence we call factory to be UNAVAILABLE in that case.
 *
 */
public class SynchronizationManagementService extends StatelessService {
    public static final String SELF_LINK = ServiceUriPaths.CORE_MANAGEMENT + "/synch";

    public static class SynchronizationManagementState {
        public enum Status {
            AVAILABLE,
            UNAVAILABLE,
            SYNCHING
        }
        /**
         * Availability status of the factory.
         */
        Status status = Status.UNAVAILABLE;

        /**
         * Factory owner's Id, which will be null if factory owner could not be determined.
         */
        String factoryOwner;
    }

    @Override
    public void handleGet(Operation get) {
        List<DeferredResult<Object>> factoryStatuses = Collections.synchronizedList(new ArrayList<>());
        List<Operation> configGets = new ArrayList<>();
        ServiceDocumentQueryResult result = new ServiceDocumentQueryResult();
        result.documents = new HashMap<>();

        // Get the list of all factories and fan-out their status retrieval operations.
        Operation op = Operation.createGet(null).setCompletion((o, e) -> {
            if (e != null) {
                get.setBody(o.getBodyRaw());
                get.complete();
                return;
            }

            // For each factory link, get its config and then get factory status based
            // on node-selector link in that factory's config.
            ServiceDocumentQueryResult factories = o.getBody(ServiceDocumentQueryResult.class);
            for (String factorySelfLink : factories.documentLinks) {
                URI configUri = UriUtils.buildConfigUri(this.getHost(), factorySelfLink);
                result.documents.put(factorySelfLink, new SynchronizationManagementState());

                // Factory config completion handler will perform all subsequent operations to get the
                // status of this factory.
                CompletionHandler completion = factoryConfigGetCompletion(result, factoryStatuses, factorySelfLink);

                Operation configGet = Operation.createGet(configUri)
                        .setCompletion(completion);
                configGets.add(configGet);
            }

            // Collect all factory results, pack them and return them to the caller.
            JoinedCompletionHandler joinedCompletion = (os, fs) -> {
                DeferredResult.allOf(factoryStatuses)
                        .thenApply(packAndReturnResults(get, result));
            };

            OperationJoin.create(configGets)
                    .setCompletion(joinedCompletion)
                    .sendWith(this);
        });

        this.getHost().queryServiceUris(EnumSet.of(ServiceOption.FACTORY), true, op, null);
    }

    private Operation.CompletionHandler factoryConfigGetCompletion(
            ServiceDocumentQueryResult result, List<DeferredResult<Object>> factoryStatuses, String link) {
        return (o, e) -> {
            if (e != null) {
                this.log(Level.WARNING, "Failed to GET factory config: %s", e);
                return;
            }

            String peerNodeSelectorPath = o.getBody(ServiceConfiguration.class).peerNodeSelectorPath;
            DeferredResult<Object> factoryStatus = getFactoryStatus(link, result, peerNodeSelectorPath);
            factoryStatuses.add(factoryStatus);
        };
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
        // Completion of factoryStatus in the future will notifies the caller that factory's status is updated.
        DeferredResult
                .allOf(getNodeSelectorAvailability(result, selectorPost, factoryLink, factoryStatus))
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
            ServiceDocumentQueryResult result, Operation synchGet, DeferredResult<Object> factoryStatus) {
        DeferredResult<Object> synchronizationTask = new DeferredResult<>();
        synchGet.setCompletion(
                (o, e) -> {
                    if (e != null) {
                        this.log(Level.WARNING, "Failed to GET synchronization task status: %s", e);
                        synchronizationTask.complete(null);
                        factoryStatus.complete(null);
                        return;
                    }

                    SynchronizationTaskService.State s = o.getBody(SynchronizationTaskService.State.class);
                    SynchronizationManagementState synchState =
                            (SynchronizationManagementState) result.documents.get(s.factorySelfLink);
                    if (s.taskInfo.stage == TaskState.TaskStage.STARTED &&
                            synchState.status.equals(SynchronizationManagementState.Status.UNAVAILABLE)) {
                        synchState.status = SynchronizationManagementState.Status.SYNCHING;
                    }
                    synchronizationTask.complete(null);
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
            SynchronizationManagementState synchState =
                    (SynchronizationManagementState) result.documents.get(UriUtils.getParentPath(s.documentSelfLink));
            ServiceStats.ServiceStat availableStat = s.entries.get(Service.STAT_NAME_AVAILABLE);
            synchState.factoryOwner = s.documentOwner;
            synchState.status = SynchronizationManagementState.Status.UNAVAILABLE;

            if (availableStat != null && availableStat.latestValue == STAT_VALUE_TRUE) {
                synchState.status = SynchronizationManagementState.Status.AVAILABLE;
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

            String synchLink = UriUtils.buildUriPath(
                    SynchronizationTaskService.FACTORY_LINK,
                    UriUtils.convertPathCharsFromLink(selectRsp.key));

            URI synchOnOwner = UriUtils.buildUri(selectRsp.ownerNodeGroupReference, synchLink);
            URI factoryStatOnOwner = UriUtils.buildStatsUri(UriUtils.buildUri(selectRsp.ownerNodeGroupReference, selectRsp.key));

            synchGet.setUri(synchOnOwner);
            factoryStatGet.setUri(factoryStatOnOwner);
            findSelector.complete(null);
        }).sendWith(this);
        return findSelector;
    }

    private DeferredResult<Object> getNodeSelectorAvailability(
            ServiceDocumentQueryResult result, Operation selectPost, String factoryLink, DeferredResult<Object> factoryStatus) {
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

            SynchronizationManagementState synchState =
                    (SynchronizationManagementState) result.documents.get(factoryLink);

            NodeSelectorState selectorRsp = o.getBody(NodeSelectorState.class);
            if (selectorRsp.status != NodeSelectorState.Status.AVAILABLE) {
                synchState.status = SynchronizationManagementState.Status.UNAVAILABLE;
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
