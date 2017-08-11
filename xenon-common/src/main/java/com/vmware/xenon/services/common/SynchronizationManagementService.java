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
import java.util.logging.Level;

import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.SynchronizationTaskService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;


public class SynchronizationManagementService extends StatelessService {
    public static final String SELF_LINK = ServiceUriPaths.CORE_MANAGEMENT + "/synch";

    public static class SynchronizationManagementState {
        public enum Status {
            AVAILABLE,
            UNAVAILABLE,
            SYNCHING
        }

        Status status = Status.UNAVAILABLE;
        String factoryOwner;
    }

    @Override
    public void handleGet(Operation get) {
        ServiceDocumentQueryResult getResult = new ServiceDocumentQueryResult();
        getResult.documents = new HashMap<>();

        Operation op = Operation.createGet(null).setCompletion((o, e) -> {
            if (e != null) {
                get.setBody(o.getBodyRaw());
                get.complete();
                return;
            }
            ServiceDocumentQueryResult results = o.getBody(ServiceDocumentQueryResult.class);
            for (String link : results.documentLinks) {
                getResult.documents.put(link, new SynchronizationManagementState());
            }
            getFactoryStatuses(this.getHost(),
                    getResult, ServiceUriPaths.DEFAULT_NODE_SELECTOR, get);
        });
        this.getHost().queryServiceUris(EnumSet.of(ServiceOption.FACTORY), true, op, null);
    }

    private void getFactoryStatuses(ServiceHost host,
                                                  ServiceDocumentQueryResult getResult,
                                                  String selectorPath, Operation get) {

        URI nodeSelector = UriUtils.buildUri(host, selectorPath);

        List<Operation> selectorPosts = new ArrayList<>();
        for (String factory : getResult.documents.keySet()) {
            NodeSelectorService.SelectAndForwardRequest req = new NodeSelectorService.SelectAndForwardRequest();
            req.key = factory;
            selectorPosts.add(Operation.createPost(nodeSelector)
                    .setReferer(host.getPublicUri())
                    .setBodyNoCloning(req));
        }

        List<Operation> synchGets = new ArrayList<>();
        List<Operation> factoryStatGets = new ArrayList<>();

        DeferredResult<Object> deferredResult = new DeferredResult<>();
        deferredResult
                .thenCompose(aVoid -> queryNodeSelectors(host, selectorPosts, synchGets, factoryStatGets))
                .thenCompose(aVoid -> queryFactoryStats(host, getResult, factoryStatGets))
                .thenCompose(aVoid -> querySynchronizationTasks(host, getResult, get, synchGets))
                .exceptionally(throwable -> {
                    get.fail(throwable);
                    return null;
                });

        deferredResult.complete(new Object());
    }

    private DeferredResult<List<Operation>> querySynchronizationTasks(
            ServiceHost host, ServiceDocumentQueryResult getResult, Operation get, List<Operation> synchGets) {
        DeferredResult<List<Operation>> deferredSynchronizationTasks = new DeferredResult<>();
        OperationJoin.create(synchGets)
                .setCompletion(
                        (oos, ffs) -> {
                            if (ffs != null && !ffs.isEmpty()) {
                                host.log(Level.WARNING, "Failure in getting state from at-least one synchronization task: %s",
                                        ffs.values().iterator().next());
                            }
                            populateResults(getResult, oos);
                            getResult.documentOwner = host.getId();
                            getResult.documentCount = (long) getResult.documents.size();
                            getResult.documentLinks = new ArrayList<>(getResult.documents.keySet());
                            Collections.sort(getResult.documentLinks);
                            get.setBodyNoCloning(getResult);
                            get.complete();
                            deferredSynchronizationTasks.complete(null);
                        }).sendWith(host);
        return deferredSynchronizationTasks;
    }

    private DeferredResult<List<Operation>> queryFactoryStats(
            ServiceHost host, ServiceDocumentQueryResult getResult, List<Operation> factoryStatGets) {
        DeferredResult<List<Operation>> deferredFactoryStats = new DeferredResult<>();
        OperationJoin.create(factoryStatGets)
                .setCompletion((oos, ffs) -> {
                    if (ffs != null && !ffs.isEmpty()) {
                        host.log(Level.WARNING, "Failure in getting state from at-least one factory: %s",
                                ffs.values().iterator().next());
                    }
                    populateResults(getResult, oos);
                    deferredFactoryStats.complete(null);
                }).sendWith(host);
        return deferredFactoryStats;
    }

    private DeferredResult<List<Operation>> queryNodeSelectors(
            ServiceHost host, List<Operation> selectPosts, List<Operation> synchGets, List<Operation> factoryStatGets) {
        DeferredResult<List<Operation>> findSelector = new DeferredResult<>();
        OperationJoin.create(selectPosts)
                .setCompletion((os, fs) -> {
                    if (fs != null && !fs.isEmpty()) {
                        host.log(Level.WARNING, "Got problems getting at-least one node selector: %s", fs.values().iterator().next());
                    }

                    os.forEach((key, value) -> {
                        if (value.getBodyRaw() instanceof ServiceErrorResponse) {
                            return;
                        }

                        NodeSelectorService.SelectOwnerResponse selectRsp = value.getBody(NodeSelectorService.SelectOwnerResponse.class);
                        String synchLink = UriUtils.buildUriPath(
                                SynchronizationTaskService.FACTORY_LINK,
                                UriUtils.convertPathCharsFromLink(selectRsp.key));
                        URI factoryStatOnOwner = UriUtils.buildStatsUri(
                                UriUtils.buildUri(selectRsp.ownerNodeGroupReference, selectRsp.key));
                        URI synchOnOwner = UriUtils.buildUri(selectRsp.ownerNodeGroupReference, synchLink);
                        synchGets.add(Operation.createGet(synchOnOwner)
                                .setReferer(host.getPublicUri()));
                        factoryStatGets.add(Operation.createGet(factoryStatOnOwner)
                                .setReferer(host.getPublicUri()));
                    });
                    findSelector.complete(null);
                }).sendWith(host);
        return findSelector;
    }

    private void populateResults(ServiceDocumentQueryResult getResult, Map<Long, Operation> oos) {
        for (Map.Entry<Long, Operation> entry : oos.entrySet()) {
            Operation op = entry.getValue();
            if (op.getBodyRaw() instanceof ServiceErrorResponse) {
                this.getHost().log(Level.WARNING, "Found errors");
                continue;
            }

            ServiceDocument sd = (ServiceDocument) op.getBodyRaw();
            if (sd.documentKind.equals(Utils.buildKind(ServiceStats.class))) {
                ServiceStats s = op.getBody(ServiceStats.class);
                SynchronizationManagementState synchState =
                        (SynchronizationManagementState) getResult.documents.get(UriUtils.getParentPath(s.documentSelfLink));
                ServiceStats.ServiceStat availableStat = s.entries.get(Service.STAT_NAME_AVAILABLE);
                synchState.factoryOwner = s.documentOwner;
                if (availableStat != null && availableStat.latestValue == STAT_VALUE_TRUE) {
                    synchState.status = SynchronizationManagementState.Status.AVAILABLE;
                } else {
                    synchState.status = SynchronizationManagementState.Status.UNAVAILABLE;
                }
            } else {
                SynchronizationTaskService.State s = op.getBody(SynchronizationTaskService.State.class);
                SynchronizationManagementState synchState =
                        (SynchronizationManagementState) getResult.documents.get(s.factorySelfLink);
                if (TaskState.isInProgress(s.taskInfo) &&
                        synchState.status.equals(SynchronizationManagementState.Status.UNAVAILABLE)) {
                    synchState.status = SynchronizationManagementState.Status.SYNCHING;
                }
            }
        }
    }
}
