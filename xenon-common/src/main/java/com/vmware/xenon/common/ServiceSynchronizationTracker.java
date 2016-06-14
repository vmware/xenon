/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceHost.MaintenanceStage;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeSelectorSynchronizationService.SynchronizePeersRequest;
import com.vmware.xenon.services.common.NodeState;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Sequences service periodic maintenance
 */
class ServiceSynchronizationTracker {
    public static ServiceSynchronizationTracker create(ServiceHost host) {
        ServiceSynchronizationTracker sst = new ServiceSynchronizationTracker();
        sst.host = host;
        return sst;
    }

    private ServiceHost host;

    private final ConcurrentSkipListMap<String, Long> synchronizationTimes = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<String, Long> synchronizationRequiredServices = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<String, Long> synchronizationActiveServices = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<String, NodeGroupState> pendingNodeSelectorsForFactorySynch = new ConcurrentSkipListMap<>();

    public void addService(String servicePath, long timeMicros) {
        this.synchronizationRequiredServices.put(servicePath, timeMicros);
    }

    public void removeService(String path) {
        this.synchronizationActiveServices.remove(path);
        this.synchronizationRequiredServices.remove(path);
    }

    private void scheduleNodeGroupChangeMaintenance(String nodeSelectorPath, Operation op) {
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        if (nodeSelectorPath == null) {
            throw new IllegalArgumentException("nodeGroupPath is required");
        }

        NodeSelectorService nss = this.host.findNodeSelectorService(nodeSelectorPath,
                Operation.createGet(null));
        if (nss == null) {
            throw new IllegalArgumentException("Node selector not found: " + nodeSelectorPath);
        }
        String ngPath = nss.getNodeGroup();
        Operation get = Operation
                .createGet(UriUtils.buildUri(this.host, ngPath))
                .setReferer(this.host.getUri())
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.log(Level.WARNING,
                                        "Failure getting node group state: %s", e.toString());
                                if (op != null) {
                                    op.fail(e);
                                }
                                return;
                            }

                            NodeGroupState ngs = o.getBody(NodeGroupState.class);
                            this.pendingNodeSelectorsForFactorySynch.put(nodeSelectorPath, ngs);
                            if (op != null) {
                                op.complete();
                            }
                        });
        this.host.sendRequest(get);
    }

    /**
     * Infrastructure use only. Invoked by a factory service to either start or synchronize
     * a child service
     */
    void startOrSynchService(Operation post, Service child, NodeGroupState ngs) {
        String path = post.getUri().getPath();
        // not a thread safe check, but startService() will do the right thing
        Service s = this.host.findService(path);

        boolean skipSynch = false;
        if (ngs != null) {
            NodeState self = ngs.nodes.get(this.host.getId());
            if (self.membershipQuorum == 1 && ngs.nodes.size() == 1) {
                skipSynch = true;
            }
        } else {
            // Only replicated services will supply node group state. Others do not need to
            // synchronize
            skipSynch = true;
        }

        if (s == null) {
            post.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK);
            this.host.startService(post, child);
            return;
        }

        if (skipSynch) {
            post.complete();
            return;
        }

        Operation synchPut = Operation.createPut(post.getUri())
                .setBody(new ServiceDocument())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_FORWARDING)
                .setReplicationDisabled(true)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH)
                .transferRefererFrom(post)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        post.setStatusCode(o.getStatusCode()).setBodyNoCloning(o.getBodyRaw())
                                .fail(e);
                        return;
                    }

                    post.complete();
                });

        this.host.sendRequest(synchPut);
    }

    /**
     * Infrastructure use only.
     *
     * Determines the owner for the given service and if the local node is owner, proceeds
     * with synchronization.
     *
     * This method is called in the following cases:
     *
     * 1) Synchronization of a factory service, due to node group change. This includes
     * synchronization after host restart. In this case isFactorySync is true and we proceed
     * with synchronizing state on behalf of the factory, even if the local node is not owner
     * for the specific child service. This solves the case where a new node is elected owner
     * for a factory but does not have any services
     *
     * 2) Synchronization due to conflict on epoch, version or owner, on a specific stateful
     * service instance. The service instance will call this method to synchronize peers.
     *
     * Note that case 1) actually causes PUTs to be send out, which implicitly invokes 2). Its
     * this recursion that we need to break which is why we check in the completion below if
     * the rsp.isLocalHostOwner == true || isFactorySync.
     */
    void selectServiceOwnerAndSynchState(Service s, Operation op, boolean isFactorySync) {
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                this.host.log(Level.WARNING, "Failure partitioning %s: %s", op.getUri(),
                        e.toString());
                op.fail(e);
                return;
            }

            SelectOwnerResponse rsp = o.getBody(SelectOwnerResponse.class);
            if (op.isFromReplication()) {
                // replicated requests should not synchronize, that is done on the owner node
                if (op.isCommit()) {
                    // remote node is telling us to commit the owner changes
                    s.toggleOption(ServiceOption.DOCUMENT_OWNER, rsp.isLocalHostOwner);
                }
                op.complete();
                return;
            }

            s.toggleOption(ServiceOption.DOCUMENT_OWNER, rsp.isLocalHostOwner);

            if (ServiceHost.isServiceCreate(op) || (!isFactorySync && !rsp.isLocalHostOwner)) {
                // if this is from a client, do not synchronize. an conflict can be resolved
                // when we attempt to replicate the POST.
                // if this is synchronization attempt and we are not the owner, do nothing
                op.complete();
                return;
            }

            // we are on owner node, proceed with synchronization logic that will discover
            // and push, latest, best state, to all peers
            synchronizeWithPeers(s, op, rsp);
        };

        Operation selectOwnerOp = Operation.createPost(null)
                .setExpiration(op.getExpirationMicrosUtc())
                .setCompletion(c);

        this.host.selectOwner(s.getPeerNodeSelectorPath(), s.getSelfLink(), selectOwnerOp);
    }

    private void synchronizeWithPeers(Service s, Operation op, SelectOwnerResponse rsp) {
        // service is durable and replicated. We need to ask our peers if they
        // have more recent state version than we do, then pick the latest one
        // (or the most valid one, depending on peer consensus)

        SynchronizePeersRequest t = SynchronizePeersRequest.create();
        t.stateDescription = this.host.buildDocumentDescription(s);
        t.wasOwner = s.hasOption(ServiceOption.DOCUMENT_OWNER);
        t.isOwner = rsp.isLocalHostOwner;
        t.ownerNodeReference = rsp.ownerNodeGroupReference;
        t.ownerNodeId = rsp.ownerNodeId;
        t.options = s.getOptions();
        t.state = op.hasBody() ? op.getBody(s.getStateType()) : null;
        t.factoryLink = UriUtils.getParentPath(s.getSelfLink());
        if (t.factoryLink == null || t.factoryLink.isEmpty()) {
            String error = String.format("Factory not found for %s."
                    + "If the service is not created through a factory it should not set %s",
                    s.getSelfLink(), ServiceOption.OWNER_SELECTION);
            op.fail(new IllegalStateException(error));
            return;
        }

        if (t.state == null) {
            // we have no initial state or state from storage. Create an empty state so we can
            // compare with peers
            ServiceDocument template = null;
            try {
                template = s.getStateType().newInstance();
            } catch (Throwable e) {
                this.host.log(Level.SEVERE, "Could not create instance state type: %s",
                        e.toString());
                op.fail(e);
                return;
            }

            template.documentSelfLink = s.getSelfLink();
            template.documentEpoch = 0L;
            // set version to negative so we do not select this over peer state
            template.documentVersion = -1;
            t.state = template;
        }

        CompletionHandler c = (o, e) -> {
            ServiceDocument selectedState = null;

            if (this.host.isStopping()) {
                op.fail(new CancellationException());
                return;
            }

            if (e != null) {
                op.fail(e);
                return;
            }

            if (o.hasBody()) {
                selectedState = o.getBody(s.getStateType());
            } else {
                // peers did not have a better state to offer
                op.linkState(null).complete();
                return;
            }

            if (ServiceDocument.isDeleted(selectedState)) {
                op.fail(new IllegalStateException(
                        "Document marked deleted by peers: " + s.getSelfLink()));
                selectedState.documentSelfLink = s.getSelfLink();
                selectedState.documentUpdateAction = Action.DELETE.toString();
                // delete local version
                this.host.saveServiceState(s, Operation.createDelete(UriUtils.buildUri(this.host,
                        s.getSelfLink())).setReferer(s.getUri()),
                        selectedState);
                return;
            }

            // indicate that synchronization occurred, we got an updated state from peers
            op.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH);

            // The remote peers have a more recent state than the one we loaded from the store.
            // Use the peer service state as the initial state.
            op.setBodyNoCloning(selectedState).complete();
        };

        URI synchServiceForGroup = UriUtils.extendUri(
                UriUtils.buildUri(this.host, s.getPeerNodeSelectorPath()),
                ServiceUriPaths.SERVICE_URI_SUFFIX_SYNCHRONIZATION);
        Operation synchPost = Operation
                .createPost(synchServiceForGroup)
                .setBodyNoCloning(t)
                .setReferer(s.getUri())
                .setCompletion(c);
        this.host.sendRequest(synchPost);
    }

    public void scheduleNodeGroupChangeMaintenance(String nodeSelectorPath) {
        long now = Utils.getNowMicrosUtc();
        this.host.log(Level.INFO, "%s %d", nodeSelectorPath, now);
        this.synchronizationTimes.put(nodeSelectorPath, now);
        scheduleNodeGroupChangeMaintenance(nodeSelectorPath, null);
    }

    public void performNodeSelectorChangeMaintenance(Operation post, long now,
            MaintenanceStage nextStage, boolean isCheckRequired, long deadline) {

        if (isCheckRequired && checkAndScheduleNodeSelectorSynch(post, nextStage, deadline)) {
            return;
        }

        try {
            Iterator<Entry<String, NodeGroupState>> it = this.pendingNodeSelectorsForFactorySynch
                    .entrySet()
                    .iterator();
            while (it.hasNext()) {
                Entry<String, NodeGroupState> e = it.next();
                it.remove();
                performNodeSelectorChangeMaintenance(e);
            }
        } finally {
            this.host.performMaintenanceStage(post, nextStage, deadline);
        }
    }

    private boolean checkAndScheduleNodeSelectorSynch(Operation post, MaintenanceStage nextStage,
            long deadline) {
        boolean hasSynchOccuredAtLeastOnce = false;
        for (Long synchTime : this.synchronizationTimes.values()) {
            if (synchTime != null && synchTime > 0) {
                hasSynchOccuredAtLeastOnce = true;
            }
        }

        if (!hasSynchOccuredAtLeastOnce) {
            return false;
        }

        Set<String> selectorPathsToSynch = new HashSet<>();
        // we have done at least once synchronization. Check if any services that require synch
        // started after the last node group change, and if so, schedule them
        for (Entry<String, Long> en : this.synchronizationRequiredServices.entrySet()) {
            Long lastSynchTime = en.getValue();
            String link = en.getKey();
            Service s = this.host.findService(link, true);
            if (s == null || s.getProcessingStage() != ProcessingStage.AVAILABLE) {
                continue;
            }
            String selectorPath = s.getPeerNodeSelectorPath();
            Long selectorSynchTime = this.synchronizationTimes.get(selectorPath);
            if (selectorSynchTime == null) {
                continue;
            }
            if (lastSynchTime < selectorSynchTime) {
                this.host.log(Level.FINE, "Service %s started at %d, last synch at %d", link,
                        lastSynchTime, selectorSynchTime);
                selectorPathsToSynch.add(s.getPeerNodeSelectorPath());
            }
        }

        if (selectorPathsToSynch.isEmpty()) {
            return false;
        }

        AtomicInteger pending = new AtomicInteger(selectorPathsToSynch.size());
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                if (!this.host.isStopping()) {
                    this.host.log(Level.WARNING, "skipping synchronization, error: %s",
                            Utils.toString(e));
                }
                this.host.performMaintenanceStage(post, nextStage, deadline);
                return;
            }
            int r = pending.decrementAndGet();
            if (r != 0) {
                return;
            }

            // we refreshed the pending selector list, now ready to do kick of synchronization
            performNodeSelectorChangeMaintenance(post, Utils.getNowMicrosUtc(), nextStage, false,
                    deadline);
        };

        for (String path : selectorPathsToSynch) {
            Operation synch = Operation.createPost(this.host.getUri()).setCompletion(c);
            scheduleNodeGroupChangeMaintenance(path, synch);
        }
        return true;
    }

    private void performNodeSelectorChangeMaintenance(Entry<String, NodeGroupState> entry) {
        String nodeSelectorPath = entry.getKey();
        Long selectorSynchTime = this.synchronizationTimes.get(nodeSelectorPath);
        NodeGroupState ngs = entry.getValue();
        long now = Utils.getNowMicrosUtc();

        for (Entry<String, Long> en : this.synchronizationActiveServices.entrySet()) {
            String link = en.getKey();
            Service s = this.host.findService(link, true);
            if (s == null) {
                continue;
            }

            ServiceHostState hostState = this.host.getStateNoCloning();
            long delta = now - en.getValue();
            boolean shouldLog = false;
            if (delta > hostState.operationTimeoutMicros) {
                s.toggleOption(ServiceOption.INSTRUMENTATION, true);
                s.adjustStat(Service.STAT_NAME_NODE_GROUP_SYNCH_DELAYED_COUNT, 1);
                ServiceStat st = s.getStat(Service.STAT_NAME_NODE_GROUP_SYNCH_DELAYED_COUNT);
                if (st != null && st.latestValue % 10 == 0) {
                    shouldLog = true;
                }
            }

            long deltaSeconds = TimeUnit.MICROSECONDS.toSeconds(delta);
            if (shouldLog) {
                this.host.log(Level.WARNING, "Service %s has been synchronizing for %d seconds",
                        link, deltaSeconds);
            }

            if (hostState.peerSynchronizationTimeLimitSeconds < deltaSeconds) {
                this.host.log(Level.WARNING, "Service %s has exceeded synchronization limit of %d",
                        link, hostState.peerSynchronizationTimeLimitSeconds);
                this.synchronizationActiveServices.remove(link);
            }
        }

        for (Entry<String, Long> en : this.synchronizationRequiredServices
                .entrySet()) {
            now = Utils.getNowMicrosUtc();
            if (this.host.isStopping()) {
                return;
            }

            String link = en.getKey();
            Long lastSynchTime = en.getValue();

            if (lastSynchTime >= selectorSynchTime) {
                continue;
            }

            if (this.synchronizationActiveServices.get(link) != null) {
                // service actively synchronizing, do not re-schedule
                this.host.log(Level.WARNING, "Skipping synch for service %s, already in progress",
                        link);
                // service can cancel a pending synchronization if it detects node group has
                // changed since it started synchronization for the current epoch
                continue;
            }

            Service s = this.host.findService(link, true);
            if (s == null) {
                continue;
            }

            if (!s.hasOption(ServiceOption.FACTORY)) {
                continue;
            }

            if (!s.hasOption(ServiceOption.REPLICATION)) {
                continue;
            }

            String serviceSelectorPath = s.getPeerNodeSelectorPath();
            if (!nodeSelectorPath.equals(serviceSelectorPath)) {
                continue;
            }

            Operation maintOp = Operation.createPost(s.getUri()).setCompletion((o, e) -> {
                this.synchronizationActiveServices.remove(link);
                if (e != null) {
                    this.host.log(Level.WARNING, "Node group change maintenance failed for %s: %s",
                            s.getSelfLink(),
                            e.getMessage());
                }

                this.host.log(Level.FINE, "Synch done for selector %s, service %s",
                        nodeSelectorPath, s.getSelfLink());
            });

            // update service entry so we do not reschedule it
            this.synchronizationRequiredServices.put(link, now);
            this.synchronizationActiveServices.put(link, now);

            ServiceMaintenanceRequest body = ServiceMaintenanceRequest.create();
            body.reasons.add(MaintenanceReason.NODE_GROUP_CHANGE);
            body.nodeGroupState = ngs;
            maintOp.setBodyNoCloning(body);

            long n = now;
            // allow overlapping node group change maintenance requests
            this.host
                    .run(() -> {
                        OperationContext.setAuthorizationContext(this.host
                                .getSystemAuthorizationContext());
                        this.host.log(Level.FINE, " Synchronizing %s (last:%d, sl: %d now:%d)",
                                link,
                                lastSynchTime, selectorSynchTime, n);
                        s.adjustStat(Service.STAT_NAME_NODE_GROUP_CHANGE_MAINTENANCE_COUNT, 1);
                        s.handleMaintenance(maintOp);
                    });
        }
    }

    public void close() {
        this.synchronizationTimes.clear();
        this.synchronizationRequiredServices.clear();
        this.synchronizationActiveServices.clear();
        this.pendingNodeSelectorsForFactorySynch.clear();
    }
}