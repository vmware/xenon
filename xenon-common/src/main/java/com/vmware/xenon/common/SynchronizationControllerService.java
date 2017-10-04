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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.vmware.xenon.services.common.NodeGroupBroadcastResponse;
import com.vmware.xenon.services.common.NodeGroupService;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeState;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;


public class SynchronizationControllerService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.SYNCHRONIZATION_CONTROLLERS;

    public static final String PROPERTY_NAME_MAX_SYNCH_RETRY_COUNT =
            Utils.PROPERTY_NAME_PREFIX + "FactoryService.MAX_SYNCH_RETRY_COUNT";

    private static final Long SYNCH_DELETED_NODE_DELAY_MICROS = TimeUnit.SECONDS.toMicros(10);

    /**
     * Maximum synch-task retry limit.
     * We are using exponential backoff for synchronization retry, that means last synch retry will be tried
     * after 2 ^ 8 * getMaintenanceIntervalMicros(), which is ~4 minutes if maintenance interval is 1 second.
     */
    public static final int MAX_SYNCH_RETRY_COUNT = Integer.getInteger(
            PROPERTY_NAME_MAX_SYNCH_RETRY_COUNT, 8);

    public static final Integer SELF_QUERY_RESULT_LIMIT = Integer.getInteger(
            Utils.PROPERTY_NAME_PREFIX
                    + "FactoryService.SELF_QUERY_RESULT_LIMIT", 1000);

    public enum NodeGroupChangeType {
        NO_CHANGE,
        NODE_ADDED,
        NODE_REMOVED,
        NODE_RESTART,
        UNSTABLE
    }

    public enum UpdateType {
        SYNCHROINZE,
        TASK_STATUS_UPDATE,
        CONTROLLER_STATUS_UPDATE,
    }

    private NodeState deletedNode = null;
    private final Object lock = new Object();
    private ScheduledFuture<?> task = null;

    public static Service createFactory() {
        return FactoryService.create(SynchronizationControllerService.class);
    }

    public static class State extends ServiceDocument {
        /**
         * Synchronization epoch
         */
        public Long synchEpoch = 0L;

        /**
         * Factory synchronization owner.
         */
        public String synchOwner;

        /**
         * Node group state
         */
        public NodeGroupService.NodeGroupState nodeGroupState;

        /**
         * SelfLink of the FactoryService that will be synchronized by this task.
         * This value is immutable and gets set once in handleStart.
         */
        public String factorySelfLink;

        /**
         * documentKind of childServices created by the FactoryService.
         * This value is immutable and gets set once in handleStart.
         */
        public String factoryStateKind;

        /**
         * The node-selector used linked to the FactoryService.
         * This value is immutable and gets set once in handleStart.
         */
        public String nodeSelectorLink;

        /**
         * ServiceOptions supported by the child service.
         * This value is immutable and gets set once in handleStart.
         */
        public EnumSet<ServiceOption> childOptions;


        /**
         * Document index link used by the child service
         */
        public String childDocumentIndexLink;

        /**
         * Patch update type to redirect to appropriate handler.
         */
        public UpdateType updateType;

        /**
         * Factory's availability flag.
         */
        public boolean isAvailable;
    }

    public SynchronizationControllerService() {
        super(State.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
        toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
    }

    @Override
    public void handleStart(Operation start) {
        State initialState = validateStartPost(start);
        if (initialState == null) {
            return;
        }

        startSynchronizationTask(initialState, null);
        start.complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        SynchronizationControllerService.State request = patch.getBody(SynchronizationControllerService.State.class);

        if (request.updateType == UpdateType.TASK_STATUS_UPDATE) {
            handleSynchronizationTaskStatusUpdate(patch, request);
            return;
        }

        if (request.updateType == UpdateType.CONTROLLER_STATUS_UPDATE) {
            handleSynchronizationControllerStatusUpdate(patch, request);
            return;
        }

        State current = getState(patch);
        NodeState node = new NodeState();
        setState(patch, request);
        Operation patchClone = patch.clone();
        NodeGroupChangeType changeType = getNodeGroupChangeType(current.nodeGroupState, request.nodeGroupState, node);
        startSynchronization(patchClone, current.nodeGroupState, request.nodeGroupState, node, request, changeType);
        patch.complete();
    }

    public void handleSynchronizationTaskStatusUpdate(Operation patch, SynchronizationControllerService.State request) {
        State state = getState(patch);
        Long epoch = state.synchEpoch + 1;
        patch.complete();

        List<Operation> patches = new ArrayList<>();

        for (NodeState node : state.nodeGroupState.nodes.values()) {
            String path = UriUtils.buildUriPath(
                    SynchronizationControllerService.FACTORY_LINK,
                    UriUtils.convertPathCharsFromLink(state.factorySelfLink));

            SynchronizationControllerService.State update = new SynchronizationControllerService.State();
            update.synchEpoch = epoch;
            update.synchOwner = this.getHost().getId();
            update.isAvailable = request.isAvailable;
            update.updateType =  UpdateType.CONTROLLER_STATUS_UPDATE;
            patches.add(Operation.createPatch(UriUtils.buildUri(node.groupReference, path))
                    .setBody(update)
                    .setRetryCount(3));
        }

        OperationJoin.create(patches)
                .setCompletion((os, fs) -> {
                    if (fs != null && !fs.isEmpty()) {
                        this.log(Level.SEVERE, "Could not update peers with status: %s", fs.values().iterator().next());
                    }
                }).sendWith(this);
    }

    public void handleSynchronizationControllerStatusUpdate(Operation patch, SynchronizationControllerService.State request) {
        State state = getState(patch);
        state.synchEpoch = request.synchEpoch;
        state.synchOwner = request.synchOwner;
        setState(patch, state);
        setFactoryAvailability(state.factorySelfLink, request.isAvailable);
        patch.complete();
    }

    public State validateStartPost(Operation post) {
        State state = post.getBody(State.class);
        if (state.nodeSelectorLink == null) {
            post.fail(new IllegalArgumentException("nodeSelectorLink must be set"));
            return null;
        }
        if (state.childOptions == null) {
            post.fail(new IllegalArgumentException("childOptions must be set."));
            return null;
        }
        if (state.childDocumentIndexLink == null) {
            post.fail(new IllegalArgumentException("childDocumentIndexLink must be set."));
            return null;
        }
        return state;
    }

    private DeferredResult<Object> startSynchronizationTask(State state, QueryTask.Query query) {
        DeferredResult<Object> deferredResult = new DeferredResult<>();

        String path = UriUtils.buildUriPath(
                SynchronizationTaskService.FACTORY_LINK,
                UriUtils.convertPathCharsFromLink(state.factorySelfLink));

        // Create a place-holder Synchronization-Task for this factory service
        Operation post = Operation
                .createPost(UriUtils.buildUri(this.getHost(), path))
                .setBody(createSynchronizationTaskState(null, state, query))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logSevere(e);
                        deferredResult.fail(e);
                        return;
                    }

                    if (!state.childOptions.contains(ServiceOption.PERSISTENCE)) {
                        setFactoryAvailability(state.factorySelfLink, true);
                        deferredResult.complete(null);
                        return;
                    }


                    deferredResult.complete(null);

                    // complete factory start POST immediately. Asynchronously
                    // kick-off the synchronization-task to load all child
                    // services. Requests to a child not yet loaded will be
                    // queued by the framework.
                    Operation clonedOp = new Operation();
                    //startFactorySynchronizationTask(clonedOp, null, state);


                    if (!state.childOptions.contains(ServiceOption.REPLICATION)) {
                        clonedOp.setCompletion((op, t) -> {
                            if (t != null && !getHost().isStopping()) {
                                logWarning("Failure in kicking-off synchronization-task: %s", t.getMessage());
                                return;
                            }
                        });
                        startFactorySynchronizationTask(clonedOp, null, state, query);
                        return;
                    }
                    // when the node group becomes available, the maintenance handler will initiate
                    // service start and synchronization

                });

        SynchronizationTaskService service = new SynchronizationTaskService();
        this.getHost().startService(post, service);
        return deferredResult;
    }

    private void setFactoryAvailability(
            String factoryLink, boolean isAvailable) {
        ServiceStats.ServiceStat body = new ServiceStats.ServiceStat();
        body.name = Service.STAT_NAME_AVAILABLE;
        body.latestValue = isAvailable ? STAT_VALUE_TRUE : STAT_VALUE_FALSE;

        Operation put = Operation.createPut(
                UriUtils.buildAvailableUri(this.getHost(), factoryLink))
                .setBody(body)
                .setRetryCount(3)
                .setConnectionSharing(true)
                .setConnectionTag(ServiceClient.CONNECTION_TAG_SYNCHRONIZATION)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logSevere("Setting factory availability failed with error %s", e.getMessage());
                    }
                });
        sendRequest(put);
    }

    private void startFactorySynchronizationTask(Operation parentOp, Long membershipUpdateTimeMicros, State state, QueryTask.Query query) {
        SynchronizationTaskService.State task = createSynchronizationTaskState(
                membershipUpdateTimeMicros, state, query);
        Operation post = Operation
                .createPost(this, ServiceUriPaths.SYNCHRONIZATION_TASKS)
                .setBody(task)
                .setCompletion((o, e) -> {
                    boolean retrySynch = false;

                    if (o.getStatusCode() >= Operation.STATUS_CODE_FAILURE_THRESHOLD) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        logWarning("HTTP error on POST to synch task: %s", Utils.toJsonHtml(rsp));

                        // Ignore if the request failed because the current synch-request
                        // was considered out-dated by the synchronization-task.
                        if (o.getStatusCode() == Operation.STATUS_CODE_BAD_REQUEST &&
                                rsp.getErrorCode() == ServiceErrorResponse.ERROR_CODE_OUTDATED_SYNCH_REQUEST) {
                            parentOp.complete();
                            return;
                        }

                        retrySynch = true;
                    }

                    if (e != null) {
                        logWarning("Failure on POST to synch task: %s", e.getMessage());
                        parentOp.fail(e);
                        retrySynch = true;
                    } else {
                        SynchronizationTaskService.State rsp = null;
                        rsp = o.getBody(SynchronizationTaskService.State.class);
                        if (rsp.taskInfo.stage.equals(TaskState.TaskStage.FAILED)) {
                            logWarning("Synch task failed %s", Utils.toJsonHtml(rsp));
                            retrySynch = true;
                        }
                        parentOp.complete();
                    }

                    if (retrySynch) {
                        scheduleSynchronizationRetry(parentOp, state, query);
                        return;
                    }

                    setStat(STAT_NAME_SYNCH_TASK_RETRY_COUNT, 0);
                });
        sendRequest(post);
    }

    private void scheduleSynchronizationRetry(Operation parentOp, State state, QueryTask.Query query) {
        if (getHost().isStopping()) {
            return;
        }

        adjustStat(STAT_NAME_SYNCH_TASK_RETRY_COUNT, 1);

        ServiceStats.ServiceStat stat = getStat(STAT_NAME_SYNCH_TASK_RETRY_COUNT);
        if (stat != null && stat.latestValue  > 0) {
            if (stat.latestValue > MAX_SYNCH_RETRY_COUNT) {
                logSevere("Synchronization task failed after %d tries", (long)stat.latestValue - 1);
                adjustStat(STAT_NAME_CHILD_SYNCH_FAILURE_COUNT, 1);
                return;
            }
        }

        // Clone the parent operation for reuse outside the schedule call for
        // the original operation to be freed in current thread.
        Operation op = parentOp.clone();

        // Use exponential backoff algorithm in retry logic. The idea is to exponentially
        // increase the delay for each retry based on the number of previous retries.
        // This is done to reduce the load of retries on the system by all the synch tasks
        // of all factories at the same time, and giving system more time to stabilize
        // in next retry then the previous retry.
        long delay = getExponentialDelay();

        logWarning("Scheduling retry of child service synchronization task in %d seconds",
                TimeUnit.MICROSECONDS.toSeconds(delay));
        getHost().scheduleCore(() -> synchronizeChildServicesIfOwner(op, state, query),
                delay, TimeUnit.MICROSECONDS);
    }

    void synchronizeChildServicesIfOwner(Operation maintOp, State state, QueryTask.Query query) {
        // Become unavailable until synchronization is complete.
        // If we are not the owner, we stay unavailable
        State state1 = maintOp.getBody(State.class);
        List<NodeState> notAvailableNodes = state1.nodeGroupState.nodes.values().stream().filter(m -> !m.status.equals(NodeState.NodeStatus.AVAILABLE)).collect(toList());

        if (notAvailableNodes.size() > 0) {
            this.log(Level.INFO, "UNAVAILABLE NODE");
        }

        setFactoryAvailability(state.factorySelfLink, false);
        OperationContext opContext = OperationContext.getOperationContext();
        // Only one node is responsible for synchronizing the child services of a given factory.
        // Ask the runtime if this is the owner node, using the factory self link as the key.
        Operation selectOwnerOp = maintOp.clone().setExpiration(Utils.fromNowMicrosUtc(
                getHost().getOperationTimeoutMicros()));
        selectOwnerOp.setCompletion((o, e) -> {
            OperationContext.restoreOperationContext(opContext);
            if (e != null) {
                logWarning("owner selection failed: %s", e.toString());
                scheduleSynchronizationRetry(maintOp, state, query);
                maintOp.fail(e);
                return;
            }
            NodeSelectorService.SelectOwnerResponse rsp = o.getBody(NodeSelectorService.SelectOwnerResponse.class);
            if (!rsp.isLocalHostOwner) {
                // We do not need to do anything
                maintOp.complete();
                return;
            }

            if (rsp.availableNodeCount > 1) {
                verifyFactoryOwnership(maintOp, rsp, state, query);
                return;
            }

            synchronizeChildServicesAsOwner(maintOp, rsp.membershipUpdateTimeMicros, state, query);
        });

        getHost().selectOwner(state.nodeSelectorLink, state.factorySelfLink, selectOwnerOp);
    }

    private void synchronizeChildServicesAsOwner(Operation maintOp, long membershipUpdateTimeMicros, State state, QueryTask.Query query) {
        maintOp.nestCompletion((o, e) -> {
            if (e != null) {
                logWarning("Synchronization failed: %s", e.toString());
            }
            maintOp.complete();
        });
        startFactorySynchronizationTask(maintOp, membershipUpdateTimeMicros, state, query);
    }

    private void startSynchronization(Operation patch, NodeGroupState prev, NodeGroupState latest,
                                      NodeState node, State state, NodeGroupChangeType changeType) {
        switch (changeType) {
        case NODE_ADDED:
        case UNSTABLE:
            synchronizeChildServicesIfOwner(patch, state, null);
            break;
        case NODE_REMOVED:
            synchronizeOnNodeRemovalIfOwner(patch, node, state);
            break;
        case NODE_RESTART:
            synchronizeOnNodeRestartIfOwner(patch, prev, latest, node, state);
            this.log(Level.INFO, "Restarting a node");
            break;
        case NO_CHANGE:
            this.log(Level.INFO, "No change in node-group state. Skipping child service synchronization");
            break;
        default:
            throw new IllegalStateException();
        }
    }

    private void synchronizeOnNodeRestartIfOwner(Operation patch, NodeGroupState prev, NodeState node, State state) {
        synchronized (this.lock) {
            if (this.task != null) {
                this.task.cancel(false);
            }
        }

        QueryTask.Query latestSinceCondition = null;
        if (prev != null) {
            Long timeInMicros = prev.nodes.get(node.id).documentUpdateTimeMicros - TimeUnit.MINUTES.toMicros(10);
            long limitToNowInMicros = Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(10);
            QueryTask.NumericRange<Long> range = QueryTask.NumericRange.createLongRange(timeInMicros, limitToNowInMicros,
                    true, false);
            latestSinceCondition = new QueryTask.Query()
                    .setTermPropertyName(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS)
                    .setNumericRange(range)
                    .setTermMatchType(QueryTask.QueryTerm.MatchType.TERM);
            latestSinceCondition.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
        }
        synchronizeChildServicesIfOwner(patch, state, latestSinceCondition);
    }


    private void synchronizeOnNodeRestartIfOwner(Operation patch, NodeGroupState prev, NodeGroupState latest, NodeState node, State state) {
        List<Operation> gets = new ArrayList<>();
        for (NodeState n : latest.nodes.values()) {
            URI uri = UriUtils.buildUri(n.groupReference, getSelfLink());
            gets.add(Operation.createGet(uri));
        }

        OperationJoin.JoinedCompletionHandler c  = (os, fs) -> {
            if (fs != null && !fs.isEmpty()) {
                // We cannot find the status of peers so lets start synchronization from unstable state.
                startSynchronization(patch, prev, latest, node, state, NodeGroupChangeType.UNSTABLE);
                return;
            }

            Set<String> synchOwners = new HashSet<>();
            Set<Long> syncEpochs = new HashSet<>();
            for (Operation o : os.values()) {
                State body = o.getBody(State.class);
                if (body == null) {
                    break;
                }

                if (body.nodeGroupState == null) {
                    break;
                }

                syncEpochs.add(body.synchEpoch);
                synchOwners.add(body.synchOwner);
            }

            if (syncEpochs.size() == 1 && synchOwners.size() == 1
                    && !synchOwners.iterator().next().equals(null)) {
                // Peers have same synchronization owner and epoch.
                // That means this was really a restart instead of new node with old Id.
                synchronizeOnNodeRestartIfOwner(patch, prev, node, state);
                return;
            }

            // Peers could not agree on same synch owner and epoch, hence we have to synchronize all child services.
            startSynchronization(patch, prev, latest, node, state, NodeGroupChangeType.UNSTABLE);
        };

        OperationJoin.create(gets).setCompletion(c)
                .sendWith(this);
    }

    private void synchronizeOnNodeRemovalIfOwner(Operation patch, NodeState node, State state) {
        synchronized (this.lock) {
            this.task = this.getHost().schedule(() -> {
                QueryTask.Query query = new QueryTask.Query();
                query.setTermPropertyName(ServiceDocument.FIELD_NAME_OWNER);
                query.setTermMatchValue(node.id);
                query.setTermMatchType(QueryTask.QueryTerm.MatchType.TERM);
                synchronizeChildServicesIfOwner(patch, state, query);
            }, SYNCH_DELETED_NODE_DELAY_MICROS, TimeUnit.MICROSECONDS);
        }
    }

    private NodeGroupChangeType getNodeGroupChangeType(NodeGroupState prev, NodeGroupState latest, NodeState node) {
        if (prev == null) {
            return NodeGroupChangeType.UNSTABLE;
        }

        Set<String> prevNodes = prev.nodes.values().stream()
                .filter(m -> m.status.equals(NodeState.NodeStatus.AVAILABLE))
                .map(m -> m.id)
                .collect(toSet());
        Set<String> latestNodes = latest.nodes.values()
                .stream()
                .filter(m -> m.status.equals(NodeState.NodeStatus.AVAILABLE))
                .map(m -> m.id)
                .collect(toSet());
        Set<String> prevNodesClone = new HashSet<>(prevNodes);
        prevNodes.removeAll(latestNodes);
        latestNodes.removeAll(prevNodesClone);

        NodeGroupChangeType changeType;
        if (prevNodes.size() == 0 && latestNodes.size() == 0) {
            changeType = NodeGroupChangeType.NO_CHANGE;
        } else if (prevNodes.size() == 0 && latestNodes.size() == 1) {
            node.id = latestNodes.iterator().next();
            changeType = NodeGroupChangeType.NODE_ADDED;
        } else if (prevNodes.size() == 1 && latestNodes.size() == 0) {
            node.id = prevNodes.iterator().next();
            changeType = NodeGroupChangeType.NODE_REMOVED;
        } else {
            changeType = NodeGroupChangeType.UNSTABLE;
        }

        if (changeType == NodeGroupChangeType.NODE_REMOVED) {
            synchronized (this.lock) {
                if (this.deletedNode == null || this.deletedNode.id.equals(node.id)) {
                    this.deletedNode = node;
                } else {
                    changeType = NodeGroupChangeType.UNSTABLE;
                }
            }
        } else if (changeType == NodeGroupChangeType.NODE_ADDED) {
            synchronized (this.lock) {
                if (this.deletedNode != null && this.deletedNode.id.equals(node.id)) {
                    changeType = NodeGroupChangeType.NODE_RESTART;
                } else if (this.deletedNode != null && !this.deletedNode.id.equals(node.id)) {
                    changeType = NodeGroupChangeType.UNSTABLE;
                }
            }
        }

        return changeType;
    }

    /**
     * Exponential backoff rely on synch task retry count stat. If this stat is not available
     * then we will fall back to constant delay for each retry.
     * To get exponential delay, multiply retry count's power of 2 with constant delay.
     */
    private long getExponentialDelay() {
        long delay = getHost().getMaintenanceIntervalMicros();
        ServiceStats.ServiceStat stat = getStat(STAT_NAME_SYNCH_TASK_RETRY_COUNT);
        if (stat != null && stat.latestValue > 0) {
            return (1 << ((long)stat.latestValue)) * delay;
        }

        return delay;
    }

    private void verifyFactoryOwnership(Operation maintOp, NodeSelectorService.SelectOwnerResponse ownerResponse, State state, QueryTask.Query query) {
        // Local node thinks it's the owner. Let's confirm that
        // majority of the nodes in the node-group
        NodeSelectorService.SelectAndForwardRequest request =
                new NodeSelectorService.SelectAndForwardRequest();
        request.key = state.factorySelfLink;

        Operation broadcastSelectOp = Operation
                .createPost(UriUtils.buildUri(this.getHost(), state.nodeSelectorLink))
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
                        } catch (Exception e) {
                            logWarning("Exception thrown in de-serializing json response. %s", e.toString());

                            // Ignore if the remote node returned a bad response. Most likely this is because
                            // the remote node is offline and if so, ownership check for the remote node is
                            // irrelevant.
                            continue;
                        }
                        if (rsp == null || rsp.ownerNodeId == null) {
                            logWarning("%s responded with '%s'", r.getKey(), r.getValue());
                        }
                        if (!rsp.ownerNodeId.equals(this.getHost().getId())) {
                            logWarning("SelectOwner response from %s does not indicate that " +
                                    "local node %s is the owner for factory %s. JsonResponse: %s",
                                    r.getKey().toString(), this.getHost().getId(), state.factorySelfLink, r.getValue());
                            maintOp.complete();
                            return;
                        }
                    }

                    logInfo("%s elected as owner for factory %s. Starting synch ...",
                            getHost().getId(), state.factorySelfLink);
                    synchronizeChildServicesAsOwner(maintOp, ownerResponse.membershipUpdateTimeMicros, state, query);
                });

        getHost().broadcastRequest(state.nodeSelectorLink, state.factorySelfLink, true, broadcastSelectOp);
    }

    private SynchronizationTaskService.State createSynchronizationTaskState(
            Long membershipUpdateTimeMicros, State state, QueryTask.Query query) {
        SynchronizationTaskService.State task = new SynchronizationTaskService.State();
        task.documentSelfLink = UriUtils.convertPathCharsFromLink(state.factorySelfLink);
        task.factorySelfLink = state.factorySelfLink;
        task.factoryStateKind = state.factoryStateKind; //Utils.buildKind(this.getStateType());
        task.membershipUpdateTimeMicros = membershipUpdateTimeMicros;
        task.nodeSelectorLink = state.nodeSelectorLink;
        task.queryResultLimit = SELF_QUERY_RESULT_LIMIT;
        task.childDocumentIndexLink = state.childDocumentIndexLink;
        task.childOptions = state.childOptions;
        task.taskInfo = TaskState.create();
        task.taskInfo.isDirect = true;
        task.query = query;
        return task;
    }
}
