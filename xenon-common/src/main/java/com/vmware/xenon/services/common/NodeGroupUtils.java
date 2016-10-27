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

import static java.util.stream.Collectors.toMap;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.OperationJoin.JoinedCompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupBroadcastResult.PeerNodeResult;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;

public final class NodeGroupUtils {
    public static final String PROPERTY_NAME_OPERATION_TIMEOUT_SECONDS = Utils.PROPERTY_NAME_PREFIX
            + "NodeGroupUtils.operationTimeoutSeconds";

    private static final long OPERATION_TIMEOUT_SECONDS = Long.getLong(
            PROPERTY_NAME_OPERATION_TIMEOUT_SECONDS,
            TimeUnit.MICROSECONDS.toSeconds(ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS / 3));

    private NodeGroupUtils() {

    }

    /**
     * Issues a GET to service/stats and looks for {@link Service#STAT_NAME_AVAILABLE}
     * The request is issued on the node selected as owner, by the node selector co-located
     * with the service. The stat must have been modified after the most recent node group
     * change
     *
     * This method should be used only on replicated, owner selected factory services
     *
     * For xenon based applications, use {@link ServiceHost#registerForServiceAvailability(CompletionHandler, boolean, String...)}
     * with boolean set to true.
     */
    public static void checkServiceAvailability(CompletionHandler ch, Service s) {
        checkServiceAvailability(ch, s.getHost(), s.getSelfLink(), s.getPeerNodeSelectorPath());
    }

    /**
     * Issues a GET to service/stats and looks for {@link Service#STAT_NAME_AVAILABLE}
     * The request is issued on the node selected as owner, by the node selector co-located
     * with the service. The stat must have been modified after the most recent node group
     * change
     *
     * This method should be used only on replicated, owner selected factory services
     *
     * For xenon based applications, use {@link ServiceHost#registerForServiceAvailability(CompletionHandler, boolean, String...)}
     * with boolean set to true.
     */
    public static void checkServiceAvailability(CompletionHandler ch, ServiceHost host,
            String link, String selectorPath) {
        if (link == null) {
            throw new IllegalArgumentException("link is required");
        }

        URI service = UriUtils.buildUri(host, link);
        checkServiceAvailability(ch, host, service, selectorPath);
    }

    /**
     * Issues a GET to service/stats and looks for {@link Service#STAT_NAME_AVAILABLE}
     * The request is issued on the node selected as owner, by the node selector co-located
     * with the service. The stat must have been modified after the most recent node group
     * change
     *
     * This method should be used only on replicated, owner selected factory services
     *
     * For xenon based applications, use {@link ServiceHost#registerForServiceAvailability(CompletionHandler, boolean, String...)}
     * with boolean set to true.
     */
    public static void checkServiceAvailability(CompletionHandler ch, ServiceHost host,
            URI service,
            String selectorPath) {
        URI statsUri = UriUtils.buildStatsUri(service);

        if (selectorPath == null) {
            throw new IllegalArgumentException("selectorPath is required");
        }

        long timeoutMicros = Math.min(host.getOperationTimeoutMicros(),
                TimeUnit.SECONDS.toMicros(OPERATION_TIMEOUT_SECONDS));

        // Create operation to retrieve stats. This completion will execute after
        // we determine the owner node
        Operation get = Operation.createGet(statsUri).setCompletion((o, e) -> {
            if (e != null) {
                host.log(Level.WARNING, "%s to %s failed: %s",
                        o.getAction(), o.getUri(), e.toString());
                ch.handle(null, e);
                return;
            }

            ServiceStats s = o.getBody(ServiceStats.class);
            ServiceStat availableStat = s.entries.get(Service.STAT_NAME_AVAILABLE);

            if (availableStat == null || availableStat.latestValue == Service.STAT_VALUE_FALSE) {
                ch.handle(o, new IllegalStateException("not available"));
                return;
            }

            ch.handle(o, null);
        });
        get.setReferer(host.getPublicUri())
                .setExpiration(Utils.fromNowMicrosUtc(timeoutMicros));

        URI nodeSelector = UriUtils.buildUri(service, selectorPath);
        SelectAndForwardRequest req = new SelectAndForwardRequest();
        req.key = service.getPath();

        Operation selectPost = Operation.createPost(nodeSelector)
                .setReferer(host.getPublicUri())
                .setExpiration(get.getExpirationMicrosUtc())
                .setBodyNoCloning(req);
        selectPost.setCompletion((o, e) -> {
            if (e != null) {
                host.log(Level.WARNING, "SelectOwner for %s to %s failed: %s",
                        req.key, nodeSelector, e.toString());
                ch.handle(get, e);
                return;
            }
            SelectOwnerResponse selectRsp = o.getBody(SelectOwnerResponse.class);
            URI serviceOnOwner = UriUtils.buildUri(selectRsp.ownerNodeGroupReference,
                    statsUri.getPath());
            get.setUri(serviceOnOwner).sendWith(host);
        }).sendWith(host);
    }

    /**
     * Issues a convergence request to the node group service on all peers and returns success
     * if all nodes confirm that they are converged (in terms of last membership update).
     * This method should be used when the supplied host is not part of the node group
     */
    public static void checkConvergenceFromAnyHost(ServiceHost host, NodeGroupState ngs,
            Operation parentOp) {
        checkConvergenceAcrossPeers(host, ngs, parentOp);
    }

    /**
     * Issues a convergence request to the node group service on all peers and returns success
     * if all nodes confirm that they are converged (in terms of last membership update).
     *
     * It is expected the supplied host is listed as a peer in node group state
     */
    public static void checkConvergence(ServiceHost host, NodeGroupState ngs, Operation parentOp) {
        NodeState self = ngs.nodes.get(host.getId());
        if (self == null) {
            parentOp.fail(new IllegalStateException("Self node is required"));
            return;
        }

        if (self.membershipQuorum == 1 && ngs.nodes.size() == 1) {
            parentOp.complete();
            return;
        }
        checkConvergenceAcrossPeers(host, ngs, parentOp);
    }

    private static void checkConvergenceAcrossPeers(ServiceHost host, NodeGroupState ngs,
            Operation parentOp) {

        // send get requests to nodegroup services which are currently available to the supplied host
        // check uniqueness of membershipUpdateTimes

        JoinedCompletionHandler joinedCompletion = (ops, failures) -> {
            if (failures != null) {
                parentOp.fail(new IllegalStateException("At least one peer failed convergence"));
                return;
            }

            Map<URI, Long> membershipUpdateTimes = new HashMap<>();
            Set<Long> uniqueTimes = new HashSet<>();
            for (Operation peerOp : ops.values()) {
                if (!peerOp.hasBody()) {
                    membershipUpdateTimes.put(peerOp.getUri(), -1L);
                    uniqueTimes.add(-1L);
                    continue;
                }
                NodeGroupState rsp = peerOp.getBody(NodeGroupState.class);
                membershipUpdateTimes.put(peerOp.getUri(), rsp.membershipUpdateTimeMicros);
                uniqueTimes.add(rsp.membershipUpdateTimeMicros);
            }

            if (uniqueTimes.size() > 1) {
                String error = String.format("Membership times not converged: %s",
                        membershipUpdateTimes);
                parentOp.fail(new IllegalStateException(error));
                return;
            }

            parentOp.complete();
        };

        List<Operation> ops = new ArrayList<>();
        for (NodeState ns : ngs.nodes.values()) {
            if (NodeState.isUnAvailable(ns)) {
                continue;
            }

            long expirationMicros = Math.min(
                    Utils.fromNowMicrosUtc(ngs.config.peerRequestTimeoutMicros),
                    parentOp.getExpirationMicrosUtc());
            Operation peerOp = Operation.createGet(ns.groupReference)
                    .transferRefererFrom(parentOp)
                    .setExpiration(expirationMicros);
            if (host.getId().equals(ns.id)) {
                // optimization, use in process fast path for self host
                peerOp.setUri(UriUtils.buildUri(host.getUri(), ns.groupReference.getPath()));
            }
            ops.add(peerOp);
        }

        if (ops.isEmpty()) {
            parentOp.fail(new IllegalStateException("no available nodes"));
            return;
        }

        OperationJoin.create(ops).setCompletion(joinedCompletion).sendWith(host);
    }

    public static void checkConvergence(ServiceHost host, URI nodegroupReference, Operation parentOp) {
        Operation.createGet(nodegroupReference)
                .transferRefererFrom(parentOp)
                .setCompletion((o, t) -> {
                    if (t != null) {
                        parentOp.fail(t);
                        return;
                    }
                    NodeGroupState ngs = o.getBody(NodeGroupState.class);
                    checkConvergenceAcrossPeers(host, ngs, parentOp);
                }).sendWith(host);
    }

    /**
     * Given a node group state, looks at the time the node group state changed. If
     * the group was updated within a certain number of maintenance intervals, we
     * consider it unstable. The interval count can be set through a PATCH to
     * core/node-groups/<group>/config.
     *
     * See {@link NodeGroupService}
     */
    public static boolean isMembershipSettled(ServiceHost host, long maintIntervalMicros,
            NodeGroupState localState) {
        NodeState selfNode = localState.nodes.get(host.getId());
        if (selfNode == null) {
            return false;
        }

        if (localState.nodes.size() == 1 && selfNode.membershipQuorum == 1) {
            // single, stand-alone node, skip update time check
            return true;
        }

        long threshold = localState.localMembershipUpdateTimeMicros
                + localState.config.stableGroupMaintenanceIntervalCount
                        * maintIntervalMicros;

        if (Utils.getSystemNowMicrosUtc() - threshold < 0) {
            return false;
        }
        return true;
    }

    /**
     * Given a node group state, and the value of NodeState.membershipQuorum for the
     * local host, it determines if enough nodes are available for the group as a whole,
     * to be considered available
     */
    public static boolean hasMembershipQuorum(ServiceHost host, NodeGroupState ngs) {
        NodeState selfNode = ngs.nodes.get(host.getId());
        if (selfNode == null) {
            return false;
        }

        int availableNodeCount = 0;
        if (ngs.nodes.size() == 1) {
            availableNodeCount = 1;
        } else {
            for (NodeState ns : ngs.nodes.values()) {
                if (!NodeState.isAvailable(ns, selfNode.id, false)) {
                    continue;
                }
                availableNodeCount++;
            }
        }

        if (availableNodeCount < selfNode.membershipQuorum) {
            return false;
        }
        return true;
    }

    /**
     * Evaluates current node group state and returns true if requests should be process,
     * forwarded, etc
     * The conditions are:
     *
     * 1) The node group membership should have been stable (gossip did not produce
     * changes) for a specific period
     *
     * 2) The number of node group members in available stage is >= membershipQuorum
     */
    public static boolean isNodeGroupAvailable(ServiceHost host, NodeGroupState localState) {
        // we invoke the isMembershipSettled first because it has low overhead, does not walk all
        // nodes in the group
        if (NodeGroupUtils.isMembershipSettled(host, host
                .getMaintenanceIntervalMicros(), localState)
                && NodeGroupUtils.hasMembershipQuorum(host, localState)) {
            return true;
        }
        return false;
    }

    /**
     * Complete operation when target service for the given path becomes available in node group
     *
     * @see ServiceHost#registerForServiceAvailability(CompletionHandler, boolean, String...)
     * @see #checkServiceAvailability(CompletionHandler, ServiceHost, URI, String)
     */
    public static void registerForReplicatedServiceAvailability(ServiceHost host, Operation op,
            String servicePath, String nodeSelectorPath) {

        CompletionHandler ch = (o, e) -> {
            if (e != null) {

                if (op.getExpirationMicrosUtc() < Utils.getSystemNowMicrosUtc()) {
                    String msg = "Failed to check replicated service availability";
                    op.fail(new TimeoutException(msg));
                    return;
                }

                // service is not yet available, reschedule
                host.schedule(() -> {
                    registerForReplicatedServiceAvailability(host, op, servicePath, nodeSelectorPath);
                }, host.getMaintenanceIntervalMicros(), TimeUnit.MICROSECONDS);
                return;
            }
            op.complete();
        };

        host.checkReplicatedServiceAvailable(ch, servicePath, nodeSelectorPath);
    }

    /**
     * Convert {@link NodeGroupBroadcastResponse} to {@link NodeGroupBroadcastResult}
     */
    public static NodeGroupBroadcastResult toBroadcastResult(NodeGroupBroadcastResponse response) {
        NodeGroupBroadcastResult result = new NodeGroupBroadcastResult();

        result.totalNodeCount = response.nodeCount;
        result.availableNodeCount = response.availableNodeCount;
        result.unavailableNodeCount = response.nodeCount - response.availableNodeCount;
        result.membershipQuorum = response.membershipQuorum;


        // entry example: "http://127.0.0.1:8000" => "host1"
        Map<URI, String> hostIdByUrl = response.selectedNodes.entrySet().stream()
                .collect(toMap(entry -> {
                    URI uri = entry.getValue();
                    return URI.create(uri.toString().replace(uri.getPath(), ""));
                }, Map.Entry::getKey));

        // for successful responses
        for (Entry<URI, String> entry : response.jsonResponses.entrySet()) {
            PeerNodeResult singleResult = new PeerNodeResult();
            URI requestUri = entry.getKey();
            String json = entry.getValue();

            URI hostUri = URI.create(requestUri.toString().replace(requestUri.getPath(), ""));
            String hostId = hostIdByUrl.get(hostUri);
            URI nodeGroupUri = response.selectedNodes.get(hostId);

            singleResult.requestUri = requestUri;
            singleResult.hostId = hostId;
            singleResult.nodeGroupUri = nodeGroupUri;
            singleResult.json = json;
            singleResult.errorResponse = null;

            result.allResponses.add(singleResult);
            result.successResponses.add(singleResult);
        }

        // for failure responses
        for (Entry<URI, ServiceErrorResponse> entry : response.failures.entrySet()) {
            PeerNodeResult singleResult = new PeerNodeResult();
            URI requestUri = entry.getKey();
            ServiceErrorResponse errorResponse = entry.getValue();

            URI hostUri = URI.create(requestUri.toString().replace(requestUri.getPath(), ""));
            String hostId = hostIdByUrl.get(hostUri);
            URI nodeGroupUri = response.selectedNodes.get(hostId);

            singleResult.requestUri = requestUri;
            singleResult.hostId = hostId;
            singleResult.nodeGroupUri = nodeGroupUri;
            singleResult.json = null;
            singleResult.errorResponse = errorResponse;

            result.allResponses.add(singleResult);
            result.failureResponses.add(singleResult);
            result.failureErrorResponses.add(errorResponse);
        }

        return result;
    }

}