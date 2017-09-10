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
import java.util.function.Consumer;

import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;

/**
 * Notification target service that listens to node group events and verifies subscription
 * is active on publisher
 */
public class ReliableSubscriptionService extends StatelessService {

    private Operation subscribeOp;
    private ServiceSubscriber subscribeRequest;
    private Consumer<Operation> consumer;
    private String peerNodeSelectorPath = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
    private URI nodeGroupCallbackUri;

    public static ReliableSubscriptionService create(Operation subscribe, ServiceSubscriber sr,
            Consumer<Operation> notificationConsumer) {
        if (subscribe == null) {
            throw new IllegalArgumentException("subscribe operation is required");
        }

        if (sr == null) {
            throw new IllegalArgumentException("subscribe request is required");
        }

        if (notificationConsumer == null) {
            throw new IllegalArgumentException("notificationConsumer is required");
        }
        ReliableSubscriptionService rss = new ReliableSubscriptionService(subscribe, sr,
                notificationConsumer);
        return rss;
    }

    private ReliableSubscriptionService(Operation subscribeOp, ServiceSubscriber sr,
            Consumer<Operation> notificationConsumer) {
        // cache the subscribe operation since we will clone and re-use if we need to
        // re-subscribe. It contains the appropriate authorization context
        this.subscribeOp = subscribeOp.clone();
        // cache the request details
        this.subscribeRequest = Utils.clone(sr);
        this.consumer = notificationConsumer;
    }

    @Override
    public String getPeerNodeSelectorPath() {
        return this.peerNodeSelectorPath;
    }

    @Override
    public void setPeerNodeSelectorPath(String path) {
        this.peerNodeSelectorPath = path;
    }

    @Override
    public void handleStart(Operation startPost) {
        this.subscribeRequest.reference = UriUtils.buildPublicUri(getHost(), getSelfLink());

        // we subscribe to the node group associated with our instance. The client should
        // set the node selector on this service, to match the one on the publisher
        sendRequest(Operation.createGet(this, getPeerNodeSelectorPath()).setCompletion(
                (o, e) -> {
                    if (e != null) {
                        startPost.fail(e);
                        return;
                    }

                    NodeSelectorState nss = o.getBody(NodeSelectorState.class);
                    URI callbackUri = getHost().startSubscriptionService(
                            Operation.createPost(this, nss.nodeGroupLink).setReferer(getUri()),
                            this::handleNodeGroupNotification);

                    this.nodeGroupCallbackUri = callbackUri;
                    startPost.complete();
                }));
    }

    @Override
    public void handleStop(Operation op) {
        // Delete the node group subscription which was created at handleStart.
        sendRequest(Operation.createGet(this, getPeerNodeSelectorPath()).setCompletion(
                (o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }

                    NodeSelectorState nss = o.getBody(NodeSelectorState.class);
                    Operation delete = Operation.createDelete(this, nss.nodeGroupLink).setReferer(getUri());
                    getHost().stopSubscriptionService(delete, this.nodeGroupCallbackUri);
                    op.complete();
                }));
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handleRequest(Operation op) {
        if (!op.isNotification()) {
            super.handleRequest(op);
            return;
        }
        this.consumer.accept(op);
    }

    private void handleNodeGroupNotification(Operation notifyOp) {
        NodeGroupState ngs = notifyOp.getBody(NodeGroupState.class);
        notifyOp.complete();
        if (ngs.nodes == null || ngs.nodes.isEmpty()) {
            return;
        }

        if (getHost().isStopping()) {
            return;
        }

        boolean isConverged = true;
        URI healthyPeerUri = null;
        // verify all nodes have converged to either available or unAvailable
        for (NodeState ns : ngs.nodes.values()) {
            boolean isAvailable = NodeState.isAvailable(ns, getHost().getId(), false);
            if (isAvailable) {
                healthyPeerUri = ns.groupReference;
            }
            if (isAvailable || NodeState.isUnAvailable(ns)) {
                continue;
            }
            isConverged = false;
            break;
        }

        if (!isConverged) {
            logInfo("group update notification but not group not converged");
            return;
        }

        // Change the URI to a healthy peer. The assumption is that the publisher service is
        // on of the nodes (possibly even this node) and if there is failure, DCP will route
        // to the new owner. But we should NOT use the original URI, since if its node fails,
        // all future requests will go nowhere
        if (!this.subscribeOp.getUri().getPath()
                .endsWith(ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS)) {
            this.subscribeOp.setUri(UriUtils.buildUri(healthyPeerUri,
                    this.subscribeOp.getUri().getPath(),
                    ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS));
        } else {
            this.subscribeOp.setUri(UriUtils.buildUri(healthyPeerUri,
                    this.subscribeOp.getUri().getPath()));
        }

        checkAndReSubscribe();
    }

    private void checkAndReSubscribe() {
        if (getHost().isStopping()) {
            return;
        }

        Operation getSubscriptions = this.subscribeOp.clone().setAction(Action.GET);
        sendRequest(getSubscriptions.setCompletion((o, e) -> {
            if (e != null) {
                selfDeleteDueToFailure(o, e);
                return;
            }

            resubscribe(o);
        }));
    }

    private void resubscribe(Operation o) {
        if (getHost().isStopping()) {
            return;
        }

        // verify we are still included in the publisher's subscriptions
        ServiceSubscriptionState rsp = o.getBody(ServiceSubscriptionState.class);
        for (ServiceSubscriber item : rsp.subscribers.values()) {
            if (item.reference != null && item.reference.getPath().equals(getSelfLink())) {
                // we found our subscription, all is good.
                return;
            }
        }

        logWarning("Subscription missing from %s, resubscribing", o.getUri());

        Operation reSubscribe = this.subscribeOp.clone()
                .setBody(this.subscribeRequest)
                .setCompletion((subOp, subE) -> {
                    if (subE != null) {
                        selfDeleteDueToFailure(subOp, subE);
                    }
                });
        sendRequest(reSubscribe);
    }

    private void selfDeleteDueToFailure(Operation o, Throwable e) {
        if (getHost().isStopping()) {
            return;
        }
        logSevere("%s to %s failed with %s :", o.getAction(), o.getUri(), e.toString());
        // self DELETE. The client, if they implemented handleRequest on the service instance
        // will be able to tell something went wrong
        sendRequest(Operation.createDelete(getUri())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NOTIFICATION)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SKIPPED_NOTIFICATIONS));
    }
}
