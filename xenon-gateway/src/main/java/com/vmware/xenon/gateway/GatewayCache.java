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

package com.vmware.xenon.gateway;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.logging.Level;

import com.google.gson.JsonObject;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Represents a Cache used to store configuration data for a
 * {@link GatewayService}. Also responsible for keeping the
 * cache uptodate.
 */
public class GatewayCache {

    public static final class State {
        public String gatewayId;
        public GatewayStatus status = GatewayStatus.UNAVAILABLE;
        public Map<String, Set<Service.Action>> paths = new ConcurrentSkipListMap<>();
        public Map<URI, NodeStatus> nodes = new ConcurrentSkipListMap<>();
    }

    private ServiceHost host;

    private State state;
    private List<URI> availableNodes = Collections
            .synchronizedList(new ArrayList<>());

    public GatewayCache(ServiceHost host, String gatewayId) {
        this.host = host;
        this.state = new State();
        this.state.gatewayId = gatewayId;
    }

    /**
     * Returns the cached State instance.
     */
    public GatewayCache.State getGatewayState() {
        return this.state;
    }

    /**
     * Returns the allowed verbs for the passed URI path.
     */
    public Set<Action> getPathVerbs(String path) {
        return this.state.paths.get(path);
    }

    /**
     * Returns the Gateway status.
     */
    public GatewayStatus getGatewayStatus() {
        return this.state.status;
    }

    /**
     * Randomly selects a node that has status {@link NodeStatus#AVAILABLE}.
     */
    public URI selectNextAvailableNode() {
        if (this.availableNodes.size() == 0) {
            return null;
        }

        ThreadLocalRandom random = ThreadLocalRandom.current();
        synchronized (this.state) {
            int i = random.nextInt(this.availableNodes.size());
            return this.availableNodes.get(i);
        }
    }

    /**
     * Creates a continuous query task to filter updates on the gateway
     * configuration services. A subscription is also created that subscribes
     * to the continuous query task.
     *
     * The continuous query task created here is a "local" query-task that is
     * dedicated for keeping the cache on the local node uptodate. This simplifies
     * the design considerably for dealing with node-group changes. Each
     * node as it starts, creates a continuous query on the local index. As
     * the configuration state gets propagated through replication or synchronization
     * the local cache gets updated as well.
     */
    public void setupSubscription(Consumer<Throwable> completionCallback) {
        try {
            QueryTask continuousQueryTask = createGatewayQueryTask();
            Operation.createPost(this.host, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                    .setBody(continuousQueryTask)
                    .setReferer(this.host.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.log(Level.SEVERE,
                                    "Failed to setup continuous query. Failure: %s", e.toString());
                            completionCallback.accept(e);
                            return;
                        }
                        QueryTask rsp = o.getBody(QueryTask.class);
                        startSubscription(completionCallback, rsp);
                    }).sendWith(this.host);
        } catch (Exception e) {
            this.host.log(Level.SEVERE, e.toString());
            completionCallback.accept(e);
        }
    }

    private void startSubscription(Consumer<Throwable> completionCallback, QueryTask queryTask) {
        try {
            // Create subscription using replay state to bootstrap the cache.
            ServiceSubscriptionState.ServiceSubscriber sr = ServiceSubscriptionState
                    .ServiceSubscriber.create(true);

            Operation subscribe = Operation
                    .createPost(this.host, queryTask.documentSelfLink)
                    .setReferer(this.host.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.log(Level.SEVERE,
                                    "Failed to subscribe to the continuous query. Failure: %s", e.toString());
                            completionCallback.accept(e);
                            return;
                        }
                        this.host.log(Level.INFO,
                                "Subscription started successfully for Gateway %s", this.state.gatewayId);
                        completionCallback.accept(null);
                    });
            this.host.startSubscriptionService(subscribe, handleConfigUpdates(), sr);
        } catch (Exception e) {
            this.host.log(Level.SEVERE, e.toString());
            completionCallback.accept(e);
        }
    }

    private QueryTask createGatewayQueryTask() {
        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(ConfigService.State.class, QueryTask.Query.Occurance.SHOULD_OCCUR)
                .addKindFieldClause(NodeService.State.class, QueryTask.Query.Occurance.SHOULD_OCCUR)
                .addKindFieldClause(PathService.State.class, QueryTask.Query.Occurance.SHOULD_OCCUR)
                .addFieldClause(ConfigService.GATEWAY_ID_FIELD_NAME, this.state.gatewayId)
                .build();

        EnumSet<QuerySpecification.QueryOption> queryOptions = EnumSet.of(
                QuerySpecification.QueryOption.EXPAND_CONTENT,
                QueryTask.QuerySpecification.QueryOption.CONTINUOUS);

        QueryTask queryTask = QueryTask.Builder
                .create()
                .addOptions(queryOptions)
                .setQuery(query)
                .build();
        queryTask.documentExpirationTimeMicros = Long.MAX_VALUE;
        return queryTask;
    }

    private Consumer<Operation> handleConfigUpdates() {
        return (notifyOp) -> {
            notifyOp.complete();
            QueryTask queryTask;
            try {
                queryTask = notifyOp.getBody(QueryTask.class);
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
            if (queryTask.results != null && queryTask.results.documents.size() > 0) {
                for (Map.Entry<String, Object> entry : queryTask.results.documents.entrySet()) {
                    boolean isJson = false;
                    String documentKind;
                    if (entry.getValue() instanceof JsonObject) {
                        isJson = true;
                        JsonObject obj = (JsonObject)entry.getValue();
                        documentKind = obj.get(ServiceDocument.FIELD_NAME_KIND).getAsString();
                    } else {
                        documentKind = ((ServiceDocument)entry.getValue()).documentKind;
                    }

                    if (documentKind.equals(ConfigService.State.KIND)) {
                        ConfigService.State obj = castOrDeserialize(
                                entry.getValue(), isJson, ConfigService.State.class);
                        handleConfigUpdate(obj);
                    } else if (documentKind.equals(PathService.State.KIND)) {
                        PathService.State obj = castOrDeserialize(
                                entry.getValue(), isJson, PathService.State.class);
                        handlePathUpdate(obj);
                    } else if (documentKind.equals(NodeService.State.KIND)) {
                        NodeService.State obj = castOrDeserialize(
                                entry.getValue(), isJson, NodeService.State.class);
                        handleNodeUpdate(obj);
                    } else {
                        this.host.log(Level.WARNING, "Unknown documentKind: %s", documentKind);
                    }
                }
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <T extends ServiceDocument> T castOrDeserialize(
            Object object, boolean requiresDeserialization, Class<T> clazz) {
        if (requiresDeserialization) {
            return Utils.fromJson(object, clazz);
        }
        return (T)object;
    }

    private void handleConfigUpdate(ConfigService.State config) {
        if (config.documentUpdateAction.equals(Service.Action.DELETE.toString())) {
            this.host.log(Level.SEVERE, "Gateway config was deleted. Gateway status updated to %s", GatewayStatus.UNAVAILABLE);
            this.state.status = GatewayStatus.UNAVAILABLE;
        } else {
            if (this.state.status != config.status) {
                this.state.status = config.status;
                this.host.log(Level.INFO, "Gateway status updated to %s", config.status);
            }
        }
    }

    private void handleNodeUpdate(NodeService.State node) {
        if (node.documentUpdateAction.equals(Service.Action.DELETE.toString())) {
            this.state.nodes.remove(node.reference);
            synchronized (this.state) {
                this.availableNodes.remove(node.reference);
            }
            this.host.log(Level.INFO, "Node %s removed", node.reference);
        } else {
            this.state.nodes.put(node.reference, node.status);
            if (node.status == NodeStatus.AVAILABLE) {
                synchronized (this.state) {
                    if (!this.availableNodes.contains(node.reference)) {
                        this.availableNodes.add(node.reference);
                    }
                }
            }
            this.host.log(Level.INFO, "Node %s added/updated with status %s",
                    node.reference, node.status);
        }
    }

    private void handlePathUpdate(PathService.State path) {
        if (path.documentUpdateAction.equals(Service.Action.DELETE.toString())) {
            this.state.paths.remove(path.path);
            this.host.log(Level.INFO, "Path %s removed", path.path);
        } else {
            Set<Service.Action> verbs = getPathVerbs(path);
            this.state.paths.put(path.path, verbs);
            this.host.log(Level.INFO, "Path %s added/ updated with allowed verbs: %s",
                    path.path, verbs);
        }
    }

    private Set<Service.Action> getPathVerbs(PathService.State path) {
        Set<Service.Action> verbs = path.verbs;
        if (path.allowAllVerbs) {
            verbs = new HashSet<>(Service.Action.values().length);
            for (Service.Action a : Service.Action.values()) {
                verbs.add(a);
            }
        }
        return verbs;
    }
}
