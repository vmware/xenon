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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Represents a Cache used to store configuration state of
 * the {@link GatewayService}. Also responsible for keeping the
 * cache up-to-date.
 */
public class GatewayCache {

    private static final EnumSet<Action> ALL_ACTIONS = EnumSet.allOf(Action.class);

    public static final class CachedState {
        public GatewayStatus status = GatewayStatus.UNAVAILABLE;
        public URI forwardingUri;
        public boolean filterRequests;
        public Map<String, EnumSet<Service.Action>> paths = new HashMap<>();
    }

    private ServiceHost host;
    private URI configHostUri;
    private String configSelfLink;

    private CachedState cachedState;

    private GatewayCache(ServiceHost host, URI configHostUri, String configSelfLink) {
        this.host = host;
        this.configHostUri = configHostUri;
        this.configSelfLink = configSelfLink;
        this.cachedState = new CachedState();
    }

    /**
     * Constructs a GatewayCache instance and returns it.
     */
    public static GatewayCache create(ServiceHost host, URI configHostUri, String configSelfLink) {
        return new GatewayCache(host, configHostUri, configSelfLink);
    }

    /**
     * Returns the cached State instance.
     */
    public CachedState getGatewayState() {
        synchronized (this.cachedState) {
            // To cached state object could be getting
            // updated concurrently. To avoid unexpected
            // races, the cloned object is returned here.
            return Utils.clone(this.cachedState);
        }
    }

    /**
     * Returns the allowed actions for the passed URI path.
     */
    public EnumSet<Action> getSupportedActions(String path) {
        synchronized (this.cachedState) {
            return this.cachedState.paths.get(path);
        }
    }

    /**
     * Returns the Gateway status.
     */
    public GatewayStatus getGatewayStatus() {
        synchronized (this.cachedState) {
            return this.cachedState.status;
        }
    }

    /**
     * Returns the forwarding URI.
     */
    public URI getForwardingUri() {
        synchronized (this.cachedState) {
            return this.cachedState.forwardingUri;
        }
    }

    /**
     * Returns true if path filtering should be
     * skipped by the GatewayService. Otherwise
     * returns false.
     */
    public boolean filterRequests() {
        synchronized (this.cachedState) {
            return this.cachedState.filterRequests;
        }
    }

    /**
     * Starts the cache instance by creating a continuous query task
     * to listen for configuration updates.
     *
     * The continuous query task created here is a "local" query-task that is
     * dedicated for keeping the cache on the local node uptodate. This simplifies
     * the design considerably for dealing with node-group changes. Each
     * node as it starts, creates a continuous query on the local index. As
     * the configuration state gets propagated through replication or synchronization
     * the local cache gets updated as well.
     */
    public void start(Consumer<Throwable> completionCallback) {
        try {
            QueryTask continuousQueryTask = createGatewayQueryTask();
            URI queryTaskUri = UriUtils.buildUri(
                    this.configHostUri, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
            Operation.createPost(queryTaskUri)
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
                        createSubscription(completionCallback, rsp);
                    }).sendWith(this.host);

        } catch (Exception e) {
            this.host.log(Level.SEVERE, e.toString());
            completionCallback.accept(e);
        }
    }

    /**
     * Stops the cache instance.
     */
    public void stop() {
        // No-op
    }

    private void createSubscription(Consumer<Throwable> completionCallback, QueryTask queryTask) {
        try {
            // Create subscription using replay state to bootstrap the cache.
            ServiceSubscriptionState.ServiceSubscriber sr = ServiceSubscriptionState
                    .ServiceSubscriber.create(true);

            Operation subscribe = Operation
                    .createPost(UriUtils.buildUri(this.configHostUri, queryTask.documentSelfLink))
                    .setReferer(this.host.getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.log(Level.SEVERE,
                                    "Failed to subscribe to the continuous query. Failure: %s", e.toString());
                            completionCallback.accept(e);
                            return;
                        }
                        this.host.log(Level.INFO,
                                "Subscription started successfully");
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
                .addKindFieldClause(GatewayConfigService.State.class, QueryTask.Query.Occurance.SHOULD_OCCUR)
                .addKindFieldClause(GatewayPathService.State.class, QueryTask.Query.Occurance.SHOULD_OCCUR)
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
                    String documentKind = Utils
                            .fromJson(entry.getValue(), ServiceDocument.class)
                            .documentKind;

                    if (documentKind.equals(GatewayConfigService.State.KIND)) {
                        GatewayConfigService.State obj = Utils.fromJson(entry.getValue(),
                                GatewayConfigService.State.class);
                        handleConfigUpdate(obj);
                    } else if (documentKind.equals(GatewayPathService.State.KIND)) {
                        GatewayPathService.State obj = Utils.fromJson(entry.getValue(),
                                GatewayPathService.State.class);
                        handlePathUpdate(obj);
                    } else {
                        this.host.log(Level.WARNING, "Unknown documentKind: %s", documentKind);
                    }
                }
            }
        };
    }

    private void handleConfigUpdate(GatewayConfigService.State config) {
        if (!config.documentSelfLink.equals(this.configSelfLink)) {
            return;
        }
        if (config.documentUpdateAction.equals(Service.Action.DELETE.toString())) {
            synchronized (this.cachedState) {
                this.cachedState.status = GatewayStatus.UNAVAILABLE;
                this.cachedState.filterRequests = true;
                this.cachedState.forwardingUri = null;
            }
            this.host.log(Level.SEVERE,
                    "Gateway config was deleted. Gateway status updated to %s",
                    GatewayStatus.UNAVAILABLE);
        } else {
            synchronized (this.cachedState) {
                this.cachedState.status = config.status != null ? config.status : GatewayStatus.UNAVAILABLE;
                this.cachedState.filterRequests = config.filterRequests != null ? config.filterRequests : true;
                this.cachedState.forwardingUri = config.forwardingUri;
            }
            this.host.log(Level.INFO, "Gateway status updated to %s", config.status + "/" + config.forwardingUri);
        }
    }

    private void handlePathUpdate(GatewayPathService.State path) {
        if (path.documentUpdateAction.equals(Service.Action.DELETE.toString())) {
            synchronized (this.cachedState) {
                this.cachedState.paths.remove(path.path);
            }
            this.host.log(Level.INFO, "Path %s removed", path.path);
        } else {
            EnumSet<Action> actions = (path.actions == null || path.actions.isEmpty())
                    ? ALL_ACTIONS : path.actions;
            synchronized (this.cachedState) {
                this.cachedState.paths.put(path.path, actions);
            }
            this.host.log(Level.INFO, "Path %s added/updated with allowed actions: %s",
                    path.path, actions);
        }
    }
}
