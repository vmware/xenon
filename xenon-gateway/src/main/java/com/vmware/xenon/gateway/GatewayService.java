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
import java.net.URISyntaxException;
import java.util.EnumSet;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

/**
 * This class represents a data driven, highly-available Gateway service,
 * used to intercept and filter http requests for a set of backend nodes.
 * Filtering of requests is performed through a set of white-listed URI paths and
 * allowed http actions.
 *
 * Configuration policies for the GatewayService are stored using different
 * Stateful Replicated services. See: {@link GatewayConfigService} and
 * {@link GatewayPathService}. Users will interact with these services directly when
 * updating state for a specific gateway.
 *
 * For maximum through-put, the Gateway Service uses an in-memory cache for all
 * configuration policies {@link GatewayCache}. The cache is updated
 * asynchronously as soon as configuration is added/updated through the services
 * listed above. The cached state can be queried from the gateway service by making
 * a  HTTP GET request on the gateway-service self-link. This can be used to ensure
 * that the ingested configuration is now Active.
 */
public class GatewayService extends StatelessService {
    public static final String SELF_LINK = "/";

    private GatewayCache cache;
    private URI configHostUri;

    public GatewayService(URI configHostUri) {
        super(ServiceDocument.class);
        super.toggleOption(ServiceOption.URI_NAMESPACE_OWNER, true);
        this.configHostUri = configHostUri;
    }

    @Override
    public void handleStart(Operation startOp) {
        try {
            // Initialize the cache. Setting up subscriptions allows
            // the cache to stay uptodate as configuration is
            // updated.
            this.cache = GatewayCache.create(
                    getHost(), this.configHostUri, GatewayUriPaths.DEFAULT_CONFIG_PATH);

            this.cache.start((t) -> {
                if (t != null) {
                    startOp.fail(t);
                    return;
                }
                startOp.complete();
            });
        } catch (Throwable t) {
            startOp.fail(t);
        }
    }

    /**
     * The main gateway routine, called for each intercepted request.
     */
    @Override
    public void handleRequest(Operation op) {
        String path = op.getUri().getPath();

        // If the request is for the Gateway endpoint
        // use regular request handling flow.
        if (path.equals("") || path.equals(SELF_LINK)) {
            super.handleRequest(op);
            return;
        }

        // Fail the request if the gateway is marked as
        // UN-AVAILABLE.
        if (this.cache.getGatewayStatus() == GatewayStatus.UNAVAILABLE) {
            failOperation(op, Operation.STATUS_CODE_UNAVAILABLE,
                    "Gateway is currently unavailable. Please retry later.");
            return;
        }

        if (this.cache.filterRequests()) {
            // Check if the requested path exists in our allowed uris.
            EnumSet<Action> actions = this.cache.getSupportedActions(path);
            if (actions == null) {
                // If not, this could be a child-service request. Get
                // the parent path and check if exists in allowed uris.
                path = UriUtils.getParentPath(path);
                actions = this.cache.getSupportedActions(path);
                if (actions == null) {
                    failOperation(op, Operation.STATUS_CODE_NOT_FOUND,
                            "Requested path %s not found.", path);
                    return;
                }
            }

            // Check if the requested Action is allowed on the requested path.
            if (!actions.contains(op.getAction())) {
                failOperation(op, Operation.STATUS_CODE_BAD_METHOD,
                        "Requested verb %s not allowed on path %s.", op.getAction(), path);
                return;
            }
        }

        // Check if the Gateway has been PAUSED. If so, queue the operation.
        if (this.cache.getGatewayStatus() == GatewayStatus.PAUSED) {
            // TODO - Implement request queuing.
            failOperation(op, Operation.STATUS_CODE_UNAVAILABLE,
                    "Gateway is currently PAUSED. Please retry later.");
            return;
        }

        // Select one available node randomly. If there are no backend
        // nodes that are currently available, simply fail the request.
        URI nodeAddress = this.cache.getForwardingUri();
        if (nodeAddress == null) {
            failOperation(op, Operation.STATUS_CODE_UNAVAILABLE,
                    "Gateway is currently unavailable. Please retry later.");
            return;
        }

        // Forward the request to the selected backend node.
        Operation outboundOp = op.clone();
        outboundOp.setUri(createNewUri(nodeAddress, op.getUri()));
        outboundOp.forceRemote();
        outboundOp.setCompletion((o, e) -> {
            op.transferResponseHeadersFrom(o);
            op.setStatusCode(o.getStatusCode());
            op.setContentType(o.getContentType());
            op.setContentLength(o.getContentLength());
            op.setBodyNoCloning(o.getBodyRaw());
            if (e != null) {
                op.fail(e);
                return;
            }
            op.complete();
        });
        getHost().sendRequest(outboundOp);
    }

    /**
     * Called when a GET is issued on the self-link of the
     * GatewayService. This method returns the cached state of the
     * Gateway.
     */
    @Override
    public void handleGet(Operation op) {
        op.setBodyNoCloning(this.cache.getGatewayState());
        op.complete();
    }

    /**
     * DELETE on the GatewayService self-link are not-allowed.
     * This would otherwise cause the GatewayService to stop.
     */
    @Override
    public void handleDelete(Operation op) {
        if (!op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)) {
            failOperation(op, Operation.STATUS_CODE_BAD_METHOD,
                    "DELETE not supported on Gateway endpoint.");
            return;
        }
        op.complete();
    }

    /**
     * Called when the GatewayService is stopped by the ServiceHost.
     */
    @Override
    public void handleStop(Operation op) {
        this.cache.stop();
        op.complete();
    }

    private URI createNewUri(URI nodeAddress, URI opUri) {
        try {
            return new URI(
                    nodeAddress.getScheme(), opUri.getUserInfo(), nodeAddress.getHost(),
                    nodeAddress.getPort(), opUri.getPath(), opUri.getQuery(), opUri.getFragment());
        } catch (URISyntaxException x) {
            throw new IllegalArgumentException(x.getMessage(), x);
        }
    }

    private void failOperation(Operation op,
            int statusCode, String msgFormat, Object... args) {
        ServiceErrorResponse rsp = new ServiceErrorResponse();
        rsp.message = String.format(msgFormat, args);
        rsp.statusCode = statusCode;
        op.fail(statusCode, null, rsp);
    }
}
