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

import java.util.EnumSet;
import java.util.Map;

import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest.ForwardingOption;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

/**
 * State-less utility service that forwards requests to nodes in the node group. It is not possible
 * to target this utility service directly, since it will forward all requests
 */
public class NodeSelectorForwardingService extends StatelessService {
    private ConsistentHashingNodeSelectorService parent;

    public NodeSelectorForwardingService(ConsistentHashingNodeSelectorService parent) {
        super(ServiceDocument.class);
        super.toggleOption(ServiceOption.UTILITY, true);
        this.parent = parent;
        super.setSelfLink(UriUtils.buildUriPath(this.parent.getSelfLink(),
                ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING));
    }

    @Override
    public void authorizeRequest(Operation op) {
        // authorization will be applied on the target we are forwarding to
        op.complete();
    }

    @Override
    public void handleRequest(Operation op) {
        Map<String, String> params = UriUtils.parseUriQueryParams(op.getUri());
        String link = params.get(UriUtils.FORWARDING_URI_PARAM_NAME_PATH);
        if (link == null) {
            if (params.isEmpty() && op.getAction() == Action.DELETE) {
                super.handleRequest(op);
                return;
            }
            op.fail(new IllegalArgumentException("link uri parameter is required"));
            return;
        }

        String key = params.get(UriUtils.FORWARDING_URI_PARAM_NAME_KEY);

        if (link.equals(getSelfLink())) {
            op.fail(new IllegalArgumentException("link can not be this service"));
            return;
        }

        String destination = params.get(UriUtils.FORWARDING_URI_PARAM_NAME_TARGET);
        if (destination == null) {
            op.fail(new IllegalArgumentException("destination uri parameter is required"));
            return;
        }
        String query = params.get(UriUtils.FORWARDING_URI_PARAM_NAME_QUERY);

        SelectAndForwardRequest body = new SelectAndForwardRequest();
        body.key = key;
        body.targetPath = link;
        body.targetQuery = query;
        body.options = EnumSet.noneOf(ForwardingOption.class);
        if (destination.equals(UriUtils.ForwardingTarget.ALL.toString())) {
            body.options.add(ForwardingOption.BROADCAST);
        } else {
            body.options.add(ForwardingOption.UNICAST);
        }

        if (destination.equals(UriUtils.ForwardingTarget.PEER_ID.toString())) {
            String peerId = params.get(UriUtils.FORWARDING_URI_PARAM_NAME_PEER);
            if (peerId == null) {
                op.fail(new IllegalArgumentException("peer uri parameter is required"));
                return;
            }
            body.key = peerId;
        }

        this.parent.selectAndForward(op, body);
    }

}
