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

import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest.ForwardingOption;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

public class QueryPageForwardingService extends StatelessService {

    public static final String SELF_LINK = ServiceUriPaths.CORE_QUERY_PAGE_FORWARDING;

    private NodeSelectorService selector;

    private static EnumSet<ForwardingOption> unicastOption = EnumSet.of(ForwardingOption.UNICAST);

    public QueryPageForwardingService(NodeSelectorService selector) {
        super();
        toggleOption(ServiceOption.CORE, true);
        this.selector = selector;
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handleRequest(Operation op) {
        Map<String, String> params = UriUtils.parseUriQueryParams(op.getUri());
        String peer = params.get(UriUtils.FORWARDING_URI_PARAM_NAME_PEER);
        if (peer == null) {
            if (params.isEmpty() && op.getAction() == Action.DELETE) {
                super.handleRequest(op);
                return;
            }
            op.fail(new IllegalArgumentException("peer uri parameter is required"));
            return;
        }

        String path = params.get(UriUtils.FORWARDING_URI_PARAM_NAME_PATH);
        if (path == null) {
            op.fail(new IllegalArgumentException("path uri parameter is required"));
            return;
        } else if (path.contains("/") || path.contains("..")) {
            op.fail(new IllegalArgumentException("path uri parameter is invalid"));
            return;
        } else if (path.equals(ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING)) {
            op.fail(new IllegalArgumentException("path uri parameter cannot be self"));
            return;
        }

        SelectAndForwardRequest body = new SelectAndForwardRequest();
        body.key = peer;
        body.targetPath = UriUtils.buildUriPath(ServiceUriPaths.CORE_QUERY_PAGE, path);
        body.targetQuery = null;
        body.options = unicastOption;
        this.selector.selectAndForward(op, body);
    }
}
