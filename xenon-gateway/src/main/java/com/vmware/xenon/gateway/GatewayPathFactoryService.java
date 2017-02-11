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

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.UriUtils;

/**
 * Factory for {@link GatewayPathService}
 */
public class GatewayPathFactoryService extends FactoryService {

    public static final String SELF_LINK = GatewayUriPaths.PATHS;

    public GatewayPathFactoryService() {
        super(GatewayPathService.State.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new GatewayPathService();
    }

    @Override
    protected String buildDefaultChildSelfLink(ServiceDocument document) {
        // A specific format for the self-link is used to guarantee
        // uniqueness of uri paths for a specific gateway.
        GatewayPathService.State state = (GatewayPathService.State)document;
        if (state.path != null) {
            return createSelfLinkFromState(state);
        }

        if (state.documentSelfLink != null) {
            return state.documentSelfLink;
        }

        return super.buildDefaultChildSelfLink();
    }

    public static final String createSelfLinkFromState(GatewayPathService.State state) {
        String uriPath = UriUtils.convertPathCharsFromLink(state.path);
        return UriUtils.buildUriPath(GatewayPathFactoryService.SELF_LINK, uriPath);
    }
}
