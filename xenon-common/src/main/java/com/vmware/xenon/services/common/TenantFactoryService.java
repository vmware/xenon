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

import java.util.UUID;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;

/**
 * Factory service for creating {@link TenantService} instances.
 */
public class TenantFactoryService extends FactoryService {
    public static final String SELF_LINK = ServiceUriPaths.CORE + "/tenants";

    public TenantFactoryService() {
        super(TenantService.TenantState.class);
    }

    @Override
    public void handlePost(Operation post) {
        if (!post.hasBody()) {
            post.fail(new IllegalArgumentException("body is required"));
            return;
        }

        TenantService.TenantState tenantState = post.getBody(TenantService.TenantState.class);
        if (tenantState.id == null || tenantState.id.isEmpty()) {
            tenantState.id = UUID.randomUUID().toString();
        }
        tenantState.documentSelfLink = tenantState.id;
        post.setBody(tenantState).complete();
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new TenantService();
    }
}
