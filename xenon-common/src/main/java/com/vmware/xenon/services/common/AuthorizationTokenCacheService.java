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

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * This is a replicated, non-persistent service to manage the authorization token cache
 * that is maintained on every service host.
 * Any authz state update can impact the authz cache and require cached entries to be removed. This
 * is achieved by invoking one of the {@link AuthorizationCacheUtils} methods via overriding
 * {@link StatefulService#processCompletionStageUpdateAuthzArtifacts(Operation)}
 * The {@link AuthorizationCacheUtils} methods identify all user subjects that are impacted by
 * the authz change and invoke a PATCH operation per subject to the singleton instance of
 * {@link AuthorizationTokenCacheService} that is started up as part of bringing up
 * the {@link ServiceHost}
 * The PATCH operation in turn invokes {@link StatefulService#processCompletionStageUpdateAuthzArtifacts(Operation)}
 * on self on all nodes. This method calls out to the {@link AuthorizationContextService} with the
 * right pragma set to clear the authorization token cache on all service hosts.
 * The operation that triggered the authz update is not marked complete till the entire chain of operations
 * described above has finished
 */
public class AuthorizationTokenCacheService extends StatefulService {
    public static final String FACTORY_LINK = ServiceUriPaths.CORE_AUTHZ_TOKEN_CACHE;
    public static final String INSTANCE_LINK = UriUtils.buildUriPath(FACTORY_LINK, "instance");

    public static Service createFactory() {
        return FactoryService.create(AuthorizationTokenCacheService.class);
    }

    public static class AuthorizationTokenCacheServiceState extends ServiceDocument {
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String serviceLink;
    }

    public AuthorizationTokenCacheService() {
        super(AuthorizationTokenCacheServiceState.class);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void processCompletionStageUpdateAuthzArtifacts(Operation op) {
        if (getHost().getAuthorizationServiceUri() == null) {
            op.complete();
            return;
        }
        AuthorizationTokenCacheServiceState state = getBody(op);
        Operation postClearCacheRequest = Operation.createPost(this, ServiceUriPaths.CORE_AUTHZ_VERIFICATION)
                .setBody(AuthorizationCacheClearRequest.create(state.serviceLink))
                .setCompletion((clearOp, clearEx) -> {
                    if (clearEx != null) {
                        logSevere(clearEx);
                        op.fail(clearEx);
                        return;
                    }
                    op.complete();
                });
        System.out.println("CLEAR CACHE invoked for: " + state.serviceLink);
        postClearCacheRequest.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_CLEAR_AUTH_CACHE);
        setAuthorizationContext(postClearCacheRequest, getSystemAuthorizationContext());
        sendRequest(postClearCacheRequest);
    }

    @Override
    public void handlePatch(Operation patch) {
        if (!patch.hasBody()) {
            patch.fail(new IllegalArgumentException("body is required"));
            return;
        }
        AuthorizationTokenCacheServiceState state = getBody(patch);
        System.out.println("handle patch invoked for: " + state.serviceLink);
        Utils.mergeWithState(getStateDescription(),getState(patch), getBody(patch));
        patch.complete();
    }
}
