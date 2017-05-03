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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

/**
 * This is a local, non-persistent service to manage the authorization token cache
 * that is maintained on every service host.
 * A PATCH to this service calls out to the {@link AuthorizationContextService} with the
 * right pragma set to clear the authorization token cache on the local service host.
 */
public class AuthorizationTokenCacheService extends StatefulService {
    public static final String SELF_LINK = ServiceUriPaths.CORE_AUTHZ_TOKEN_CACHE;

    public static class AuthorizationTokenCacheServiceState extends ServiceDocument {
        public String serviceLink;
    }

    public AuthorizationTokenCacheService() {
        super(AuthorizationTokenCacheServiceState.class);
    }

    @Override
    public void handlePatch(Operation patch) {
        AuthorizationTokenCacheServiceState state = getBody(patch);
        Operation postClearCacheRequest = Operation.createPost(getHost().getAuthorizationServiceUri())
                .setBody(AuthorizationCacheClearRequest.create(state.serviceLink))
                .setCompletion((clearOp, clearEx) -> {
                    if (clearEx != null) {
                        logSevere(clearEx);
                        patch.fail(clearEx);
                        return;
                    }
                    patch.complete();
                });
        postClearCacheRequest.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_CLEAR_AUTH_CACHE);
        setAuthorizationContext(postClearCacheRequest, getSystemAuthorizationContext());
        sendRequest(postClearCacheRequest);
    }
}
