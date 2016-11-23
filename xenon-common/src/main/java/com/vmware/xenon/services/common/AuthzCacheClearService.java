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
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;

public class AuthzCacheClearService extends StatefulService {
    public static final String SELF_LINK = ServiceUriPaths.CORE_AUTHZ_CLEAR;

    public static class AuthzCacheClearServiceState extends ServiceDocument {
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String serviceLink;
    }

    public AuthzCacheClearService() {
        super(AuthzCacheClearServiceState.class);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void processCompletionStageUpdateAuthzArtifacts(Operation op) {
        if (getHost().getAuthorizationServiceUri() == null) {
            op.complete();
            return;
        }
        AuthzCacheClearServiceState state = getBody(op);
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
        Utils.mergeWithState(getStateDescription(),getState(patch), getBody(patch));
        patch.complete();
    }
}
