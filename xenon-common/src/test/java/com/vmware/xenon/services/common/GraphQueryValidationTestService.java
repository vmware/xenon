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
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;

public class GraphQueryValidationTestService extends StatefulService {

    public static final String FACTORY_LINK = "test/graph-query-targets";

    public static class NestedType {
        public String id;
        public long longValue;
        @ServiceDocument.PropertyOptions(usage = PropertyUsageOption.LINK)
        public String link;
    }

    public GraphQueryValidationTestService() {
        super(QueryValidationServiceState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handlePut(Operation put) {
        QueryValidationServiceState replacementState = put
                .getBody(QueryValidationServiceState.class);

        // PUT replaces entire state, so update the linked state
        setState(put, replacementState);
        put.setBody(null).complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        QueryValidationServiceState body = patch
                .getBody(QueryValidationServiceState.class);
        QueryValidationServiceState currentState = getState(patch);
        currentState.documentExpirationTimeMicros = body.documentExpirationTimeMicros;
        currentState.serviceLink = body.serviceLink;
        patch.setBody(null).complete();
    }

}