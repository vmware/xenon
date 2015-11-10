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

package com.vmware.dcp.common;

import java.util.logging.Level;

import com.vmware.dcp.services.common.QueryFilter;

/**
 * Query filter utility functions
 */
public class QueryFilterUtils {

    /**
     * Infrastructure use only.
     *
     * Decodes an operation body using the type defined on the corresponding service instance.
     * This is useful where a service's type is not known up front and a query filter
     * needs to be evaluated against its state.
     *
     * The service pointed to by the operation's URI path must be started on the host.
     */
    public static ServiceDocument getServiceState(Operation op, ServiceHost host) {
        Service s = host.findService(op.getUri().getPath());
        Class<? extends ServiceDocument> type = s.getStateType();
        if (type == null) {
            return null;
        }

        return op.getBody(type);
    }

    /**
     * Evaluates the given document state given the filter and an available service document
     * description cached by the service host.
     *
     * The service associated with the state must be started on the host.
     */
    public static boolean evaluate(QueryFilter filter, ServiceDocument state, ServiceHost host) {
        ServiceDocumentDescription sdd = host.buildDocumentDescription(state.documentSelfLink);
        if (sdd == null) {
            host.log(Level.WARNING, "Service %s not found", state.documentSelfLink);
            return false;
        }
        return filter.evaluate(state, sdd);
    }
}
