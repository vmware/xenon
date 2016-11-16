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

package com.vmware.xenon.common;

import java.util.logging.Level;

import com.vmware.xenon.services.common.QueryFilter;

/**
 * Query filter utility functions
 */
public final class QueryFilterUtils {

    private QueryFilterUtils() {

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

    /**
     * Evaluates the given document state given the filter
     */
    public static boolean evaluate(QueryFilter filter, ServiceDocument state) {
        ServiceDocumentDescription sdd = ServiceDocumentDescription.Builder.create().buildDescription(state.getClass());
        return filter.evaluate(state, sdd);
    }
}
