/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.metrics;

import java.util.Arrays;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

import com.vmware.xenon.common.Service.Action;

final class PerActionAndStatusCodeLookup {

    public static class Entry {
        final Counter.Child counter;
        final Summary.Child responseTimeMillis;

        public Entry(Counter.Child counter, Summary.Child responseTimeMillis) {
            this.counter = counter;
            this.responseTimeMillis = responseTimeMillis;
        }
    }

    static final Action[] COMMON_ACTIONS = {
            Action.GET,
            Action.POST,
            Action.PUT,
            Action.DELETE,
            Action.PATCH
    };

    static final int[] COMMON_STATUS_CODES = {
            200, // OK
            201, // Created
            202, // Accepted

            301, // Moved Permanently
            302, // Found
            304, // Not Modified (RFC 7232)

            400, // Bad Request
            401, // Unauthorized (RFC 7235)
            403, // Forbidden
            404, // Not Found
            409, // Conflict

            500, // Internal Server Error
            503, // Service Unavailable
    };

    private final Entry[] entries;

    public PerActionAndStatusCodeLookup(String selfLink, HostHttpCollector collector) {
        this.entries = new Entry[COMMON_ACTIONS.length * COMMON_STATUS_CODES.length];

        for (Action action : COMMON_ACTIONS) {
            for (int statusCode : COMMON_STATUS_CODES) {
                int index = index(action, statusCode);
                this.entries[index] = makeEntry(collector, selfLink, action, statusCode);
            }
        }
    }

    private int index(Action action, int statusCode) {
        // simplest perfect minimal hashing
        int i = Arrays.binarySearch(COMMON_STATUS_CODES, statusCode);
        if (i < 0) {
            return -1;
        }
        if (action == Action.OPTIONS) {
            return -1;
        }

        return action.ordinal() * COMMON_STATUS_CODES.length + i;
    }

    static int key(Action action, int statusCode) {
        return statusCode + action.ordinal() * 10;
    }

    private Entry makeEntry(HostHttpCollector collector, String selfLink, Action action, int statusCode) {
        return new Entry(
                collector.requestCount(action, selfLink, statusCode),
                collector.responseTimeMillis(action, selfLink, statusCode)
        );
    }

    public Entry get(Action action, int statusCode) {
        int index = index(action, statusCode);
        if (index < 0 || index > this.entries.length) {
            return null;
        }
        return this.entries[index];
    }
}
