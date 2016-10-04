/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import static java.util.stream.Collectors.toSet;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.Utils;

/**
 * Utility class that provides simple API for {@link NodeGroupBroadcastResponse}
 *
 * @see NodeGroupUtils#toBroadcastResult(NodeGroupBroadcastResponse)
 */
public class NodeGroupBroadcastResult {

    /**
     * Represent single broadcast response from each host
     */
    public static class PeerNodeResult {
        public URI requestUri;
        public String hostId;
        public URI nodeGroupUri;
        public String json;
        public ServiceErrorResponse errorResponse;

        public boolean isSuccess() {
            return this.json != null;
        }

        public boolean isFailure() {
            return this.errorResponse != null;
        }

        public <T> T castBodyTo(Class<T> bodyType) {
            return Utils.fromJson(this.json, bodyType);
        }
    }

    public long availableNodeCount;
    public long unavailableNodeCount;
    public long totalNodeCount;
    public long membershipQuorum;
    public Set<PeerNodeResult> allResponses = new HashSet<>();
    public Set<PeerNodeResult> successResponses = new HashSet<>();
    public Set<PeerNodeResult> failureResponses = new HashSet<>();
    public Set<ServiceErrorResponse> failureErrorResponses = new HashSet<>();


    public boolean hasSuccess() {
        return !this.successResponses.isEmpty();
    }

    public boolean hasFailure() {
        return !this.failureResponses.isEmpty();
    }

    public boolean isMajoritySuccess() {
        return this.membershipQuorum <= this.successResponses.size();
    }

    public boolean isMajorityFailure() {
        return this.membershipQuorum <= this.failureResponses.size();
    }

    public boolean isAllSuccess() {
        return this.failureResponses.isEmpty();
    }

    public boolean isAllFailure() {
        return this.successResponses.isEmpty();
    }

    public <T> Set<T> getSuccessesAs(Class<T> type) {
        return this.successResponses.stream()
                .map(singleResponse -> singleResponse.castBodyTo(type))
                .collect(toSet());
    }
}
