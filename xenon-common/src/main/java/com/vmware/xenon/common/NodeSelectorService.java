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

import java.net.URI;
import java.util.Collection;
import java.util.EnumSet;

import com.vmware.xenon.common.Operation.OperationOption;
import com.vmware.xenon.services.common.NodeState;

/**
 * Infrastructure use only. Service interface used by {@code ServiceHost} implementations to efficiently
 * dispatch operations to node selection services.
 *
 * This should be considered an internal API and is subject to change at any time.
 * It is not common pattern for services to implement anything other than the Service interface since
 * all interaction with a service, regardless of location should be through REST asynchronous operations
 */
public interface NodeSelectorService extends Service {
    public static final String STAT_NAME_QUEUED_REQUEST_COUNT = "queuedRequestCount";
    public static final String STAT_NAME_SYNCHRONIZATION_COUNT = "synchronizationCount";

    public static final OperationOption FORWARDING_OPERATION_OPTION = getOperationOption(
            "NodeSelectorService.FORWARDING_OPERATION_OPTION", OperationOption.CONNECTION_SHARING);

    public static final OperationOption REPLICATION_OPERATION_OPTION = getOperationOption(
            "NodeSelectorService.REPLICATION_OPERATION_OPTION", null);

    public static final int REPLICATION_TAG_CONNECTION_LIMIT = Integer.getInteger(
            Utils.PROPERTY_NAME_PREFIX
                    + "NodeSelectorService.REPLICATION_TAG_CONNECTION_LIMIT", 32);

    public static final int FORWARDING_TAG_CONNECTION_LIMIT = Integer.getInteger(
            Utils.PROPERTY_NAME_PREFIX
                    + "NodeSelectorService.FORWARDING_TAG_CONNECTION_LIMIT", 32);

    static OperationOption getOperationOption(String name, OperationOption defaultOpt) {
        String paramName = Utils.PROPERTY_NAME_PREFIX + name;
        String paramValue = System.getProperty(paramName);
        if (OperationOption.SEND_WITH_CALLBACK.name().equals(paramValue)) {
            return OperationOption.SEND_WITH_CALLBACK;
        } else if (OperationOption.CONNECTION_SHARING.name().equals(paramValue)) {
            return OperationOption.CONNECTION_SHARING;
        } else {
            return defaultOpt;
        }
    }

    /**
     * Request to select one or more nodes from the available nodes in the node group, and optionally
     * forward the request
     */
    public static class SelectAndForwardRequest {
        public static enum ForwardingOption {
            REPLICATE,
            BROADCAST,
            UNICAST,
            EXCLUDE_ENTRY_NODE
        }

        /**
         * Key used in the assignment scheme.
         */
        public String key;

        /**
         * Target service path for an assign and forward request. The link is used as the key if the
         * key is null
         */
        public String targetPath;

        /**
         * Target query path
         */
        public String targetQuery;

        /**
         * Infrastructure use only.
         *
         * Tracks operation associated with the selection request. Used internally when request
         * has to be queued.
         */
        public transient Operation associatedOp;

        public EnumSet<ForwardingOption> options;

        public EnumSet<ServiceOption> serviceOptions;
    }

    /**
     * Selection response
     */
    public class SelectOwnerResponse {
        public String key;
        public String ownerNodeId;
        public URI ownerNodeGroupReference;
        public boolean isLocalHostOwner;

        /**
         * Number of nodes eligible and available for selection
         */
        public int availableNodeCount;

        /**
         * All nodes eligible for the supplied key
         */
        public Collection<NodeState> selectedNodes;

        /**
         * Membership update time correlated with this response. This value is the
         * max between all times reported by the peers and should be used only for
         * relative comparisons
         */
        public long membershipUpdateTimeMicros;

        public static URI buildUriToOwner(SelectOwnerResponse rsp, String path, String query) {
            return UriUtils.buildUri(rsp.ownerNodeGroupReference.getScheme(),
                    rsp.ownerNodeGroupReference.getHost(), rsp.ownerNodeGroupReference.getPort(), path,
                    query);
        }

        public static URI buildUriToOwner(SelectOwnerResponse rsp, Operation op) {
            return UriUtils.buildUri(rsp.ownerNodeGroupReference.getScheme(),
                    rsp.ownerNodeGroupReference.getHost(), rsp.ownerNodeGroupReference.getPort(), op
                    .getUri().getPath(), op.getUri().getQuery());
        }
    }

    /**
     * Returns the node group path associated with this selector
     */
    String getNodeGroup();

    void selectAndForward(Operation op, SelectAndForwardRequest body);
}