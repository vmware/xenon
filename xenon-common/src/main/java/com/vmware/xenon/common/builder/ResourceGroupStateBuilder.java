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

package com.vmware.xenon.common.builder;

import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ResourceGroupService;

/**
 * Builder to build #{link resourceGroupState}
 */
public class ResourceGroupStateBuilder {

    private ResourceGroupStateBuilder() {
    }

    public static Builder resourceGroupState() {
        return new Builder();
    }

    public static class Builder {
        private ResourceGroupService.ResourceGroupState resourceGroupState = new ResourceGroupService.ResourceGroupState();

        public Builder withQuery(QueryTask.Query query) {
            this.resourceGroupState.query = query;
            return this;
        }

        public Builder withUserGroupSelfLink(String userGroupSelfLink) {
            this.resourceGroupState.documentSelfLink = userGroupSelfLink;
            return this;
        }

        public ResourceGroupService.ResourceGroupState build() {
            return this.resourceGroupState;
        }
    }
}
