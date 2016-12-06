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

import java.util.Set;

import com.vmware.xenon.common.Service;
import com.vmware.xenon.services.common.RoleService;

/**
 * Builder to build #{link RoleState}
 */
public class RoleStateBuilder {

    private RoleStateBuilder() {
    }

    public static Builder roleState() {
        return new Builder();
    }

    public static class Builder {
        private RoleService.RoleState roleState = new RoleService.RoleState();

        public Builder withUserGroupLink(String userGroupLink) {
            this.roleState.userGroupLink = userGroupLink;
            return this;
        }

        public Builder withResourceGroupSelfLink(String resourceGroupSelfLink) {
            this.roleState.resourceGroupLink = resourceGroupSelfLink;
            return this;
        }

        public Builder withVerbs(Set<Service.Action> verbs) {
            this.roleState.verbs = verbs;
            return this;
        }

        public Builder withPolicy(RoleService.Policy policy) {
            this.roleState.policy = policy;
            return this;
        }

        public Builder withRoleSelfLink(String roleSelfLink) {
            this.roleState.documentSelfLink = roleSelfLink;
            return this;
        }

        public RoleService.RoleState build() {
            return this.roleState;
        }
    }
}
