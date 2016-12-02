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
import com.vmware.xenon.services.common.UserGroupService;

/**
 * Builder to build #{link UserGroupState}
 */
public class UserGroupStateBuilder {

    private UserGroupStateBuilder() {
    }

    public static Builder userGroupState() {
        return new Builder();
    }

    public static class Builder {
        private UserGroupService.UserGroupState userGroupState = new UserGroupService.UserGroupState();

        public Builder withQuery(QueryTask.Query query) {
            this.userGroupState.query = query;
            return this;
        }

        public Builder withUserGroupSelfLink(String userGroupSelfLink) {
            this.userGroupState.documentSelfLink = userGroupSelfLink;
            return this;
        }

        public UserGroupService.UserGroupState build() {
            return this.userGroupState;
        }
    }
}
