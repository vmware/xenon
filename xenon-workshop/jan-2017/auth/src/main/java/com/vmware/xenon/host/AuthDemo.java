/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.host;

import java.util.EnumSet;

import com.vmware.xenon.common.AuthorizationSetupHelper;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.UserService.UserState;

public final class AuthDemo {

    private AuthDemo() {
    }

    public static void main(String[] args) throws Throwable {

        ServiceHost host = new ServiceHost() {
            @Override
            public ServiceHost start() throws Throwable {
                super.start();
                this.startDefaultCoreServicesSynchronously();

                setAuthorizationContext(this.getSystemAuthorizationContext());
                AuthorizationSetupHelper.create()
                        .setHost(this)
                        .setUserEmail("xenon-admin@vmware.com")
                        .setUserPassword("xenon")
                        .setUserSelfLink(Utils.computeHash("xenon-admin@vmware.com"))
                        .setRoleName("admin-role")
                        .setUserGroupName("admin-group")
                        .setUserGroupQuery(Query.Builder.create()
                                .addCollectionItemClause(UserState.FIELD_NAME_EMAIL, "xenon-admin@vmware.com")
                                .build())
                        .setResourceGroupName("admin-resource-group")
                        .setResourceQuery(Query.Builder.create()
                                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, "*", MatchType.WILDCARD).build())
                        .setVerbs(EnumSet.of(Action.GET, Action.POST, Action.PUT, Action.PATCH, Action.DELETE))
                        .setCompletion((authEx) -> {
                            if (authEx != null) {
                                return;
                            }
                        })
                        .start();
                return this;
            }
        };

        host.initialize(new String[]{"--isAuthorizationEnabled=true"});
        host.registerRuntimeShutdownHook();
        host.start();

    }
}
