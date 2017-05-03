/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenonlabs;

import static com.vmware.xenonlabs.UpgradeDemoEmployeeService.Employee;

import java.util.HashMap;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.MigrationTaskService;

public class UpgradeDemoEmployeeServiceTransformService extends StatelessService {
    public static final String SELF_LINK = "/quickstart/employees-transform";

    @Override
    public void handlePost(Operation postOperation) {
        MigrationTaskService.TransformRequest request = postOperation.getBody(MigrationTaskService.TransformRequest.class);
        Employee state = Utils.fromJson(request.originalDocument, Employee.class);
        logInfo("Running transformation on [documentSelfLink: %s]", state.documentSelfLink);

        if (state.location == null) {
            state.location = "Palo Alto, CA";
            logInfo("Defaulted 'location' of %s to: %s", state.documentSelfLink, state.location);
        }

        UpgradeDemoEmployeeService.validateManagerLink(state, this, (e) -> {
            if (e != null) {
                logWarning("Problem when migrating: %s%n[managerLink=%s] does not exist. Will default it to 'null'",
                        state, state.managerLink);
                state.managerLink = null;
            }

            MigrationTaskService.TransformResponse response = new MigrationTaskService.TransformResponse();
            response.destinationLinks = new HashMap<>();
            response.destinationLinks.put(Utils.toJson(state), request.destinationLink);
            postOperation.setBody(response).complete();
        });
    }
}
