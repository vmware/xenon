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

package com.vmware.xenon.services.common;

import java.util.EnumSet;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.services.common.ReplicationTestService.ReplicationTestServiceState;

public class ReplicationFactoryTestService extends FactoryService {
    public static String OWNER_SELECTION_SELF_LINK = ServiceUriPaths.CORE
            + "/test/replication-instances";

    public static String STRICT_SELF_LINK = ServiceUriPaths.CORE
            + "/test/strict-replication-instances";

    public static String SIMPLE_REPL_SELF_LINK = ServiceUriPaths.CORE
            + "/test/simple-replication-instances";

    private static EnumSet<ServiceOption> OWNER_SELECTION_OPTIONS = EnumSet.of(
            ServiceOption.REPLICATION, ServiceOption.PERSISTENCE,
            ServiceOption.INSTRUMENTATION, ServiceOption.OWNER_SELECTION);
    private static EnumSet<ServiceOption> STRICT_UPDATE_OPTIONS = EnumSet.of(
            ServiceOption.REPLICATION, ServiceOption.PERSISTENCE, ServiceOption.OWNER_SELECTION,
            ServiceOption.STRICT_UPDATE_CHECKING);
    private static EnumSet<ServiceOption> REPLICATION_OPTIONS = EnumSet.of(
            ServiceOption.REPLICATION, ServiceOption.PERSISTENCE);

    public ReplicationFactoryTestService() {
        super(ReplicationTestServiceState.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        ReplicationTestService r = new ReplicationTestService();
        EnumSet<ServiceOption> options = null;
        if (getSelfLink().endsWith(SIMPLE_REPL_SELF_LINK)) {
            options = REPLICATION_OPTIONS;
        } else if (getSelfLink().endsWith(STRICT_SELF_LINK)) {
            options = STRICT_UPDATE_OPTIONS;
        } else {
            options = OWNER_SELECTION_OPTIONS;
        }
        for (ServiceOption c : options) {
            r.toggleOption(c, true);
        }
        r.toggleOption(ServiceOption.INSTRUMENTATION, true);
        return r;
    }
}
