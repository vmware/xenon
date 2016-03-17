/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.performance;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

public class FullCapService extends StatefulService {
    public static String FACTORY_LINK = PerfUtils.BENCH + "/full-caps";

    public static Service createFactory(Class<? extends ServiceDocument> stateClass) {
        return FactoryService.create(FullCapService.class, stateClass);
    }

    public FullCapService(Class <? extends ServiceDocument> stateClass) {
        super(stateClass);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
        toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
    }
}