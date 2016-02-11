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

import java.util.ArrayList;
import java.util.List;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

public class FullCapService extends StatefulService {
    public static class FullCapFactoryService extends FactoryService {
        public static String SELF_LINK = PerfUtils.BENCH + "/full-caps";
        public List<ServiceOption> caps = new ArrayList<>();

        public FullCapFactoryService(Class <? extends ServiceDocument> stateClass) {
            super(stateClass);
        }

        public static FullCapFactoryService create(Class<? extends ServiceDocument> stateClass) {
            FullCapFactoryService gfs = new FullCapFactoryService(stateClass);
            return gfs;
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new FullCapService(this.stateType);
        }
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