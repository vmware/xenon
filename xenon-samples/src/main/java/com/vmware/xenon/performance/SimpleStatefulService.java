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

public class SimpleStatefulService extends StatefulService {
    public static class SimpleStatefulFactoryService extends FactoryService {
        public static String SELF_LINK = PerfUtils.BENCH + "/stateful";
        public List<ServiceOption> caps = new ArrayList<>();

        public SimpleStatefulFactoryService(Class <? extends ServiceDocument> stateClass) {
            super(stateClass);
        }

        public static SimpleStatefulFactoryService create(Class<? extends ServiceDocument> stateClass) {
            SimpleStatefulFactoryService gfs = new SimpleStatefulFactoryService(stateClass);
            return gfs;
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new SimpleStatefulService(this.stateType);
        }
    }

    public SimpleStatefulService(Class <? extends ServiceDocument> stateClass) {
        super(stateClass);
    }
}