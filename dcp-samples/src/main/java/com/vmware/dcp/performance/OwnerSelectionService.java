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

package com.vmware.dcp.performance;

import com.vmware.dcp.common.FactoryService;
import com.vmware.dcp.common.Service;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.StatefulService;

import java.util.ArrayList;
import java.util.List;

public class OwnerSelectionService extends StatefulService {
    public static class OwnerSelectionFactoryService extends FactoryService {
        public static String SELF_LINK = PerfUtils.BENCH + "/owner-selected";
        public List<ServiceOption> caps = new ArrayList<>();

        public OwnerSelectionFactoryService(Class <? extends ServiceDocument> stateClass) {
            super(stateClass);
        }

        public static OwnerSelectionFactoryService create(Class<? extends ServiceDocument> stateClass) {
            OwnerSelectionFactoryService gfs = new OwnerSelectionFactoryService(stateClass);
            return gfs;
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new OwnerSelectionService(this.stateType);
        }
    }

    public OwnerSelectionService(Class <? extends ServiceDocument> stateClass) {
        super(stateClass);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }
}