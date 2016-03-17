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

public class SimpleStatefulService extends StatefulService {
    public static String FACTORY_LINK = PerfUtils.BENCH + "/stateful";

    public static Service createFactory(Class<? extends ServiceDocument> stateClass) {
        return FactoryService.create(SimpleStatefulService.class, stateClass);
    }

    public SimpleStatefulService(Class <? extends ServiceDocument> stateClass) {
        super(stateClass);
    }
}