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

package com.vmware.xenon.services.samples;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Simplest version of echo service: Records a message (PUT); returns it when asked (GET).
 */
public class SampleSimpleEchoService extends StatefulService {
    public static final String FACTORY_LINK = ServiceUriPaths.SAMPLES + "/echoes";

    public static Service createFactory() {
        return FactoryService.create(SampleSimpleEchoService.class);
    }

    public static class EchoServiceState extends ServiceDocument {
        // simplest possible state
        public String message;
    }

    public SampleSimpleEchoService() {
        super(EchoServiceState.class);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
    }
}