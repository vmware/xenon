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

package com.vmware.xenon.workshop;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.RootNamespaceService;

public class StatefulDemo {

    public static void main(String[] stringArgs) throws Throwable {

        ServiceHost host = ServiceHost.create("--sandbox=/tmp/multinode/xenondb");
        host.start();
        host.startDefaultCoreServicesSynchronously();

        // A stateless service that enumerates all the
        // factory services started on the Service host.
        host.startService(new RootNamespaceService());

        // Starting the employee factory service.
        host.startFactory(new StatefulDemoEmployeeService());
    }

    private StatefulDemo() {
    }
}
