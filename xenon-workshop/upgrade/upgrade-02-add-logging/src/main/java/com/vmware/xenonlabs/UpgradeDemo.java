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

import java.util.logging.Level;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.MigrationTaskService;
import com.vmware.xenon.services.common.RootNamespaceService;

/**
 * Stand alone process entry point
 */
public class UpgradeDemo {

    static ServiceHost createAndStartHost(String... args) throws Throwable {
        ServiceHost host = ServiceHost.create(args);
        host.start();
        host.startDefaultCoreServicesSynchronously();
        host.startService(new RootNamespaceService());
        host.startFactory(new UpgradeDemoEmployeeService());

        // In order to support upgrading your Xenon application, you need to start the MigrationTaskService
        host.startFactory(new MigrationTaskService());

        // Added for 'add-logging' project.
        host.log(Level.INFO,
                "Starting host [add-logging] with args these args:%n[id=%s]%n[port=%d]%n[sandbox=%s]%n[peerNodes=%s]",
                host.getId(), host.getPort(), host.getStorageSandbox(), host.getInitialPeerHosts());

        return host;
    }


    public static void main(String[] args) throws Throwable {
        createAndStartHost("--sandbox=/tmp/xenondb");
    }

    private UpgradeDemo() {
    }

}
