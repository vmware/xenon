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

package com.vmware.xenon.host;

import java.util.logging.Level;

import com.vmware.xenon.common.AuthorizationSetupHelper;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.RootNamespaceService;
import com.vmware.xenon.ui.UiService;

/**
 * Stand alone process entry point
 */
public class DecentralizedControlPlaneHost extends ServiceHost {

    public static class ServiceHostArguments extends Arguments {
        /**
         * The email address of a user that should be granted "admin" privileges to all services
         */
        public String adminUser = "admin@localhost";

        /**
         * (Required) The password of the adminUser
         */
        public String adminPassword;
    }

    private ServiceHostArguments args;

    public static void main(String[] args) throws Throwable {
        DecentralizedControlPlaneHost h = new DecentralizedControlPlaneHost();
        h.initialize(args);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    @Override
    public ServiceHost initialize(String[] args) throws Throwable {
        this.args = new ServiceHostArguments();
        super.initialize(args, this.args);

        if (this.args.adminPassword == null) {
            throw new IllegalStateException("adminPassword is required. specify \"--adminPassword=<pass>\" param");
        }

        return this;
    }

    @Override
    public ServiceHost start() throws Throwable {
        super.start();

        startDefaultCoreServicesSynchronously();

        setAuthorizationContext(this.getSystemAuthorizationContext());

        super.startService(new RootNamespaceService());

        // start an example factory for folks that want to experiment with service instances
        super.startFactory(ExampleService.class, ExampleService::createFactory);

        // Start UI service
        super.startService(new UiService());

        AuthorizationSetupHelper.create()
                .setHost(this)
                .setUserEmail(this.args.adminUser)
                .setUserPassword(this.args.adminPassword)
                .setIsAdmin(true)
                .start();

        setAuthorizationContext(null);

        return this;
    }
}
