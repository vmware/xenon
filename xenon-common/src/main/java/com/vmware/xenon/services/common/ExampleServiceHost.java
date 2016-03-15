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

import java.util.logging.Level;

import com.vmware.xenon.common.AuthorizationSetupHelper;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class ExampleServiceHost extends ServiceHost {

    public static class ExampleHostArguments extends Arguments {
        /**
         * The email address of a user that should be granted "admin" privileges to all services
         */
        public String adminUser;

        /**
         * The password of the adminUser
         */
        public String adminUserPassword;

        /**
         * The email address of a user that should be granted privileges just to example services
         * that they own
         */
        public String exampleUser;

        /**
         * The password of the exampleUser
         */
        public String exampleUserPassword;
    }

    private ExampleHostArguments args;

    public static void main(String[] args) throws Throwable {
        ExampleServiceHost h = new ExampleServiceHost();
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
        this.args = new ExampleHostArguments();
        super.initialize(args, this.args);
        if (this.args.adminUser != null && this.args.adminUserPassword == null) {
            throw new IllegalStateException("adminUser specified, but not adminUserPassword");
        }
        if (this.args.exampleUser != null && this.args.exampleUserPassword == null) {
            throw new IllegalStateException("exampleUser specified, but not exampleUserPassword");
        }
        return this;
    }

    @Override
    public ServiceHost start() throws Throwable {
        super.start();

        startDefaultCoreServicesSynchronously();

        setAuthorizationContext(this.getSystemAuthorizationContext());

        // Start the example service factory
        super.startFactory(ExampleService.class, ExampleService::createFactory);

        // Start the example task service factory: when it receives a task, it will delete
        // all example services
        super.startFactory(ExampleTaskService.class, ExampleTaskService::createFactory);

        // Start the root namespace factory: this will respond to the root URI (/) and list all
        // the factory services.
        super.startService(new RootNamespaceService());

        // The args are null because many of the tests use this class (via VerificationHost)
        // without providing arguments.
        if (this.args != null) {
            if (this.args.adminUser != null) {
                AuthorizationSetupHelper.create()
                        .setHost(this)
                        .setUserEmail(this.args.adminUser)
                        .setUserPassword(this.args.adminUserPassword)
                        .setIsAdmin(true)
                        .start();
            }
            if (this.args.exampleUser != null) {
                AuthorizationSetupHelper.create()
                        .setHost(this)
                        .setUserEmail(this.args.exampleUser)
                        .setUserPassword(this.args.exampleUserPassword)
                        .setIsAdmin(false)
                        .setDocumentKind(Utils.buildKind(ExampleServiceState.class))
                        .start();
            }
        }

        setAuthorizationContext(null);

        return this;
    }

}