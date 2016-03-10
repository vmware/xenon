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

package com.vmware.xenon.samples;

import java.util.logging.Level;


import io.swagger.models.Contact;
import io.swagger.models.Info;
import io.swagger.models.License;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.RootNamespaceService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.samples.SampleFactoryServiceWithCustomUi;
import com.vmware.xenon.services.samples.SamplePreviousEchoFactoryService;
import com.vmware.xenon.services.samples.SampleServiceWithSharedCustomUi;
import com.vmware.xenon.services.samples.SampleSimpleEchoFactoryService;
import com.vmware.xenon.swagger.SwaggerDescriptorService;
import com.vmware.xenon.ui.UiService;

/**
 * Our entry point, spawning a host that run/showcase examples we can play with.
 */
public class SampleHost extends ServiceHost {

    public static void main(String[] args) throws Throwable {
        SampleHost h = new SampleHost();
        h.initialize(args);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    /**
     * Start services: a host can run multiple services.
     */
    @Override
    public ServiceHost start() throws Throwable {
        super.start();

        // Start core services (logging, gossiping)-- must be done once
        startDefaultCoreServicesSynchronously();

        // Start the root namespace service: this will list all available factory services for
        // queries to the root (/)
        super.startService(new RootNamespaceService());

        // start the custom ui factory
        super.startService(new SampleServiceWithSharedCustomUi());

        // start the shared UI resources service
        super.startService(new SampleFactoryServiceWithCustomUi());

        // Start a factory for echo sample service
        super.startService(new SampleSimpleEchoFactoryService());

        // Start a factory for the service that returns the previous results
        super.startService(new SamplePreviousEchoFactoryService());

        // Start UI service
        super.startService(new UiService());

        // Serve Swagger 2.0 compatible API description
        SwaggerDescriptorService swagger = new SwaggerDescriptorService();

        // exclude some core services
        swagger.setExcludedPrefixes(
                "/core/transactions",
                "/core/node-groups");

        // Provide API metainfo
        Info apiInfo = new Info();
        apiInfo.setVersion("1.0.0");
        apiInfo.setTitle("Xenon SampleHost");
        apiInfo.setLicense(new License().name("Apache 2.0").url("https://github.com/vmware/xenon/blob/master/LICENSE"));
        apiInfo.setContact(new Contact().url("https://github.com/vmware/xenon"));
        swagger.setInfo(apiInfo);

        // Serve swagger on default uri
        SwaggerDescriptorService.startService(this, swagger);
        System.out.println("Checkout swaggerUI: " + this.getPublicUri() + ServiceUriPaths.SWAGGER + "/ui");

        return this;
    }
}
