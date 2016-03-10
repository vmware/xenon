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

package com.vmware.xenon.swagger;

import java.util.logging.Level;

import io.swagger.models.Info;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.ui.UiService;

/**
 * A demo class showing how SwaggerDescriptorService can be used.
 */
public class SwaggerExampleHost extends  ServiceHost{
    public static void main(String[] args) throws Throwable {
        SwaggerExampleHost host = new SwaggerExampleHost();
        host.initialize(args);
        host.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            host.log(Level.WARNING, "Host stopping ...");
            host.stop();
            host.log(Level.WARNING, "Host is stopped");
        }));




        System.out.println("Visit " + host.getPublicUri() + "/discovery/swagger/ui");
    }

    @Override
    public ServiceHost start() throws Throwable {
        super.start();

        startDefaultCoreServicesSynchronously();

        SwaggerDescriptorService swagger = new SwaggerDescriptorService();
        Info info = new Info();
        info.setDescription("example description");
        info.setTermsOfService("terms of service");
        info.setTitle("title");
        info.setVersion("version");

        swagger.setInfo(info);

        // exclude some of the core services from the descriptor
        swagger.setExcludedPrefixes("/core/authz/", "/core/transactions");

        SwaggerDescriptorService.startOn(this, swagger);

        this.startService(
                Operation.createPost(UriUtils.buildFactoryUri(this, ExampleService.class)),
                ExampleService.createFactory());

        this.startService(Operation.createPost(UriUtils.buildFactoryUri(this, CarService.class)),
                CarService.createFactory());

        this.startService(Operation.createPost(UriUtils.buildUri(this, UiService.class)),
                new UiService());

        this.startService(
                Operation.createPost(UriUtils.buildFactoryUri(this, ExampleService.class)),
                new ExampleService());

        return this;
    }
}
