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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import com.vmware.xenon.ui.UiService;
import io.swagger.models.Info;
import io.swagger.models.Swagger;
import io.swagger.util.Json;
import io.swagger.util.Yaml;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;

/**
 */
public class TestSwaggerDescriptorService{

    public static final String INFO_DESCRIPTION = "description";
    public static final String INFO_TERMS_OF_SERVICE = "terms of service";
    public static VerificationHost host;

    @BeforeClass
    public static void setup() throws Throwable {
        host = VerificationHost.create(0);

        SwaggerDescriptorService swagger = new SwaggerDescriptorService();
        Info info = new Info();
        info.setDescription(INFO_DESCRIPTION);
        info.setTermsOfService(INFO_TERMS_OF_SERVICE);
        info.setTitle("title");
        info.setVersion("version");

        swagger.setInfo(info);
        host.start();

        SwaggerDescriptorService.startOn(host, swagger);
        host.startService(Operation.createPost(UriUtils.buildFactoryUri(host, ExampleService.class)),
                ExampleService.createFactory());

        host.startService(Operation.createPost(UriUtils.buildFactoryUri(host, CarService.class)),
                CarService.createFactory());

        host.startService(Operation.createPost(UriUtils.buildUri(host, UiService.class)),
                new UiService());
        host.waitForServiceAvailable(SwaggerDescriptorService.SELF_LINK);
    }

    @AfterClass
    public static void destroy() {
        host.stop();
    }

    @Test
    public void getDescriptionInJson() throws Throwable {
        host.testStart(1);

        Operation op = Operation
                .createGet(UriUtils.buildUri(host, SwaggerDescriptorService.SELF_LINK))
                .setReferer(host.getUri())
                .setCompletion(host.getSafeHandler(this::assertDescriptorJson));

        host.sendRequest(op);

        host.testWait();

        Thread.sleep(1000000);
    }

    @Test
    public void getDescriptionInYaml() throws Throwable {
        host.testStart(1);

        Operation op = Operation
                .createGet(UriUtils.buildUri(host, SwaggerDescriptorService.SELF_LINK))
                .addRequestHeader(Operation.ACCEPT_HEADER, "text/x-yaml")
                .setReferer(host.getUri())
                .setCompletion(host.getSafeHandler(this::assertDescriptorYaml));

        host.sendRequest(op);

        host.testWait();
    }

    private void assertDescriptorYaml(Operation o, Throwable e) {
        assertNull(e);
        try {
            Swagger swagger = Yaml.mapper().readValue(o.getBody(String.class), Swagger.class);
            System.out.println(Yaml.pretty().writeValueAsString(swagger));

            assertSwagger(swagger);
        } catch (IOException ioe) {
            fail(ioe.getMessage());
        }
    }


    private void assertDescriptorJson(Operation o, Throwable e) {
        assertNull(e);
        try {
            Swagger swagger = Json.mapper().readValue(o.getBody(String.class), Swagger.class);
            System.out.println(Json.pretty().writeValueAsString(swagger));

            assertSwagger(swagger);
        } catch (IOException ioe) {
            fail(ioe.getMessage());
        }
    }

    private void assertSwagger(Swagger swagger) {
        assertEquals("/", swagger.getBasePath());
        assertEquals(host.getPublicUri().toString(), swagger.getHost());
        assertEquals(INFO_DESCRIPTION, swagger.getInfo().getDescription());
        assertEquals(INFO_TERMS_OF_SERVICE, swagger.getInfo().getTermsOfService());
    }
}
