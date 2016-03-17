/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;

import io.swagger.models.Info;
import io.swagger.models.Path;
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
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.ui.UiService;

/**
 */
public class TestSwaggerDescriptorService {

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
        swagger.setExcludedPrefixes("/core/authz/");
        host.start();

        host.startService(swagger);

        host.startService(
                Operation.createPost(UriUtils.buildFactoryUri(host, ExampleService.class)),
                ExampleService.createFactory());

        host.startService(Operation.createPost(UriUtils.buildFactoryUri(host, CarService.class)),
                CarService.createFactory());

        host.startService(Operation.createPost(UriUtils.buildUri(host, UiService.class)),
                new UiService());

        host.startService(
                Operation.createPost(UriUtils.buildFactoryUri(host, ExampleService.class)),
                new ExampleService());

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
            assertSwagger(swagger);
        } catch (IOException ioe) {
            fail(ioe.getMessage());
        }
    }

    @Test
    public void testSwaggerUiAvailable() throws Throwable {
        host.testStart(1);

        URI uri = UriUtils.buildUri(host, SwaggerDescriptorService.SELF_LINK + ServiceUriPaths.UI_PATH_SUFFIX);
        Operation op = Operation
                .createGet(new URI(uri.toString() + "/"))
                .setReferer(host.getUri())
                .setCompletion(host.getSafeHandler(this::assertSwaggerUiAvailable));

        host.sendRequest(op);

        host.testWait();
    }

    private void assertSwaggerUiAvailable(Operation o, Throwable e) {
        assertEquals(Operation.STATUS_CODE_OK, o.getStatusCode());

        String body = o.getBody(String.class);
        assertTrue(body.contains("swagger-ui-container"));
    }

    private void assertDescriptorJson(Operation o, Throwable e) {
        if (e != null) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            Swagger swagger = Json.mapper().readValue(o.getBody(String.class), Swagger.class);
            assertSwagger(swagger);
        } catch (IOException ioe) {
            fail(ioe.getMessage());
        }
    }

    private void assertSwagger(Swagger swagger) {
        assertEquals("/", swagger.getBasePath());

        assertEquals(INFO_DESCRIPTION, swagger.getInfo().getDescription());
        assertEquals(INFO_TERMS_OF_SERVICE, swagger.getInfo().getTermsOfService());


        // excluded prefixes
        assertNull(swagger.getPath(ServiceUriPaths.CORE_AUTHZ_USERS));
        assertNull(swagger.getPath(ServiceUriPaths.CORE_AUTHZ_ROLES));

        assertNotNull(swagger.getPath(ServiceUriPaths.CORE_PROCESSES));
        assertNotNull(swagger.getPath(ServiceUriPaths.CORE_CREDENTIALS));

        Path p = swagger.getPath("/cars");
        assertNotNull(p);
        assertNotNull(p.getPost());
        assertNotNull(p.getGet());


        assertNotNull(swagger.getPath("/cars/template"));
        assertNotNull(swagger.getPath("/cars/available"));
        assertNotNull(swagger.getPath("/cars/config"));
        assertNotNull(swagger.getPath("/cars/stats"));
        assertNotNull(swagger.getPath("/cars/subscriptions"));

        assertNotNull(swagger.getPath("/cars/{id}/template"));
        assertNotNull(swagger.getPath("/cars/{id}/available"));
        assertNotNull(swagger.getPath("/cars/{id}/config"));
        assertNotNull(swagger.getPath("/cars/{id}/stats"));
        assertNotNull(swagger.getPath("/cars/{id}/subscriptions"));


        p = swagger.getPath("/cars/{id}");
        assertNotNull(p);
        assertNotNull(p.getPost());
        assertNotNull(p.getGet());
        assertNotNull(p.getPatch());
        assertNotNull(p.getPut());
    }
}
