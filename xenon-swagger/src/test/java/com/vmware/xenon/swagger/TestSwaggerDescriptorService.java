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

import static com.vmware.xenon.common.Operation.ACCEPT_ENCODING_HEADER;
import static com.vmware.xenon.common.Operation.CONTENT_ENCODING_GZIP;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import com.vmware.xenon.common.test.TestRequestSender;
import io.swagger.models.Info;
import io.swagger.models.Model;
import io.swagger.models.Path;
import io.swagger.models.Swagger;
import io.swagger.models.properties.Property;
import io.swagger.util.Json;
import io.swagger.util.Yaml;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.swagger.TokenService.UserToken;
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

        host.startService(Operation.createPost(UriUtils.buildUri(host, TokenService.class)),
                new TokenService());

        host.waitForServiceAvailable(SwaggerDescriptorService.SELF_LINK);
    }

    @AfterClass
    public static void destroy() {
        host.stop();
    }

    @Test
    public void getDescriptionInJson() throws Throwable {
        TestRequestSender sender = host.getTestRequestSender();

        Operation op = Operation
                .createGet(UriUtils.buildUri(host, SwaggerDescriptorService.SELF_LINK))
                .setReferer(host.getUri());

        Operation result = null;
        Throwable error = null;
        try {
            result = sender.sendAndWait(op);
        } catch (Throwable e) {
            error = e;
        }

        assertDescriptorJson(result, error);
    }

    @Test
    public void getDescriptionInCompressedJson() throws Throwable {
        TestRequestSender sender = host.getTestRequestSender();

        Operation op = Operation
                .createGet(UriUtils.buildUri(host, SwaggerDescriptorService.SELF_LINK))
                .setReferer(host.getUri())
                .addRequestHeader(ACCEPT_ENCODING_HEADER, CONTENT_ENCODING_GZIP);
        Operation result = null;
        Throwable error = null;
        try {
            result = sender.sendAndWait(op);
        } catch (Throwable e) {
            error = e;
        }

        assertDescriptorJson(result, error);
    }

    @Test
    public void getDescriptionInYaml() throws Throwable {
        TestRequestSender sender = host.getTestRequestSender();

        Operation op = Operation
                .createGet(UriUtils.buildUri(host, SwaggerDescriptorService.SELF_LINK))
                .addRequestHeader(Operation.ACCEPT_HEADER, "text/x-yaml")
                .setReferer(host.getUri());

        Operation result = sender.sendAndWait(op);

        assertDescriptorYaml(result);
    }

    private void assertDescriptorYaml(Operation o) {
        try {
            Swagger swagger = Yaml.mapper().readValue(o.getBody(String.class), Swagger.class);
            assertSwagger(swagger);
        } catch (IOException ioe) {
            fail(ioe.getMessage());
        }
    }

    @Test
    public void testSwaggerUiAvailable() throws Throwable {
        TestRequestSender sender = host.getTestRequestSender();

        URI uri = UriUtils.buildUri(host, SwaggerDescriptorService.SELF_LINK + ServiceUriPaths.UI_PATH_SUFFIX);
        Operation op = Operation
                .createGet(new URI(uri.toString() + "/"))
                .setReferer(host.getUri());

        Operation result = sender.sendAndWait(op);

        assertSwaggerUiAvailable(result);
    }

    private void assertSwaggerUiAvailable(Operation o) {
        assertEquals(Operation.STATUS_CODE_OK, o.getStatusCode());

        String body = o.getBody(String.class);
        assertTrue(body.contains("swagger-ui-container"));
    }

    private void assertDescriptorJson(Operation o, Throwable e) {
        if (e != null) {
            Throwable[] suppressed = e.getSuppressed();
            if (suppressed != null && suppressed.length > 0) {
                e = suppressed[0];
            }

            e.printStackTrace();

            if (e.getMessage().contains("Unparseable JSON body")) {
                // Ignore failure
                // Expecting GSON classloading issue to be fixed:
                //  - https://github.com/google/gson/issues/764
                //  - https://www.pivotaltracker.com/story/show/120885303
                Utils.logWarning("GSON initialization failure: %s", e);
                // Stop assertion logic here, test will finish as success
                return;
            } else {
                fail(e.getMessage());
            }
        }

        decompressResponse(o);

        try {
            Swagger swagger = Json.mapper().readValue(o.getBody(String.class), Swagger.class);
            assertSwagger(swagger);
        } catch (IOException ioe) {
            fail(ioe.getMessage());
        }
    }

    private void decompressResponse(Operation o) {
        String acceptEncoding = o.getRequestHeader(ACCEPT_ENCODING_HEADER);
        if (acceptEncoding != null && acceptEncoding.contains(CONTENT_ENCODING_GZIP)) {
            acceptEncoding = CONTENT_ENCODING_GZIP;
        }

        String encoding = o.getResponseHeader(Operation.CONTENT_ENCODING_HEADER);

        if (CONTENT_ENCODING_GZIP.equals(acceptEncoding)
                && CONTENT_ENCODING_GZIP.equals(encoding)) {
            try {
                byte[] bytes = o.getBody(byte[].class);
                Utils.decodeBody(o, ByteBuffer.wrap(bytes), false);
            } catch (Exception ex) {
                fail(ex.getMessage());
            }
        } else if (!Objects.equals(acceptEncoding, encoding)) {
            fail(String.format("Wrong encoding accept-encoding=%s, encoding=%s",
                    acceptEncoding, encoding));
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
        assertNull(p.getPost());
        assertNull(p.getPatch());
        assertNotNull(p.getGet());
        assertNotNull(p.getPut());

        p = swagger.getPath("/tokens");
        assertNotNull(p);
        assertNotNull(p.getGet());
        assertNotNull(p.getGet().getResponses());
        assertNotNull(p.getPost());
        assertNotNull(p.getPost().getParameters());
        assertNull(p.getPatch());
        assertNull(p.getDelete());

        Model model = swagger.getDefinitions().get(Utils.buildKind(UserToken.class));
        Map<String, Property> properties = model.getProperties();
        assertNull(properties.get(UserToken.FIELD_NAME_INTERNAL_ID));
    }
}
