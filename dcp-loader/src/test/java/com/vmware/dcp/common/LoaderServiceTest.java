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

package com.vmware.dcp.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.LoaderService.LoaderServiceState;
import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.test.VerificationHost;

public class LoaderServiceTest {
    private VerificationHost host;

    @Before
    public void setUp() throws Exception {
        try {
            // Make sure a local tmp dir exists to work around the issue with
            // maven test pointing "java.io.tmpdir" system property to a relative "./tmp" path.
            File directory = new File("tmp");
            if (!directory.exists()) {
                directory.mkdir();
            }

            this.host = VerificationHost.create(0, null);
            this.host.start();
            // start the factory
            CompletionHandler completionHandler = (o, e) -> {
                if (e != null) {
                    fail("Service failed start");
                    return;
                }
                Operation post = LoaderFactoryService.createDefaultPostOp(
                        this.host).setReferer(UriUtils.buildUri(this.host, ""));
                this.host.sendRequest(post);
            };

            this.host.startService(Operation.createPost(
                    UriUtils.buildUri(this.host, LoaderFactoryService.class))
                    .setCompletion(completionHandler), new LoaderFactoryService());

            this.host.waitForServiceAvailable(LoaderFactoryService.SELF_LINK);

        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        this.host.tearDown();
    }

    @Test
    public void factoryPost() throws Throwable {
        // make sure factory is started.
        this.host.waitForServiceAvailable(LoaderFactoryService.SELF_LINK);

        URI factoryUri = UriUtils.buildUri(this.host, LoaderFactoryService.class);
        int childCount = 2;
        this.host.testStart(childCount);
        String prefix = "LoaderTest-";
        URI[] childURIs = new URI[childCount];
        for (int i = 0; i < childCount; i++) {
            LoaderServiceState initialState = new LoaderServiceState();
            initialState.path = initialState.documentSelfLink = prefix + i;
            final int finalI = i;
            // create a service instance
            Operation createPost = Operation
                    .createPost(factoryUri)
                    .setBody(initialState)
                    .setCompletion(
                            (o, e) -> {
                                if (e != null) {
                                    this.host.failIteration(e);
                                    return;
                                }
                                ServiceDocument rsp =
                                        o.getBody(ServiceDocument.class);
                                childURIs[finalI] =
                                        UriUtils.buildUri(this.host,
                                                rsp.documentSelfLink);
                                this.host.completeIteration();
                            });
            this.host.send(createPost);
        }
        this.host.testWait();

        // do GET on all child URIs
        Map<URI, LoaderServiceState> childStates =
                this.host.getServiceState(null, LoaderServiceState.class, childURIs);
        for (LoaderServiceState s : childStates.values()) {
            assertTrue(s.path.startsWith(prefix));
        }

        // verify template GET works on factory
        ServiceDocumentQueryResult templateResult = this.host.getServiceState(null,
                ServiceDocumentQueryResult.class,
                UriUtils.extendUri(factoryUri, ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE));

        assertTrue(templateResult.documentLinks.size() == templateResult.documents.size());
        LoaderServiceState childTemplate = Utils.fromJson(
                templateResult.documents.get(templateResult.documentLinks.iterator().next()),
                LoaderServiceState.class);
        assertTrue(childTemplate.path != null);
        assertTrue(childTemplate.documentDescription != null);
    }

    @Test
    public void testDefaultInstance() throws Throwable {
        String uriPath = UriUtils.buildUriPath(
                LoaderFactoryService.SELF_LINK, LoaderService.FILESYSTEM_DEFAULT_GROUP);
        URI defaultURI =
                UriUtils.buildUri(this.host, uriPath);

        this.host.waitForServiceAvailable(uriPath);
        LoaderServiceState defaultServiceState =
                this.host
                        .getServiceState(null, LoaderServiceState.class, defaultURI);

        assertEquals(defaultServiceState.path,
                LoaderService.FILESYSTEM_DEFAULT_PATH);

        // Since there are no service packages in the default location currently
        // we expect the number of packages discovered to be 0
        assertTrue(defaultServiceState.servicePackages.size() == 0);

        // Now lets deploy some test services
        File newDirectory = new File(this.host.getStorageSandbox().getPath(), "/services");
        if (!newDirectory.exists()) {
            assertTrue(newDirectory.mkdirs());
        }

        this.host.log("Copying test service package to host storage %s", newDirectory);
        FileUtils.copyFiles(new File("target/services"), newDirectory);

        assertTrue("No service packages in host storage dir", newDirectory.list().length > 0);

        // Issue a POST to the default LoaderService instance to trigger reload
        Operation createPost = Operation
                .createPost(defaultURI)
                .setBody("");
        this.host.send(createPost);

        // Verify the services are loaded
        Date expiration = this.host.getTestExpiration();
        while (new Date().before(expiration)) {
            defaultServiceState = this.host
                    .getServiceState(null, LoaderServiceState.class, defaultURI);

            // Now there is one service package
            this.host.log("Verifying existence of loaded service packages");
            if (defaultServiceState.servicePackages.size() == 1) {
                // Expecting 3 test service classes in the package
                this.host.log("Verifying loaded service classes");
                if (defaultServiceState.servicePackages.values().iterator()
                        .next().serviceClasses.size() == 3) {
                    this.host.log("Found expected service classes");
                    break;
                } else {
                    this.host.log("Expected service classes not found");
                }
            } else {
                this.host.log("Waiting for service packages to be loaded");
            }

            Thread.sleep(1000L);
        }

        if (new Date().after(expiration)) {
            throw new TimeoutException("Unable to verify test service package loading");
        }
    }
}
