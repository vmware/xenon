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

package com.vmware.xenon.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.logging.Level;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.LoaderService.LoaderServiceState;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.test.VerificationHost;

public class LoaderServicePersistTest {
    private VerificationHost host;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private void initHostAfterStart() throws Throwable {
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
        this.host.log(Level.INFO, "Host started");
        loadServices();
    }

    private void loadServices() throws Throwable {
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
                .setBody("")
                .setCompletion(this.host.getCompletion());
        this.host.sendAndWait(createPost);
    }

    private void createHost() throws Throwable {
        VerificationHost h = VerificationHost.create();
        ServiceHost.Arguments args = VerificationHost.buildDefaultServiceHostArguments(0);
        args.sandbox = this.temporaryFolder.getRoot().toPath();
        VerificationHost.initialize(h, args);
        this.host = h;
    }

    static class State {
        String name;
    }

    @Test
    public void testPersistence() throws Throwable {
        try {
            String className = "com.vmware.xenon.examples.ExampleFooFactoryService";
            this.getClass().getClassLoader().loadClass(className);
            Assert.fail("The class " + className + " is in the classpath before loading the services!");
        } catch (ClassNotFoundException e) {
            // We expect this!
        }

        this.temporaryFolder.newFolder();
        createHost();
        this.host.start();
        initHostAfterStart();
        String fooFactoryLink = UriUtils.buildUriPath("loader", "examples", "foo");
        this.host.waitForServiceAvailable(fooFactoryLink);
        State state = new State();
        state.name = "bar";
        Operation createFoo = Operation.createPost(this.host, fooFactoryLink)
                .setBody(state)
                .setCompletion(this.host.getCompletion());
        this.host.sendAndWait(createFoo);
        Operation query = Operation.createGet(this.host, fooFactoryLink);
        query.setCompletion((op, ex) -> {
            if (ex != null) {
                this.host.failIteration(ex);
                return;
            }
            System.out.println(op.getBody(String.class));
            query.setBody(op.getBody(ServiceDocumentQueryResult.class));
            this.host.completeIteration();
        });
        this.host.sendAndWait(query);
        ServiceDocumentQueryResult queryResult = query.getBody(ServiceDocumentQueryResult.class);
        // assertEquals(1, queryResult.documentCount.longValue());
        String documentLink = queryResult.documentLinks.get(0);
        System.out.println(documentLink);
        Operation getDoc = Operation.createGet(this.host, documentLink)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.host.failIteration(ex);
                        return;
                    }
                    State body = op.getBody(State.class);
                    assertEquals("bar", body.name);
                    this.host.completeIteration();
                });
        this.host.sendAndWait(getDoc);
        Thread.sleep(1000);
        this.host.stop();

        createHost();
        this.host.start();
        initHostAfterStart();
        this.host.waitForServiceAvailable(fooFactoryLink);
        this.host.waitForServiceAvailable(documentLink);
        Operation getDoc2 = Operation.createGet(this.host, documentLink)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.host.failIteration(ex);
                        return;
                    }
                    State body = op.getBody(State.class);
                    assertEquals("bar", body.name);
                    this.host.completeIteration();
                });
        this.host.sendAndWait(getDoc2);
        Thread.sleep(1000);
        this.host.stop();
    }
}
