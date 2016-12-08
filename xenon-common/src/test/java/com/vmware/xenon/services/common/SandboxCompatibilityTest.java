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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.google.gson.JsonObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceHost.Arguments;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.NodeGroupService.JoinPeerRequest;

/**
 * This test will not run unless xenon.sandboxRoot is defined. It is meant to be run from CI or manually when making
 * changes to the low level aspects like serialization, file management, configuration, etc.
 *
 */
public class SandboxCompatibilityTest {

    /** Store case files. One json file in this directory stores a single case. */
    private Path casesDir;

    private List<VerificationHost> hosts;

    private final AtomicInteger caseCounter = new AtomicInteger();

    /** Links needed to be available before tests proceed. */
    private List<String> services;

    public static class FindForLink {
        public String selfLink;
        public JsonObject json;
        public String documentClass;

        private transient ServiceDocument document;

        private FindForLink() {
            // serialization constructor
        }

        public FindForLink(ServiceDocument document) {
            this.selfLink = document.documentSelfLink;
            String s = Utils.toJson(document);
            this.json = Utils.fromJson(s, JsonObject.class);
            this.documentClass = document.getClass().getName();
        }

        @SuppressWarnings("unchecked")
        public Class<ServiceDocument> getDocumentClass() throws ClassNotFoundException {
            return (Class<ServiceDocument>) Thread.currentThread().getContextClassLoader()
                    .loadClass(this.documentClass);
        }

        public ServiceDocument getDocument() throws ClassNotFoundException {
            if (this.document == null) {
                Class<ServiceDocument> cls = this.getDocumentClass();
                this.document = Utils.fromJson(this.json, cls);
            }

            return this.document;
        }
    }

    /** Workdir for all in-process hosts. If not set test will exit immediately with success. */
    public Path sandboxRoot;

    public int nodeCount = 3;

    public int convergenceTimeoutSeconds = 30;

    @Before
    public void setup() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        this.hosts = new ArrayList<>();

        // add links to services we are interested in in order to wait for them to start
        this.services = new ArrayList<>();
        this.services.add(ExampleService.FACTORY_LINK);
    }

    @After
    public void tearDown() {
        for (VerificationHost host : this.hosts) {
            host.stop();
        }
    }

    private void startHosts() throws Throwable {
        for (int i = 0; i < this.nodeCount; i++) {
            Arguments args = new Arguments();
            args.port = 0;
            args.id = "node-" + i;
            args.sandbox = this.sandboxRoot.resolve(args.id);

            VerificationHost h = VerificationHost.create(args);
            h.start();

            startServicesOnHost(h);
            this.hosts.add(h);
        }

        joinNodesAndVerifyConvergence();
    }

    private void startServicesOnHost(VerificationHost h) throws IOException {
        // TODO start services that go through edge cases in json/kryo/etc.
    }

    public void joinNodesAndVerifyConvergence() throws Throwable {
        for (int i = 0; i < this.nodeCount - 1; i++) {
            int next = (i + this.nodeCount + 1) % this.nodeCount;

            VerificationHost host = this.hosts.get(i);
            VerificationHost nextHost = this.hosts.get(next);

            JoinPeerRequest joinBody = JoinPeerRequest
                    .create(UriUtils.buildUri(host, ServiceUriPaths.DEFAULT_NODE_GROUP), this.nodeCount);

            URI nodeGroupUri = UriUtils.buildUri(nextHost, ServiceUriPaths.DEFAULT_NODE_GROUP);
            Operation.createPost(nodeGroupUri)
                    .setBody(joinBody)
                    .setReferer(nextHost.getReferer())
                    .sendWith(nextHost);
        }

        TestNodeGroupManager manager = new TestNodeGroupManager(ServiceUriPaths.DEFAULT_NODE_GROUP_NAME);
        manager.addHosts(new ArrayList<>(this.hosts));
        manager.setTimeout(Duration.ofSeconds(this.convergenceTimeoutSeconds));
        manager.waitForConvergence();

        awaitServicesAvailable();
    }

    private void awaitServicesAvailable() throws InterruptedException {
        CountDownLatch cl = new CountDownLatch(this.nodeCount * this.services.size());

        for (VerificationHost host : this.hosts) {
            for (String serviceLink : this.services) {
                host.registerForServiceAvailability((o, e) -> cl.countDown(), true, serviceLink);
            }
        }

        cl.await(this.convergenceTimeoutSeconds, TimeUnit.SECONDS);
    }

    private void createSandboxDirectories() throws IOException {
        Files.createDirectories(this.sandboxRoot);
        this.casesDir = this.sandboxRoot.resolve("cases");
        Files.createDirectories(this.casesDir);
    }

    private VerificationHost getHost() {
        return this.hosts.get(ThreadLocalRandom.current().nextInt(this.hosts.size()));
    }

    private void saveCase(FindForLink obj) throws IOException {
        Path p = this.casesDir.resolve("case-" + this.caseCounter.incrementAndGet() + ".json");
        Files.write(p, Utils.toJsonHtml(obj).getBytes(Utils.CHARSET));
    }

    private void checkSavedCases() throws Exception {
        File[] caseFiles = this.casesDir.toFile().listFiles(f -> f.getName().endsWith(".json"));
        if (caseFiles == null) {
            return;
        }

        for (File json : caseFiles) {
            byte[] contents = Files.readAllBytes(json.toPath());
            FindForLink ffl = Utils.fromJson(new String(contents, Utils.CHARSET), FindForLink.class);
            assertStoredStateMatchesFetched(ffl);
        }
    }

    private void assertStoredStateMatchesFetched(FindForLink ffl) throws Exception {
        VerificationHost host = getHost();
        Operation op = Operation.createGet(host, ffl.selfLink);

        op = host.waitForResponse(op);
        assertEquals(Operation.STATUS_CODE_OK, op.getStatusCode());

        ServiceDocument fetchedDoc = op.getBody(ffl.getDocumentClass());
        ServiceDocument originalDoc = ffl.getDocument();

        ServiceDocumentDescription desc = host.buildDescription(originalDoc.getClass());

        String s1 = Utils.computeSignature(fetchedDoc, desc);
        String s2 = Utils.computeSignature(originalDoc, desc);

        if (!s1.equals(s2)) {
            String msg = "A document from the sandbox has different signature after upgrade" + ffl.selfLink;
            Logger.getAnonymousLogger()
                    .info(String.format("Expected: %s, got %s", Utils.toJson(originalDoc), Utils.toJson(fetchedDoc)));
            fail(msg);
        }
    }

    private void saveCases() throws IOException {
        for (int i = 0; i < 20; i++) {
            VerificationHost host = getHost();
            ExampleServiceState state = new ExampleServiceState();
            state.name = UUID.randomUUID().toString();
            state.id = UUID.randomUUID().toString();
            state.counter = System.currentTimeMillis();
            state.sortedCounter = state.counter + 1;
            state.keyValues = new HashMap<>();
            state.keyValues.put(state.name, state.name);
            state.keyValues.put(state.id, state.id);
            Operation op = Operation.createPost(UriUtils.buildFactoryUri(host, ExampleService.class))
                    .setBody(state);

            op = host.waitForResponse(op);

            saveCase(new FindForLink(op.getBody(ExampleServiceState.class)));
        }
    }

    /**
     * Given a correct {@link #sandboxRoot} this method will start the hosts again and assertStoredStateMatchesFetched if the signature of all
     * services stored to json files by {@link #prepareSandbox()} match the signatures of the services when retrieved
     * using GET.
     * @throws Throwable
     */
    @Test
    public void resumeFromSandbox() throws Throwable {
        if (this.sandboxRoot == null) {
            return;
        }
        createSandboxDirectories();

        startHosts();
        checkSavedCases();
    }

    /**
     * When run this methods creates {@link #nodeCount} local inprocess nodes and put their sandboxes in
     * {@link #sandboxRoot}. Several services are created and their contents written to separate json files also in
     * the sandboxRoot, one file per service.
     *
     * @throws Throwable
     */
    @Test
    public void prepareSandbox() throws Throwable {
        if (this.sandboxRoot == null) {
            return;
        }
        try {
            FileUtils.deleteFiles(this.sandboxRoot.toFile());
        } catch (Exception ignore) {

        }

        createSandboxDirectories();
        startHosts();
        saveCases();
    }
}