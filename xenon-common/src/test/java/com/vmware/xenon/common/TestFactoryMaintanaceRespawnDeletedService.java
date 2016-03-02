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

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;

import org.junit.Test;

import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;

/**
 * This tests a very tricky scenario:
 * Running a node group maintenance of factory service with REPLICATION and PERSISTENCE.
 * When running maintanace, and at the same time a child service was deleted and short after created with API's DELETE and POST,
 * the maintenance will try to re-create the deleted (and stopped) child service.
 * Test verifies that with such race condition no issues should happen.
 *
 * TODO: Remove bellow lines after test starts to pass
 *
 * There are 2 common exceptions that occur depending on the order of the factory maintenance and creation operation:
 * 1: Service /subpath/testfactory/instanceX failed start: java.lang.IllegalStateException: Service already exists or previously deleted: /subpath/testfactory/instanceX:DELETE]
 *    After which operation times out
 * 2: com.vmware.xenon.common.ServiceHost$ServiceAlreadyStartedException: /subpath/testfactory/instanceX
      at com.vmware.xenon.common.ServiceHost.startService(ServiceHost.java:1860)
      ...
      after which POST fails
 *
 * Test is hard to fail, to reproduce interact with the MAINTENANCE_DELAY_HANDLE_MILLIS value
 *
 */
public class TestFactoryMaintanaceRespawnDeletedService {

    private static final int MAINTENANCE_DELAY_HANDLE_MILLIS = 4;
    private static final int MAINTENANCE_INTERVAL_MILLIS = 1000;
    private static final String TEST_FACTORY_PATH = "/subpath/testfactory";
    private static final String TEST_SERVICE_PATH = TEST_FACTORY_PATH + "/instanceX";

    private URI factoryUri;
    private VerificationHost host;

    @Test
    public void test() throws Throwable {
        for (int i = 0; i < 20; i++) {
            Logger.getAnonymousLogger().info(String.format("\n\niteration %s\n\n", i));
            createHostAndServicePostDeletePost();
        }
    }

    private void createHostAndServicePostDeletePost() throws Throwable {
        this.host = VerificationHost.create(0);
        this.host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                .toMicros(MAINTENANCE_INTERVAL_MILLIS));
        CommandLineArgumentParser.parseFromProperties(this.host);
        this.factoryUri = UriUtils.buildUri(this.host, TestFactoryService.class);

        this.host.start();

        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        TestFactoryService factoryService = startFactoryService();

        TestDocument doc = new TestDocument();
        doc.documentSelfLink = TEST_SERVICE_PATH;

        this.host.testStart(1);
        doPost(doc, (e) -> {
            if (e != null) {
                this.host.failIteration(e);
                return;
            }

            factoryService.onMaintenance(() -> {
                doDelete(doc.documentSelfLink, (e1) -> {
                    if (e1 != null) {
                        this.host.failIteration(e1);
                        return;
                    }

                    doPost(doc, (e2) -> {
                        if (e2 != null) {
                            this.host.failIteration(e2);
                            return;
                        }
                        this.host.completeIteration();
                    });
                });
            });

        });

        this.host.testWait();

        this.host.tearDown();
    }

    private void doPost(TestDocument doc, Consumer<Throwable> callback) {
        this.host.send(Operation
                .createPost(this.factoryUri)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                .setBody(doc)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        callback.accept(e);
                    } else {
                        callback.accept(null);
                    }
                }));
    }

    private void doDelete(String documentSelfLink, Consumer<Throwable> callback) {
        this.host.send(Operation.createDelete(
                UriUtils.buildUri(this.host, documentSelfLink))
                .setBody(new TestDocument())
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                callback.accept(e);
                            } else {
                                callback.accept(null);
                            }
                        }));
    }

    private TestFactoryService startFactoryService() throws Throwable {
        TestFactoryService factoryService = new TestFactoryService();

        this.host.startService(
                Operation.createPost(this.factoryUri), factoryService);
        this.host.waitForServiceAvailable(TestFactoryService.SELF_LINK);

        return factoryService;
    }

    public static class TestFactoryService extends FactoryService {

        public static final String SELF_LINK = TEST_FACTORY_PATH;

        Runnable waitingForMainatance;

        public void onMaintenance(Runnable r) {
            this.waitingForMainatance = r;
        }

        TestFactoryService() {
            super(TestDocument.class);
            toggleOption(ServiceOption.IDEMPOTENT_POST, true);
            toggleOption(ServiceOption.PERSISTENCE, true);
            toggleOption(ServiceOption.REPLICATION, true);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new TestStatefulService();
        }

        @Override
        public void handleNodeGroupMaintenance(Operation post) {
            if (this.waitingForMainatance != null) {
                getHost().schedule(this.waitingForMainatance, MAINTENANCE_DELAY_HANDLE_MILLIS,
                        TimeUnit.MILLISECONDS);
                this.waitingForMainatance = null;
            }
            super.handleNodeGroupMaintenance(post);

        }

    }

    public static class TestStatefulService extends StatefulService {

        TestStatefulService() {
            super(TestDocument.class);

            toggleOption(ServiceOption.PERSISTENCE, true);
            toggleOption(ServiceOption.REPLICATION, true);
        }
    }

    public static class TestDocument extends ServiceDocument {
        public int value;

    }
}
