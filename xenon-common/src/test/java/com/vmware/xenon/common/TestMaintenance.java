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

package com.vmware.xenon.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupConfig;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestMaintenance extends BasicReportTestCase {
    private static final int NODE_COUNT = 2;
    private static final int SERVICE_COUNT = 5;
    private static final int PERIODIC_MAINTENANCE_MAX = 5;

    public void beforeHostStart(VerificationHost host) {
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
    }

    @After
    public void tearDown() {
        this.host.tearDown();
        this.host.tearDownInProcessPeers();
    }

    private void setUpPeers() throws Throwable {
        this.host.setUpPeerHosts(NODE_COUNT);
        this.host.joinNodesAndVerifyConvergence(NODE_COUNT);
    }

    @Test
    public void statelessServiceHandlePeriodicMaintenance() throws Throwable {
        AtomicInteger periodicMaintenanceCount = new AtomicInteger();
        StatelessService statelessService = new StatelessService() {
            {
                super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
            }

            @Override
            public void handlePeriodicMaintenance(Operation post) {
                ServiceMaintenanceRequest request = post.getBody(ServiceMaintenanceRequest.class);
                if (!request.reasons.contains(
                        ServiceMaintenanceRequest.MaintenanceReason.PERIODIC_SCHEDULE)) {
                    post.fail(new IllegalArgumentException("expected PERIODIC_SCHEDULE reason"));
                    return;
                }

                post.complete();

                if (periodicMaintenanceCount.incrementAndGet() >= PERIODIC_MAINTENANCE_MAX) {
                    super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
                }
            }
        };

        validatePeriodicMaintenance(statelessService, PERIODIC_MAINTENANCE_MAX,
                periodicMaintenanceCount);
    }

    @Test
    public void statefulServiceHandlePeriodicMaintenance() throws Throwable {
        AtomicInteger periodicMaintenanceCount = new AtomicInteger();
        StatefulService statefulService = new StatefulService(ServiceDocument.class) {
            {
                super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
            }

            @Override
            public void handlePeriodicMaintenance(Operation post) {
                ServiceMaintenanceRequest request = post.getBody(ServiceMaintenanceRequest.class);
                if (!request.reasons.contains(
                        ServiceMaintenanceRequest.MaintenanceReason.PERIODIC_SCHEDULE)) {
                    post.fail(new IllegalArgumentException("expected PERIODIC_SCHEDULE reason"));
                    return;
                }

                post.complete();

                if (periodicMaintenanceCount.incrementAndGet() >= PERIODIC_MAINTENANCE_MAX) {
                    super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
                }
            }
        };

        validatePeriodicMaintenance(statefulService, PERIODIC_MAINTENANCE_MAX,
                periodicMaintenanceCount);
    }

    private void validatePeriodicMaintenance(Service s, int maintenanceMax,
            AtomicInteger maintenanceCount) throws Throwable {
        this.host.startServiceAndWait(s, UUID.randomUUID().toString(), null);

        Date exp = this.host.getTestExpiration();

        while (maintenanceCount.get() < maintenanceMax) {
            Thread.sleep(250);
            this.host.log("Handled %d periodic maintenance events, expecting %d",
                    maintenanceCount.get(), maintenanceMax);
            if (new Date().after(exp)) {
                throw new TimeoutException();
            }
        }

        Thread.sleep(250);

        assertEquals(maintenanceMax, maintenanceCount.get());
    }

    @Test
    public void handleNodeGroupMaintenance() throws Throwable {
        setUpPeers();
        // pick one host to post to
        VerificationHost serviceHost = this.host.getPeerHost();
        // test host to receive notifications
        VerificationHost localHost = this.host;
        List<URI> exampleURIs = new ArrayList<>();

        final String factoryLink = ServiceUriPaths.CORE + "/examples-stateful-maintenance";

        AtomicInteger groupMaintenanceCount = new AtomicInteger();
        AtomicInteger periodicMaintenanceCount = new AtomicInteger();

        List<VerificationHost> hosts = new ArrayList<>();
        hosts.add(localHost);
        hosts.addAll(localHost.getInProcessHostMap().values());

        for (VerificationHost h : hosts) {
            // Create factory per host
            FactoryService factoryService = new FactoryService(ServiceDocument.class) {
                {
                    // Enabling periodic maintenance to make sure we don't mix
                    // with group notifications.
                    toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
                    toggleOption(ServiceOption.INSTRUMENTATION, true);
                }

                @Override
                public Service createServiceInstance() throws Throwable {
                    return new StatefulService(ServiceDocument.class) {
                        {
                            toggleOption(Service.ServiceOption.PERSISTENCE, true);
                            toggleOption(Service.ServiceOption.REPLICATION, true);
                            toggleOption(Service.ServiceOption.INSTRUMENTATION, true);
                            toggleOption(Service.ServiceOption.OWNER_SELECTION, true);
                            toggleOption(Service.ServiceOption.PERIODIC_MAINTENANCE, true);
                        }

                        @Override
                        public void handleStart(Operation post) {
                            StatelessService statelessService = new StatelessService() {
                                {
                                    toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
                                    toggleOption(ServiceOption.INSTRUMENTATION, true);
                                }

                                @Override
                                public void handleNodeGroupMaintenance(Operation post) {
                                    post.fail(new IllegalStateException(
                                            "handleNodeGroupMaintenance not expected for child "
                                            + "stateless service"));

                                    int count = groupMaintenanceCount.incrementAndGet();
                                    logInfo("Stateless service handleNodeGroupMaintenance, "
                                            + "count: %s", count);
                                }

                                @Override
                                public void handlePeriodicMaintenance(Operation post) {
                                    post.complete();

                                    int count = periodicMaintenanceCount.incrementAndGet();
                                    logInfo("Factory service handlePeriodicMaintenance, count: %s",
                                            count);
                                }
                            };

                            Operation startStatelessService = Operation.createPost(
                                    UriUtils.extendUri(getUri(), "statelessService"))
                                    .setCompletion((o, e) -> {
                                        if (e != null) {
                                            post.fail(e);
                                            return;
                                        }
                                        post.complete();
                                    });

                            this.getHost().startService(startStatelessService,
                                    statelessService);
                        }

                        @Override
                        public void handleNodeGroupMaintenance(Operation post) {
                            post.complete();

                            int count = groupMaintenanceCount.incrementAndGet();
                            logInfo("Stateful service handleNodeGroupMaintenance, count: %d "
                                    + "post: %s",count, post);
                        }

                        @Override
                        public void handlePeriodicMaintenance(Operation post) {
                            post.complete();

                            int count = periodicMaintenanceCount.incrementAndGet();
                            logInfo("Stateful service handlePeriodicMaintenance, count: %d", count);
                        }
                    };
                }

                @Override
                public void handleNodeGroupMaintenance(Operation post) {
                    post.complete();

                    int count = groupMaintenanceCount.incrementAndGet();
                    logInfo("Factory service handleNodeGroupMaintenance, count: %d post: %s", count,
                            post);
                }

                @Override
                public void handlePeriodicMaintenance(Operation post) {
                    post.complete();

                    int count = periodicMaintenanceCount.incrementAndGet();
                    logInfo("Factory service handlePeriodicMaintenance, count: %d", count);
                }
            };

            h.startService(
                    Operation.createPost(UriUtils.buildUri(h, factoryLink)),
                    factoryService);

            h.waitForServiceAvailable(factoryLink);
        }

        // Create services
        createServices(serviceHost, factoryLink, SERVICE_COUNT, exampleURIs);

        // Get owner host
        String[] ownerHostId = new String[1];
        URI uri = exampleURIs.get(0);

        this.host.testStart(1);
        this.host.send(Operation.createGet(uri).setCompletion((o, e) -> {
            if (e != null) {
                this.host.failIteration(e);
                return;
            }
            ServiceDocument rsp = o.getBody(ServiceDocument.class);
            ownerHostId[0] = rsp.documentOwner;
            this.host.completeIteration();

        }));
        this.host.testWait();

        VerificationHost ownerHost = null;
        // find the host that owns the example service
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            if (h.getId().equals(ownerHostId[0])) {
                ownerHost = h;
                break;
            }
            h.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(1));
        }

        this.host.log("Owner node: %s, host: %s", ownerHostId[0], ownerHost);

        // stop host that has ownership of example service
        NodeGroupConfig cfg = new NodeGroupConfig();
        cfg.nodeRemovalDelayMicros = TimeUnit.SECONDS.toMicros(5);
        this.host.setNodeGroupConfig(cfg);
        this.host.stopHost(ownerHost);

        this.host.waitForNodeGroupConvergence(1);

        // expected count to be the number of services and one for factory
        // Sometimes there are 2 group notifications for factory services in two
        // different hosts
        final int expectedGroupMaintenanceCount = SERVICE_COUNT + 1;

        Date exp = this.host.getTestExpiration();
        while (groupMaintenanceCount.get() < expectedGroupMaintenanceCount
                || periodicMaintenanceCount.get() < 2 * expectedGroupMaintenanceCount) {
            Thread.sleep(250);
            this.host.log("Received %d group and %d periodic notifications, expecting %d group "
                    + "notifications",
                    groupMaintenanceCount.get(), periodicMaintenanceCount.get(),
                    expectedGroupMaintenanceCount);
            if (new Date().after(exp)) {
                throw new TimeoutException();
            }
        }

        Thread.sleep(250);

        // Making sure we don't get too many group notifications
        assertTrue(expectedGroupMaintenanceCount <= expectedGroupMaintenanceCount + 1);
    }

    private void createServices(VerificationHost h, String factoryLink, int serviceCount,
            List<URI> exampleURIs) throws Throwable {
        h.testStart(serviceCount);

        URI factoryUri = UriUtils.buildUri(h, factoryLink);

        // create example services
        for (int i = 0; i < serviceCount; i++) {
            ServiceDocument initialState = new ServiceDocument();
            initialState.documentSelfLink = UUID.randomUUID().toString();
            Operation createPost = Operation
                    .createPost(factoryUri)
                    .setBody(initialState).setCompletion((o, e) -> {
                        if (e != null) {
                            h.failIteration(e);
                            return;
                        }
                        h.completeIteration();
                    });
            h.send(createPost);
            exampleURIs.add(UriUtils.extendUri(factoryUri, initialState.documentSelfLink));
        }

        h.testWait();
    }
}