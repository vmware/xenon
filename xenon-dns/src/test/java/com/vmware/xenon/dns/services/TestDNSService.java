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

package com.vmware.xenon.dns.services;

import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Service that represents DNS records
 */

public class TestDNSService extends BasicTestCase {

    public VerificationHost dnsHost;
    private static long HEALTH_CHECK_INTERVAL = 1L;
    /**
     * Command line argument specifying default number of in process service hosts
     */
    public int nodeCount = 3;

    private boolean isAuthorizationEnabled = false;

    private void configureHost(int localHostCount) throws Throwable {

        CommandLineArgumentParser.parseFromProperties(this);
        this.host = VerificationHost.create(0);
        this.host.setAuthorizationEnabled(this.isAuthorizationEnabled);
        this.host.start();

        this.dnsHost = VerificationHost.create(0);
        this.dnsHost.setAuthorizationEnabled(this.isAuthorizationEnabled);
        this.dnsHost.start();

        if (this.host.isAuthorizationEnabled()) {
            this.host.setSystemAuthorizationContext();
        }

        if (this.dnsHost.isAuthorizationEnabled()) {
            this.dnsHost.setSystemAuthorizationContext();
        }

        DNSServices.startServices(this.dnsHost, null);

        CommandLineArgumentParser.parseFromProperties(this.host);
        this.host.setStressTest(this.host.isStressTest);
        this.host.setUpPeerHosts(localHostCount);

        for (VerificationHost h1 : this.host.getInProcessHostMap().values()) {
            setUpPeerHostWithAdditionalServices(h1);
        }
    }

    private void setUpPeerHostWithAdditionalServices(VerificationHost h1) throws Throwable {
        h1.setStressTest(this.host.isStressTest);
        h1.waitForServiceAvailable(ExampleService.FACTORY_LINK);
    }

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
    }

    @After
    public void tearDown() throws InterruptedException {
        if (this.host == null) {
            return;
        }

        this.host.tearDownInProcessPeers();
        this.host.toggleNegativeTestMode(false);
        this.host.tearDown();

        if (this.dnsHost == null) {
            return;
        }

        this.dnsHost.tearDown();

    }

    @Test
    public void registerWithDNSTest() throws Throwable {

        configureHost(this.nodeCount);
        // the join will set quorum equal to node count
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        for (VerificationHost h1 : this.host.getInProcessHostMap().values()) {

            Operation.CompletionHandler completionHandler = (o, e) -> {
                assert (e == null);
                this.host.completeIteration();
            };
            this.host.testStart(1);
            h1.sendRequest(DNSFactoryService.createPost(this.dnsHost.getPublicUri(),
                    h1,
                    ExampleService.FACTORY_LINK,
                    ExampleService.class.getSimpleName(),
                    null,
                    ExampleService.FACTORY_LINK + "/available",
                    HEALTH_CHECK_INTERVAL).setCompletion(completionHandler));
            this.host.testWait();
        }

        while (new Date().before(this.host.getTestExpiration())) {
            /* Verify records exist at DNS service */

            Thread.sleep(TimeUnit.SECONDS.toMillis(HEALTH_CHECK_INTERVAL));

            ServiceDocumentQueryResult res = doQuery(String.format("$filter=serviceName eq %s",
                    ExampleService.class.getSimpleName()));

            assert (res.documentLinks != null);
            assert (res.documentLinks.size() == 1);

            DNSService.DNSServiceState serviceState =
                    Utils.fromJson((String) res.documents.get(res.documentLinks.get(0)),
                            DNSService.DNSServiceState.class);
            if (serviceState.nodeReferences.size() == this.nodeCount) {
                break;
            }
        }

        if (new Date().after(this.host.getTestExpiration())) {
            new TimeoutException();
        }

        doServiceFailureTest();
    }


    private void doServiceFailureTest() throws Throwable {

        // before we stop a node, we must reduce the quorum
        this.host.setNodeGroupQuorum(this.nodeCount - 1);

        /*
            Stop a peer node, issue the query again after the HEALTH_CHECK_INTERVAL
            Verify that the available node count goes down by one.
         */

        for (VerificationHost hostToStop : this.host.getInProcessHostMap().values()) {

            if (!hostToStop.getId().equals(this.host.getId())) {
                this.host.log("Stopping host %s", hostToStop);
                this.host.stopHost(hostToStop);
                break;
            }

        }

        while (new Date().before(this.host.getTestExpiration())) {
            /* Verify records exist at DNS service */

            Thread.sleep(TimeUnit.SECONDS.toMillis(HEALTH_CHECK_INTERVAL));

            ServiceDocumentQueryResult res = doQuery(String.format("$filter=serviceName eq %s",
                    ExampleService.class.getSimpleName()));

            assert (res.documentLinks != null);
            assert (res.documentLinks.size() == 1);
            DNSService.DNSServiceState serviceState =
                    Utils.fromJson((String) res.documents.get(res.documentLinks.get(0)),
                            DNSService.DNSServiceState.class);

            if (serviceState.serviceStatus ==
                    DNSService.DNSServiceState.ServiceStatus.AVAILABLE) {
                break;
            }

        }

        if (new Date().after(this.host.getTestExpiration())) {
            new TimeoutException();
        }

        /* Lets shutdown the remaining nodes as well. And verify the service status again */
        this.host.getInProcessHostMap().values().stream()
                .filter(hostToStop -> !hostToStop.getId().equals(this.host.getId()) && hostToStop
                        .isStarted()).forEach(hostToStop -> {
                            this.host.log("Stopping host %s", hostToStop);
                            this.host.stopHost(hostToStop);
                        });

        while (new Date().before(this.host.getTestExpiration())) {

            Thread.sleep(TimeUnit.SECONDS.toMillis(HEALTH_CHECK_INTERVAL));

            ServiceDocumentQueryResult res2 = doQuery(String.format(
                    "$filter=serviceName eq '%s' and serviceStatus eq AVAILABLE",
                    ExampleService.class.getSimpleName()));

            assert (res2.documentLinks != null);

            if (res2.documentLinks.size() == 0) {
                break;
            }
        }

        if (new Date().after(this.host.getTestExpiration())) {
            new TimeoutException();
        }

    }

    private ServiceDocumentQueryResult doQuery(String query) throws Throwable {
        URI odataQuery = UriUtils.buildUri(this.dnsHost, ServiceUriPaths.DNS + "/query", query);
        final ServiceDocumentQueryResult[] qr = { null };
        Operation get = Operation.createGet(odataQuery)
                .setCompletion((ox, ex) -> {
                    if (ex != null) {
                        this.host.failIteration(ex);
                    }
                    ServiceDocumentQueryResult qr1 = ox.getBody(ServiceDocumentQueryResult.class);
                    qr[0] = qr1;
                    this.host.completeIteration();
                });

        this.host.testStart(1);
        this.host.send(get);
        this.host.testWait();

        ServiceDocumentQueryResult res = qr[0];

        assertNotNull(res);
        assertNotNull(res.documents);

        return res;
    }
}
