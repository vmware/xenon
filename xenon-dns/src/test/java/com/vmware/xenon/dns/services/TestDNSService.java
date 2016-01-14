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

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;

import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleFactoryService;
import com.vmware.xenon.services.common.ServiceUriPaths;


/**
 * Service that represents DNS records
 */

public class TestDNSService extends BasicTestCase {

    public VerificationHost dnsHost;
    private static Long HEALTH_CHECK_INTERVAL = 1L;
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
        h1.waitForServiceAvailable(ExampleFactoryService.SELF_LINK);
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
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        /*
        Register the Example Factory with the DNS service
        */

        for (VerificationHost h1 : this.host.getInProcessHostMap().values()) {
            DNSService.DNSServiceState.Check check = new DNSService.DNSServiceState.Check();
            check.url = h1.getUri().toString() + ExampleFactoryService.SELF_LINK;
            check.interval = HEALTH_CHECK_INTERVAL;
            Operation.CompletionHandler completionHandler = (o, e) -> {
                assert (e == null);
                this.host.completeIteration();
            };
            this.host.testStart(1);
            h1.sendRequest(DNSUtils.registerServiceOp(this.dnsHost.getPublicUri(), h1,
                    ExampleFactoryService.SELF_LINK,
                    ExampleFactoryService.class.getSimpleName(), null, check).setCompletion(completionHandler));
            this.host.testWait();
        }


        /* Verify records exist at DNS service */
        Map<String, Object> out = doQuery(String.format("$filter=serviceName eq %s",
                ExampleFactoryService.class.getSimpleName()));

        assert (out != null);
        assert (out.keySet().size() == this.nodeCount);
    }

    @Test
    public void serviceFailureTest() throws Throwable {

        configureHost(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);


        /*
        Register the Example Factory with the DNS service
        */
        this.host.testStart(3);
        for (VerificationHost h1 : this.host.getInProcessHostMap().values()) {
            DNSService.DNSServiceState.Check check = new DNSService.DNSServiceState.Check();
            check.url = h1.getUri().toString() + ExampleFactoryService.SELF_LINK;
            check.interval = HEALTH_CHECK_INTERVAL;
            Operation.CompletionHandler completionHandler = (o, e) -> {
                assert (e == null);
                this.host.completeIteration();
            };
            h1.sendRequest(DNSUtils.registerServiceOp(this.dnsHost.getPublicUri(), h1,
                    ExampleFactoryService.SELF_LINK,
                    ExampleFactoryService.class.getSimpleName(), null, check).setCompletion(completionHandler));
        }
        this.host.testWait();
        /* Verify records exist at DNS service */
        Map<String, Object> out = doQuery(String.format("$filter=serviceName eq %s",
                ExampleFactoryService.class.getSimpleName()));

        assert (out != null);
        assert (out.keySet().size() == this.nodeCount);

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
        /*
            Modify this to use a better sleep mechanism
         */
        Thread.sleep( TimeUnit.SECONDS.toMillis(2 * HEALTH_CHECK_INTERVAL.intValue()));

         /* Verify records exist at DNS service */
        Map<String, Object> out1 = doQuery(String.format("$filter=serviceName eq '%s' and available eq true",
                ExampleFactoryService.class.getSimpleName()));

        assert (out1 != null);
        assert (out1.keySet().size() == this.nodeCount - 1);


    }

    private Map<String, Object> doQuery(String query) throws Throwable {
        URI odataQuery = UriUtils.buildUri(this.dnsHost, ServiceUriPaths.DNS + "/query",query);
        final ServiceDocumentQueryResult[] qr = {null};
        Operation get = Operation.createGet(odataQuery)
                .setReferer(this.host.getUri())
                .setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON)
                .setCompletion((ox, ex) -> {
                    if (ex != null) {
                        this.host.failIteration(ex);
                    }

                    ServiceDocumentQueryResult qr1 = ox.getBody(ServiceDocumentQueryResult.class);
                    qr[0] = qr1;
                    this.dnsHost.completeIteration();
                });


        this.dnsHost.testStart(1);
        this.dnsHost.sendRequest(get);
        this.dnsHost.testWait();

        ServiceDocumentQueryResult res = qr[0];

        assertNotNull(res);
        assertNotNull(res.documents);

        return res.documents;
    }
}