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

package com.vmware.dcp.services.samples;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.RequestRouter.Route;
import com.vmware.dcp.common.Service.Action;
import com.vmware.dcp.common.ServiceDocumentDescription;
import com.vmware.dcp.common.ServiceHost;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.test.VerificationHost;
import com.vmware.dcp.services.samples.BankAccountService.BankAccountServiceRequest;
import com.vmware.dcp.services.samples.BankAccountService.BankAccountServiceState;

public class TestBankAccountService {

    private VerificationHost host;

    @Before
    public void setUp() throws Exception {
        try {
            this.host = VerificationHost.create(0, null);
            this.host.start();
            // Start a factory for bank account service
            this.host.startServiceAndWait(BankAccountFactoryService.class,
                    BankAccountFactoryService.SELF_LINK);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        this.host.tearDown();
    }

    @Test
    public void testCRUD() throws Throwable {
        // locate factory and create a service instance
        URI factoryUri = UriUtils.buildUri(this.host, BankAccountFactoryService.class);
        this.host.testStart(1);
        BankAccountServiceState initialState = new BankAccountServiceState();
        double initialBalance = 100.0;
        initialState.balance = initialBalance;
        initialState.documentSelfLink = UUID.randomUUID().toString();
        URI childURI = UriUtils.buildUri(this.host, BankAccountFactoryService.SELF_LINK + "/"
                + initialState.documentSelfLink);
        BankAccountServiceState[] responses = new BankAccountServiceState[1];
        Operation post = Operation
                .createPost(factoryUri)
                .setBody(initialState).setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    responses[0] = o.getBody(BankAccountServiceState.class);
                    this.host.completeIteration();
                });
        this.host.send(post);
        this.host.testWait();
        assertEquals(initialBalance, responses[0].balance, 0);

        // deposit
        this.host.testStart(1);
        double depositAmount = 30.0;
        BankAccountServiceRequest body = new BankAccountServiceRequest();
        body.kind = BankAccountServiceRequest.Kind.DEPOSIT;
        body.amount = depositAmount;
        Operation patch = Operation
                .createPatch(childURI)
                .setBody(body).setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    responses[0] = o.getBody(BankAccountServiceState.class);
                    this.host.completeIteration();
                });
        this.host.send(patch);
        this.host.testWait();
        assertEquals(initialBalance + depositAmount, responses[0].balance, 0);

        // withdraw
        this.host.testStart(1);
        double withdrawAmount = 120.0;
        body = new BankAccountServiceRequest();
        body.kind = BankAccountServiceRequest.Kind.WITHDRAW;
        body.amount = withdrawAmount;
        patch = Operation
                .createPatch(childURI)
                .setBody(body).setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    responses[0] = o.getBody(BankAccountServiceState.class);
                    this.host.completeIteration();
                });
        this.host.send(patch);
        this.host.testWait();
        assertEquals(initialBalance + depositAmount - withdrawAmount, responses[0].balance, 0);

        // delete instance
        this.host.testStart(1);
        Operation delete = Operation
                .createDelete(childURI)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.send(delete);
        this.host.testWait();
    }

    @Test
    public void testGetTemplate() throws Throwable {
        // locate factory and create a service instance
        URI factoryUri = UriUtils.buildUri(this.host, BankAccountFactoryService.class);
        this.host.testStart(1);
        BankAccountServiceState initialState = new BankAccountServiceState();
        double initialBalance = 100.0;
        initialState.balance = initialBalance;
        initialState.documentSelfLink = UUID.randomUUID().toString();
        URI childURI = UriUtils.buildUri(this.host, BankAccountFactoryService.SELF_LINK + "/"
                + initialState.documentSelfLink);
        BankAccountServiceState[] responses = new BankAccountServiceState[1];
        Operation post = Operation
                .createPost(factoryUri)
                .setBody(initialState).setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    responses[0] = o.getBody(BankAccountServiceState.class);
                    this.host.completeIteration();
                });
        this.host.send(post);
        this.host.testWait();
        assertEquals(initialBalance, responses[0].balance, 0);

        // get template
        this.host.testStart(1);
        URI templateURI = UriUtils.extendUri(childURI, ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE);
        Operation get = Operation
                .createGet(templateURI)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }
                            responses[0] = o.getBody(BankAccountServiceState.class);
                            this.host.completeIteration();
                        });
        this.host.send(get);
        this.host.testWait();
        ServiceDocumentDescription sdd = responses[0].documentDescription;
        List<Route> patchRoutes = sdd.serviceRequestRoutes.get(Action.PATCH);
        assert (patchRoutes.size() == 2);
        patchRoutes.forEach(route -> {
            assert (route.description.equals("Deposit") || route.description
                    .equals("Withdraw"));
        });

        // delete instance
        this.host.testStart(1);
        Operation delete = Operation
                .createDelete(childURI)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.send(delete);
        this.host.testWait();
    }
}
