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

import java.net.URI;

import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.UserService.UserState;

public class TestUserService extends BasicTestCase {

    @Test
    public void testFactoryPost() throws Throwable {
        UserState state = new UserState();
        state.email = "jane@doe.com";

        final UserState[] outState = new UserState[1];

        URI uri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USERS);
        Operation op = Operation.createPost(uri)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    outState[0] = o.getBody(UserState.class);
                    this.host.completeIteration();
                });

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();

        assertEquals(state.email, outState[0].email);
    }

    @Test
    public void testFactoryPostDuplicate() throws Throwable {
        UserState state = new UserState();
        state.email = "jane@doe.com";
        state.documentSelfLink = state.email;

        final UserState[] outState = new UserState[1];

        URI uri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USERS);
        Operation op = Operation.createPost(uri)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    outState[0] = o.getBody(UserState.class);
                    this.host.completeIteration();
                });

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();

        op = Operation.createPost(uri)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    if (o.getStatusCode() == Operation.STATUS_CODE_NOT_MODIFIED) {
                        this.host.completeIteration();
                        return;
                    }
                    this.host.failIteration(new IllegalStateException("Status code 304 expected"));
                });

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();
        assertEquals(state.email, outState[0].email);
    }

    @Test
    public void testFactoryPostFailure() throws Throwable {
        UserState state = new UserState();
        state.email = "not an email";

        Operation[] outOp = new Operation[1];
        Throwable[] outEx = new Throwable[1];

        URI uri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USERS);
        Operation op = Operation.createPost(uri)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        outOp[0] = o;
                        outEx[0] = e;
                        this.host.completeIteration();
                        return;
                    }

                    // No exception, fail test
                    this.host.failIteration(new IllegalStateException("expected failure"));
                });

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();

        assertEquals(Operation.STATUS_CODE_FAILURE_THRESHOLD, outOp[0].getStatusCode());
        assertEquals("email is invalid", outEx[0].getMessage());
    }
}
