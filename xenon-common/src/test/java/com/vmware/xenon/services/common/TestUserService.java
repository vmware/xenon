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
import java.util.HashSet;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.services.common.UserService.UserState;

public class TestUserService extends BasicReusableHostTestCase {
    private URI factoryUri;
    private TestRequestSender sender;

    @Before
    public void setUp() {
        this.factoryUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USERS);
        this.sender = new TestRequestSender(this.host);
    }

    @After
    public void cleanUp() throws Throwable {
        this.host.deleteAllChildServices(this.factoryUri);
    }

    @Test
    public void testFactoryPostAndDelete() throws Throwable {
        UserState state = new UserState();
        state.email = "jane@doe.com";

        UserState outState = this.sender.postThenGetBody(ServiceUriPaths.CORE_AUTHZ_USERS,
                UserState.class, op -> op.setBody(state));

        assertEquals(state.email, outState.email);
    }


    @Test
    public void testFactoryIdempotentPost() throws Throwable {
        UserState state = new UserState();
        state.email = "jane@doe.com";
        state.documentSelfLink = state.email;

        UserState responseState = (UserState) this.host.verifyPost(UserState.class,
                ServiceUriPaths.CORE_AUTHZ_USERS,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.email,responseState.email);

        responseState = (UserState) this.host.verifyPost(UserState.class,
                ServiceUriPaths.CORE_AUTHZ_USERS,
                state,
                Operation.STATUS_CODE_NOT_MODIFIED);

        assertEquals(state.email,responseState.email);

        state.email = "john@doe.com";

        responseState = (UserState) this.host.verifyPost(UserState.class,
                ServiceUriPaths.CORE_AUTHZ_USERS,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.email, responseState.email);
    }

    @Test
    public void testFactoryPostFailure() throws Throwable {
        UserState state = new UserState();
        state.email = "not an email";

        Operation[] outOp = new Operation[1];
        Throwable[] outEx = new Throwable[1];

        Operation op = Operation.createPost(this.factoryUri).setBody(state);
        this.sender.sendExpectFailure(op, (o, e)-> {
            outOp[0] = o;
            outEx[0] = e;
        });

        assertEquals(Operation.STATUS_CODE_FAILURE_THRESHOLD, outOp[0].getStatusCode());
        assertEquals("email is invalid", outEx[0].getMessage());
    }

    @Test
    public void testPatch() throws Throwable {
        UserState state = new UserState();
        state.email = "jane@doe.com";
        state.documentSelfLink = UUID.randomUUID().toString();
        state.userGroupLinks = new HashSet<String>();
        state.userGroupLinks.add("link1");
        state.userGroupLinks.add("link2");


        UserState responseState = (UserState) this.host.verifyPost(UserState.class,
                ServiceUriPaths.CORE_AUTHZ_USERS,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.email,responseState.email);
        assertEquals(state.userGroupLinks.size(),state.userGroupLinks.size());

        state.email = "john@doe.com";
        state.userGroupLinks.clear();
        state.userGroupLinks.add("link2");
        state.userGroupLinks.add("link3");

        String path = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, state.documentSelfLink);

        UserState patchedState = this.sender.patchThenGetBody(path, UserState.class, op -> op.setBody(state));
        assertEquals(state.email, patchedState.email);
        assertEquals(3, patchedState.userGroupLinks.size());

    }
}
