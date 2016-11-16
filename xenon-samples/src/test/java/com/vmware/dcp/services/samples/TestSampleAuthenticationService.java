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
import static org.junit.Assert.assertNotNull;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmware.xenon.common.Claims;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.http.netty.CookieJar;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;
import com.vmware.xenon.services.samples.SampleAuthenticationService;

public class TestSampleAuthenticationService {

    private VerificationHost createAndStartHost()
            throws Throwable {
        VerificationHost host = VerificationHost.create(0);
        host.setAuthorizationEnabled(true);

        // set the authentication service as SampleAuthenticationService
        host.setAuthenticationService(new SampleAuthenticationService());

        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        host.start();
        return host;
    }

    @Test
    public void testSampleAuthenticationService() throws Throwable {

        VerificationHost host = createAndStartHost();

        // test un-authenticated request redirect
        testUnauthenticatedRequestRedirect(host);

        // test accessToken request
        testAccessTokenRequest(host);

        // test token verification of valid token
        testTokenVerification(host);
    }

    private void testUnauthenticatedRequestRedirect(VerificationHost host) {
        // make a un-authenticated request on the host without any token
        Operation requestOp = Operation.createGet(host.getUri());
        Operation responseOp = host.getTestRequestSender().sendAndWait(requestOp);

        // check the redirect response
        assertEquals(Operation.STATUS_CODE_MOVED_TEMP, responseOp.getStatusCode());

        // check the location header to redirect
        assertEquals("http://www.vmware.com",
                responseOp.getResponseHeader(Operation.LOCATION_HEADER));
    }

    private void testAccessTokenRequest(VerificationHost host) {
        // make a request to get the accessToken for the authentication service
        Operation requestOp = Operation.createGet(host, SampleAuthenticationService.SELF_LINK)
                .forceRemote();
        Operation responseOp = host.getTestRequestSender().sendAndWait(requestOp);

        String cookieHeader = responseOp.getResponseHeader(Operation.SET_COOKIE_HEADER);
        assertNotNull(cookieHeader);

        Map<String, String> cookieElements = CookieJar.decodeCookies(cookieHeader);

        // assert the auth token cookie
        assertEquals(SampleAuthenticationService.ACCESS_TOKEN,
                cookieElements.get(AuthenticationConstants.REQUEST_AUTH_TOKEN_COOKIE));

        // assert the auth token header
        assertEquals(SampleAuthenticationService.ACCESS_TOKEN,
                responseOp.getResponseHeader(Operation.REQUEST_AUTH_TOKEN_HEADER));
    }

    private void testTokenVerification(VerificationHost host) {
        TestRequestSender.setAuthToken(SampleAuthenticationService.ACCESS_TOKEN);
        // make a request to verification service
        Operation requestOp = Operation.createPost(host, SampleAuthenticationService.SELF_LINK)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);

        Operation responseOp = host.getTestRequestSender().sendAndWait(requestOp);
        Claims claims = responseOp.getBody(Claims.class);
        assertNotNull(claims);

        TestRequestSender.clearAuthToken();
    }
}
