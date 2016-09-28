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

import java.net.URI;

import org.junit.Test;

import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestAuthentication extends BasicTestCase {

    public static class TestAuthenticationService extends StatelessService {

        public static final String SELF_LINK = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHN,
                "test");

        @Override
        public void authorizeRequest(Operation op) {
            op.complete();
        }

        @Override
        public boolean queueRequest(Operation op) {
            op.addResponseHeader(Operation.LOCATION_HEADER, "http://www.vmware.com");
            op.setStatusCode(Operation.STATUS_CODE_MOVED_TEMP);
            op.complete();
            return true;
        }
    }

    @Override
    public void beforeHostStart(VerificationHost host) {
        // Set the AuthenticationService
        host.setAuthenticationService(new TestAuthenticationService());
        host.setAuthorizationEnabled(true);
    }

    @Test
    public void testAuthenticationServiceRedirect() {

        assertEquals(UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHN,
                "test"), host.getAuthenticationServiceUri().getPath());

        // Do any call on the host
        URI getUri = UriUtils.buildUri(this.host, "/");
        this.host.testStart(1);
        this.host.send(Operation
                .createGet(getUri)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }
                            if (o.getStatusCode() != Operation.STATUS_CODE_MOVED_TEMP) {
                                this.host.failIteration(new IllegalStateException(
                                        "Operation did not complete with proper status code"));
                                return;
                            }
                            if (!o.getResponseHeader(Operation.LOCATION_HEADER).equals(
                                    "http://www.vmware.com")) {
                                this.host.failIteration(new IllegalStateException(
                                        "Operation did not complete with proper location header"));
                                return;
                            }
                            this.host.completeIteration();
                        })
                .forceRemote());
        this.host.testWait();
    }
}
