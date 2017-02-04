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
import java.util.UUID;
import java.util.function.Function;

import org.junit.Test;

import com.vmware.xenon.common.ServiceHost.Arguments;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.authn.BasicAuthenticationService;

public class TestAuthUtils {

    @Test
    public void defaultBehavior() throws Exception {
        Arguments args = new Arguments();
        args.id = "id";
        args.port = 0;
        args.authProviderHostUri = "http://auth-service.somewhere.local:4444";

        VerificationHost customHost = new VerificationHost();
        VerificationHost.initialize(customHost, args);

        String link = "/link-to-sth";
        URI uri = AuthUtils.buildAuthProviderHostUri(customHost, link);
        assertEquals(URI.create(args.authProviderHostUri + link), uri);
    }

    @Test
    public void customClaimsBuilderWithCustomAuth() throws Throwable {
        Arguments args = new Arguments();
        args.id = "id";
        args.port = 0;
        args.authProviderHostUri = "http://auth-service.somewhere.local:4444";

        VerificationHost customHost = new VerificationHost();
        VerificationHost.initialize(customHost, args);

        // random authServiceSelfLink: dont polute global state
        String authServiceSelfLink = "/sso-" + UUID.randomUUID();
        BasicAuthenticationService myService = new BasicAuthenticationService();
        myService.setSelfLink(authServiceSelfLink);

        customHost.setAuthenticationService(myService);

        Function<Claims, String> f = claimsObj -> {
            return authServiceSelfLink + "/" + claimsObj.getSubject();
        };
        AuthUtils.registerUserLinkBuilder(authServiceSelfLink, f);

        try {
            customHost.start();

            Claims claims = new Claims.Builder()
                    .setSubject("test-subject")
                    .getResult();

            URI uri = AuthUtils.buildUserUriFromClaims(customHost, claims);
            assertEquals(URI.create(args.authProviderHostUri + authServiceSelfLink + "/" + claims.getSubject()), uri);

        } finally {
            customHost.stop();
        }
    }
}
