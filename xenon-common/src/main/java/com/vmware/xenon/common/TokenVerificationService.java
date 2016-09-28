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

import java.security.GeneralSecurityException;
import java.util.logging.Level;

import com.vmware.xenon.common.jwt.Verifier;
import com.vmware.xenon.common.jwt.Verifier.TokenException;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TokenVerificationService extends StatelessService {

    public static String SELF_LINK = ServiceUriPaths.CORE_AUTHN_VERIFICATION;

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handlePost(Operation op) {
        String token = op.getBody(String.class);
        if (token == null) {
            op.fail(new IllegalArgumentException("Token is empty"));
        }
        if (this.getHost().getAuthenticationServiceUri() != null) {
            // request the verification from the authenticationService
            Operation verifyOp = Operation.createPost(this.getHost().getAuthenticationServiceUri())
                    .setBody(token)
                    .setCompletion((responseOp, ex) -> {
                        if (ex != null) {
                            op.fail(ex);
                        } else {
                            Claims claim = responseOp.getBody(Claims.class);
                            if (claim == null) {
                                op.fail(new IllegalArgumentException("Claims object is null"));
                            } else {
                                op.setBody(claim);
                                op.complete();
                            }
                        }
                    });
            sendRequest(verifyOp);
        } else {
            try {
                // use JWT token verifier for basic auth
                Verifier verifier = this.getHost().getTokenVerifier();
                Claims claims = verifier.verify(token, Claims.class);
                op.setBody(claims);
                op.complete();
            } catch (TokenException | GeneralSecurityException e) {
                log(Level.INFO, "Error verifying token: %s", e);
                op.fail(e);
            }
        }
    }
}
