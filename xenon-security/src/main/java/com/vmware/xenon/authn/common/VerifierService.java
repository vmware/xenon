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

package com.vmware.xenon.authn.common;

import java.util.logging.Level;

import com.vmware.xenon.common.Claims;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;

public abstract class VerifierService extends StatelessService {

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handlePost(Operation op) {
        handleVerification(op);
    }

    /**
     * handleVerification function is triggered when a POST request is made to the verification
     * service of any auth provider. It sets the operation state as the claimsVerificationState
     * after successful verification
     */
    public void handleVerification(Operation parentOp) {
        String token = parentOp.getRequestHeader("token");
        Claims claims;
        try {
            claims = verify(token);
        } catch (Exception e) {
            log(Level.WARNING , "Exception while verifying the token : %s" , e.getMessage());
            parentOp.fail(Operation.STATUS_CODE_NOT_FOUND);
            return ;
        }
        parentOp.setStatusCode(Operation.STATUS_CODE_OK);
        parentOp.setBodyNoCloning(claims).complete();
        return ;
    }

    /**
     * The auth provider has to implement this method which will decode the token
     * and generate a ClaimsVerificationState object in return. If unable to decode the token or
     * create the object, throw an appropriate exception
     * @return ClaimsVerificationState
     * @throws Exception
     */
    public abstract Claims verify(String token) throws Exception ;
}
