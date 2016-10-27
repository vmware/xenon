/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.jwt;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.xenon.common.PrivateKeyReader;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;

public final class JWTUtils {

    private static final Logger logger = Logger.getLogger(JWTUtils.class.getName());

    private JWTUtils() {

    }

    /**
     * Default logic to retrieve JWT secret.
     *
     * Currently using private key if provided, otherwise use default string.
     */
    public static byte[] getJWTSecret(URI privateKeyFileUri, String privateKeyPassphrase,
            boolean isAuthorizationEnabled) throws IOException {
        byte[] secret;

        if (privateKeyFileUri != null) {
            Path privateKeyFilePath = Paths.get(privateKeyFileUri);
            PrivateKey privateKey = PrivateKeyReader
                    .fromPem(privateKeyFilePath, privateKeyPassphrase);
            secret = privateKey.getEncoded();
        } else {
            if (isAuthorizationEnabled) {
                String msg = "\n\n"
                        + "########################################################\n"
                        + "##  Using default secret to sign/verify JSON(JWT)     ##\n"
                        + "##  This is NOT secure. Please consider enabling SSL. ##\n"
                        + "########################################################\n"
                        + "\n";
                logger.log(Level.WARNING, msg);
            }
            secret = AuthenticationConstants.DEFAULT_JWT_SECRET.getBytes(Utils.CHARSET);
        }

        return secret;
    }

}
