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

package com.vmware.dcp.common.jwt;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Signer {
    protected Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

    private byte[] secret;

    protected Gson gson;

    public Signer(byte[] secret) {
        this(secret, new GsonBuilder().create());
    }

    public Signer(byte[] secret, Gson gson) {
        this.secret = Arrays.copyOf(secret, secret.length);
        this.gson = gson;
    }

    public <T extends Rfc7519Claims> String sign(T claims) throws GeneralSecurityException {
        return sign(claims, null);
    }

    public <T extends Rfc7519Claims> String sign(T claims, Algorithm algorithm)
            throws GeneralSecurityException {
        if (algorithm == null) {
            algorithm = Constants.DEFAULT_ALGORITHM;
        }

        Header header = new Header();
        header.algorithm = algorithm.name();
        header.type = Constants.JWT_TYPE;

        // Build first part of JWT (to be signed)
        StringBuilder builder = new StringBuilder();
        builder.append(encode(header));
        builder.append(Constants.JWT_SEPARATOR);

        String encClaims = encode(claims);
        builder.append(encClaims);

        // Compute and append signature
        byte[] signature = algorithm.sign(builder.toString().getBytes(Constants.DEFAULT_CHARSET),
                this.secret);
        builder.append(Constants.JWT_SEPARATOR);
        builder.append(encode(signature));

        return builder.toString();
    }

    protected String encode(byte[] payload) {
        return new String(this.encoder.encode(payload), Constants.DEFAULT_CHARSET);
    }

    protected String encode(Object object) {
        String json = this.gson.toJson(object);
        return encode(json.getBytes(Constants.DEFAULT_CHARSET));
    }
}
