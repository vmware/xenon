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
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class Verifier {
    protected Base64.Decoder decoder = Base64.getUrlDecoder();

    private byte[] secret;

    protected Gson gson;

    public Verifier(byte[] secret) {
        this(secret, new GsonBuilder().create());
    }

    public Verifier(byte[] secret, Gson gson) {
        this.secret = Arrays.copyOf(secret, secret.length);
        this.gson = gson;
    }

    public Rfc7519Claims verify(String jwt) throws TokenException, GeneralSecurityException {
        return verify(jwt, Rfc7519Claims.class);
    }

    public <T extends Rfc7519Claims> T verify(String jwt, Class<T> klass) throws TokenException,
            GeneralSecurityException {
        int headerIndex = jwt.indexOf(Constants.JWT_SEPARATOR, 0);
        if (headerIndex == -1 || headerIndex == 0) {
            throw new InvalidTokenException("Separator for header not found");
        }

        int payloadIndex = jwt.indexOf(Constants.JWT_SEPARATOR, headerIndex + 1);
        if (payloadIndex == -1 || payloadIndex == headerIndex + 1) {
            throw new InvalidTokenException("Separator for payload not found");
        }

        String encodedHeader = jwt.substring(0, headerIndex);
        String encodedPayload = jwt.substring(headerIndex + 1, payloadIndex);
        String encodedSignature = jwt.substring(payloadIndex + 1);

        Header header = null;
        try {
            header = decode(encodedHeader, Header.class);
        } catch (JsonSyntaxException ex) {
            throw new InvalidTokenException(String.format("Invalid header JSON: %s",
                    ex.getMessage()));
        }

        if (header == null) {
            throw new InvalidTokenException("Invalid header: null");
        }

        if (header.type == null) {
            throw new InvalidTokenException("Invalid header: no type");
        }

        if (!header.type.equals(Constants.JWT_TYPE)) {
            throw new InvalidTokenException(String.format("Header type: %s", header.type));
        }

        Algorithm algorithm;
        try {
            algorithm = Algorithm.fromString(header.algorithm);
        } catch (Algorithm.UnknownAlgorithmException ex) {
            throw new InvalidTokenException(ex.getMessage());
        }

        // Verify signature
        byte[] bytesToSign = jwt.substring(0, payloadIndex).getBytes(Constants.DEFAULT_CHARSET);
        byte[] expectedSignature = algorithm.sign(bytesToSign, this.secret);
        byte[] actualSignature = decode(encodedSignature);
        if (!MessageDigest.isEqual(expectedSignature, actualSignature)) {
            throw new InvalidSignatureException("Signature does not match");
        }

        T payload;
        try {
            payload = decode(encodedPayload, klass);
        } catch (JsonSyntaxException ex) {
            throw new InvalidTokenException(String.format("Invalid payload JSON: %s",
                    ex.getMessage()));
        }

        if (payload == null) {
            throw new InvalidTokenException("Invalid payload: null");
        }

        return payload;
    }

    protected byte[] decode(String payload) {
        return this.decoder.decode(payload.getBytes(Constants.DEFAULT_CHARSET));
    }

    protected <T> T decode(String payload, Class<T> klass) {
        String json = new String(decode(payload), Constants.DEFAULT_CHARSET);
        return this.gson.fromJson(json, klass);
    }

    public class TokenException extends Exception {
        private static final long serialVersionUID = 1640724864336370400L;

        TokenException(String message) {
            super(message);
        }
    }

    public class InvalidTokenException extends TokenException {
        private static final long serialVersionUID = -1387613440530397711L;

        InvalidTokenException(String message) {
            super(message);
        }
    }

    public class InvalidSignatureException extends TokenException {
        private static final long serialVersionUID = -8683380846847289365L;

        InvalidSignatureException(String message) {
            super(message);
        }
    }
}
