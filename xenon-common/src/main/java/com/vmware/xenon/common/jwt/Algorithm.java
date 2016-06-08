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

package com.vmware.xenon.common.jwt;

import java.security.GeneralSecurityException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public enum Algorithm {
    HS256("HmacSHA256"), HS384("HmacSHA384"), HS512("HmacSHA512");

    public static class UnknownAlgorithmException extends RuntimeException {
        private static final long serialVersionUID = 7719318497737763328L;

        public UnknownAlgorithmException(String name) {
            super(String.format("Unknown algorithm: %s", name));
        }
    }

    public static Algorithm fromString(String name) {
        if (name == null) {
            return null;
        }

        switch (name) {
        case "HS256":
            return HS256;
        case "HS384":
            return HS384;
        case "HS512":
            return HS512;
        default:
            throw new UnknownAlgorithmException(name);
        }
    }

    Algorithm(String value) {
        this.value = value;
    }

    private String value;

    public String getValue() {
        return this.value;
    }

    public byte[] sign(byte[] payload, byte[] secret) throws GeneralSecurityException {
        Mac mac = Mac.getInstance(this.value);
        mac.init(new SecretKeySpec(secret, this.value));
        return mac.doFinal(payload);
    }
}
