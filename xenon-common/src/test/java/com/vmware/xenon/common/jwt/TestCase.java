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

import java.util.Base64;

import com.google.gson.annotations.SerializedName;
import org.junit.Before;

public class TestCase {
    byte[] secret = "foo".getBytes();

    byte[] rfc7515A1key;

    String rfc7515A1jwt;

    /**
     * Variables for testing the signer and verifier with the example listed in RFC 7515 Appendix A1.
     */
    @Before
    public void setRfc7515A1() {
        String encodedKey = "AyM1SysPpbyDfgZld3umj1qzKObwVMkoqQ-EstJQLr_T-1qS0gZH75aKtMN3Yj0iPS4hcgUuTwjAzZr1Z9CAow";
        this.rfc7515A1key = Base64.getUrlDecoder().decode(encodedKey);

        StringBuilder jwt = new StringBuilder();
        jwt.append("eyJ0eXAiOiJKV1QiLA0KICJhbGciOiJIUzI1NiJ9");
        jwt.append(".");
        jwt.append("eyJpc3MiOiJqb2UiLA0KICJleHAiOjEzMDA4MTkzODAsDQogImh0dHA6Ly9leGFtcGxlLmNvbS9pc19yb290Ijp0cnVlfQ");
        jwt.append(".");
        jwt.append("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk");
        this.rfc7515A1jwt = jwt.toString();
    }

    public static class Rfc7515A1Claims extends Rfc7519Claims {
        @SerializedName("http://example.com/is_root")
        private boolean isRoot;

        public boolean getIsRoot() {
            return this.isRoot;
        }

        public static class Builder extends Rfc7519Builder<Rfc7515A1Claims> {
            public Builder() {
                super(Rfc7515A1Claims.class);
            }

            public Builder setIsRoot(boolean isRoot) {
                getInstance().isRoot = isRoot;
                return this;
            }
        }
    }

    protected String b64e(byte[] payload) {
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        return encoder.encodeToString(payload);
    }

    protected String b64e(String payload) {
        return b64e(payload.getBytes(Constants.DEFAULT_CHARSET));
    }

    protected String b64d(String payload) {
        byte[] decoded = Base64.getUrlDecoder().decode(payload);
        return new String(decoded, Constants.DEFAULT_CHARSET);
    }

    protected String sign(String header, String payload) throws Exception {
        String input = String.format("%s.%s", header, payload);
        byte[] signature = Algorithm.HS256.sign(input.getBytes(Constants.DEFAULT_CHARSET),
                this.secret);
        return b64e(signature);
    }
}
