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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class TestEndToEnd {
    private final byte[] secret = "secret".getBytes();

    private Signer signer;
    private Verifier verifier;

    @Before
    public void setup() {
        this.signer = new Signer(this.secret);
        this.verifier = new Verifier(this.secret);
    }

    @Test
    public void testClaimsSubclass() throws Exception {
        CustomClaims in = new CustomClaims();
        in.customProperty = "hello world";
        String jwt = this.signer.sign(in);

        CustomClaims out = this.verifier.verify(jwt, CustomClaims.class);
        assertEquals(in.customProperty, out.customProperty);
    }

    public static class CustomClaims extends Rfc7519Claims {
        public String customProperty;
    }
}
