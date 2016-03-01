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
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.jwt.Verifier.InvalidSignatureException;
import com.vmware.xenon.common.jwt.Verifier.InvalidTokenException;

public class TestVerifier extends TestCase {

    private Verifier verifier;

    @Before
    public void initializeVerifier() {
        this.verifier = new Verifier(secret);
    }

    @Test
    public void testRfc7515A1() throws Throwable {
        Verifier verifier = new Verifier(rfc7515A1key);
        Rfc7515A1Claims claims = verifier.verify(rfc7515A1jwt, Rfc7515A1Claims.class);
        assertEquals("joe", claims.getIssuer());
        assertEquals(new Long(1300819380), claims.getExpirationTime());
        assertEquals(true, claims.getIsRoot());
    }

    @Test(expected = InvalidTokenException.class)
    public void invalidSeparators() throws Exception {
        this.verifier.verify(".....");
    }

    @Test(expected = InvalidTokenException.class)
    public void invalidHeader() throws Exception {
        this.verifier.verify("header.payload.signature");
    }

    @Test(expected = InvalidTokenException.class)
    public void nullHeader() throws Exception {
        String header = b64e("null");
        this.verifier.verify(String.format("%s.payload.signature", header));
    }

    @Test(expected = InvalidTokenException.class)
    public void emptyObjectHeader() throws Exception {
        String header = b64e("{}");
        this.verifier.verify(String.format("%s.payload.signature", header));
    }

    @Test(expected = InvalidTokenException.class)
    public void invalidTypeHeader() throws Exception {
        String header = b64e("{\"typ\": 123}");
        this.verifier.verify(String.format("%s.payload.signature", header));
    }

    @Test(expected = InvalidTokenException.class)
    public void invalidAlgorithmHeader() throws Exception {
        String header = b64e("{\"typ\": \"JWT\", \"alg\": \"rot13\"}");
        this.verifier.verify(String.format("%s.payload.signature", header));
    }

    @Test(expected = InvalidSignatureException.class)
    public void invalidSignature() throws Exception {
        String header = b64e("{\"typ\": \"JWT\", \"alg\": \"HS256\"}");
        String payload = b64e("f00dcafe");
        this.verifier.verify(String.format("%s.%s.deadbeef", header, payload));
    }

    @Test(expected = InvalidTokenException.class)
    public void invalidPayload() throws Exception {
        String header = b64e("{\"typ\": \"JWT\", \"alg\": \"HS256\"}");
        String payload = b64e("f00dcafe");
        String jwt = String.format("%s.%s.%s", header, payload, sign(header, payload));
        this.verifier.verify(jwt);
    }

    @Test(expected = InvalidTokenException.class)
    public void nullPayload() throws Exception {
        String header = b64e("{\"typ\": \"JWT\", \"alg\": \"HS256\"}");
        String payload = b64e("null");
        String jwt = String.format("%s.%s.%s", header, payload, sign(header, payload));
        this.verifier.verify(jwt);
    }

    @Test
    public void emptyPayload() throws Exception {
        String header = b64e("{\"typ\": \"JWT\", \"alg\": \"HS256\"}");
        String payload = b64e("{}");
        String jwt = String.format("%s.%s.%s", header, payload, sign(header, payload));
        Rfc7519Claims claims = this.verifier.verify(jwt);
        assertNotNull(claims);
    }
}
