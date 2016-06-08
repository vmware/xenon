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

import java.util.Map.Entry;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Test;

public class TestSigner extends TestCase {

    @Test
    public void testEmptyClaims() throws Exception {
        Signer signer = new Signer("secret".getBytes());
        Rfc7519Claims claims = new Rfc7519Claims();
        String jwt = signer.sign(claims);
        assertEquals(
                "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.e30.DMCAvRgzrcf5w0Z879BsqzcrnDFKBY_GN6c3qKOUFtQ",
                jwt);
    }

    @Test
    public void testRfc7515A1() throws Throwable {
        Rfc7515A1Claims.Builder builder = new Rfc7515A1Claims.Builder();
        builder.setIssuer("joe");
        builder.setExpirationTime(new Long(1300819380));
        builder.setIsRoot(true);
        Rfc7519Claims claims = builder.getResult();
        Signer signer = new Rfc7515A1Signer(rfc7515A1key);
        assertEquals(rfc7515A1jwt, signer.sign(claims));
    }

    /**
     * Overrides the {@code encode} function so that the JSON output
     * is identical to that described in the RFC.
     */
    public static class Rfc7515A1Signer extends Signer {
        public Rfc7515A1Signer(byte[] secret) {
            super(secret);
        }

        @Override
        protected String encode(Object object) {
            String json = encodeWithOrderedFields(object);
            json = json.replaceAll(",", ",\r\n ");
            return encode(json.getBytes(Constants.DEFAULT_CHARSET));
        }

        private String encodeWithOrderedFields(Object object) {
            JsonObject out = new JsonObject();
            JsonObject in = gson.toJsonTree(object).getAsJsonObject();

            // Order for header object
            transferProperty(in, out, "typ");
            transferProperty(in, out, "alg");

            // Order for claims object
            transferProperty(in, out, "iss");
            transferProperty(in, out, "exp");
            transferProperty(in, out, "http://example.com/is_root");

            // Remaining properties
            for (Entry<String, JsonElement> e : in.entrySet()) {
                out.add(e.getKey(), e.getValue());
            }

            return this.gson.toJson(out);
        }

        private void transferProperty(JsonObject in, JsonObject out, String key) {
            out.add(key, in.remove(key));
        }
    }
}
