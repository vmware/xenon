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

package com.vmware.xenon.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivateKey;

import org.junit.Test;

public class TestPrivateKeyReader {

    @Test
    public void readNonEncryptedPemPrivateKey() throws Exception {
        // non encrypted private key
        Path path = Paths.get(getClass().getResource("/ssl/server.pem").toURI());
        PrivateKey privateKey = PrivateKeyReader.fromPem(path, null);
        assertNotNull(privateKey);
        assertEquals("PKCS#8", privateKey.getFormat());

        try {
            PrivateKeyReader.fromPem(path, "");
            fail("Should fail with empty password");
        } catch (Exception ex) {
        }

        try {
            PrivateKeyReader.fromPem(path, "password");
            fail("Should fail with any password for non encrypted key");
        } catch (Exception ex) {
        }
    }

    @Test
    public void readEncryptedPemPrivateKey() throws Exception {
        // encrypted private key
        Path path = Paths.get(getClass().getResource("/ssl/server-with-pass.p8").toURI());
        PrivateKey privateKey = PrivateKeyReader.fromPem(path, "password");
        assertNotNull(privateKey);
        assertEquals("PKCS#8", privateKey.getFormat());

        try {
            PrivateKeyReader.fromPem(path, null);
            fail("Should fail with null password");
        } catch (Exception ex) {
        }

        try {
            PrivateKeyReader.fromPem(path, "");
            fail("Should fail with empty password");
        } catch (Exception ex) {
        }

        try {
            PrivateKeyReader.fromPem(path, "wrong-password");
            fail("Should fail with wrong password");
        } catch (Exception ex) {
        }
    }
}
