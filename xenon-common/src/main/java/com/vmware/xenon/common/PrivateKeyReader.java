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

import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.Key;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 *  Read private key file(PKCS#8).
 */
public class PrivateKeyReader {

    /**
     * Retrieve {@link PrivateKey} from PEM encoded PKCS#8 file.
     *
     * @param path path to the PKCS#8 PEM file
     * @param password null if not encrypted
     * @return
     */
    public static PrivateKey fromPem(Path path, String password) {
        PrivateKey privateKey;
        try {
            byte[] der = getDer(path);
            KeySpec keySpec = getKeySpec(der, password);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            privateKey = kf.generatePrivate(keySpec);
        } catch (Exception e) {
            String msg = String.format("Failed to read PKCS#8 PEM file. file=%s", path);
            Utils.logWarning(msg);
            throw new RuntimeException(msg, e);
        }
        return privateKey;
    }

    /**
     * Convert PKCS#8 PEM file to DER encoded private key
     */
    private static byte[] getDer(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path);

        // remove header and footer, combine to single string without line breaks
        String base64Text = lines.subList(1, lines.size() - 1).stream().collect(joining());

        Decoder decoder = Base64.getDecoder();
        return decoder.decode(base64Text);
    }

    private static KeySpec getKeySpec(byte[] encodedKey, String password) throws Exception {
        KeySpec keySpec;
        if (password == null) {
            keySpec = new PKCS8EncodedKeySpec(encodedKey);
        } else {
            // decrypt private key
            PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());

            EncryptedPrivateKeyInfo privateKeyInfo = new EncryptedPrivateKeyInfo(encodedKey);
            String algorithmName = privateKeyInfo.getAlgName();
            Cipher cipher = Cipher.getInstance(algorithmName);
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(algorithmName);

            Key pbeKey = secretKeyFactory.generateSecret(pbeKeySpec);
            AlgorithmParameters algParams = privateKeyInfo.getAlgParameters();
            cipher.init(Cipher.DECRYPT_MODE, pbeKey, algParams);
            keySpec = privateKeyInfo.getKeySpec(cipher);
        }
        return keySpec;
    }
}
