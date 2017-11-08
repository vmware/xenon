/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.UUID;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;

public final class BenchmarkDataFactory {
    private static Random random = new Random(0);

    private BenchmarkDataFactory() {

    }

    static QueryValidationServiceState makeDefaultTestDoc() {
        QueryValidationServiceState document = VerificationHost.buildQueryValidationState();
        document.documentKind = Utils.buildKind(document.getClass());
        document.documentSelfLink = UUID.randomUUID().toString();
        document.documentVersion = 0;
        document.documentExpirationTimeMicros = Utils.getNowMicrosUtc();
        document.documentSourceLink = UUID.randomUUID().toString();
        document.documentOwner = UUID.randomUUID().toString();
        document.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        document.documentAuthPrincipalLink = UUID.randomUUID().toString();
        document.documentUpdateAction = UUID.randomUUID().toString();

        document.mapOfStrings = new LinkedHashMap<>();
        document.mapOfStrings.put("key1", "value1");
        document.mapOfStrings.put("key2", "value2");
        document.mapOfStrings.put("key3", "value3");
        document.binaryContent = document.documentKind.getBytes();
        document.booleanValue = false;
        document.doublePrimitive = 3;
        document.doubleValue = Double.valueOf(3);
        document.id = document.documentSelfLink;
        document.serviceLink = document.documentSelfLink;
        document.dateValue = new Date();
        document.listOfStrings = Arrays.asList("1", "2", "3", "4", "5");

        return document;
    }

    static ExampleServiceState makeDocOfJsonSizeKb(int sizeInKb) {
        ExampleServiceState res = new ExampleServiceState();
        res.documentSelfLink = "/examples/" + UUID.randomUUID();
        res.required = "true";
        res.name = "name " + new Date();
        res.id = res.documentSelfLink;
        res.documentUpdateAction = Action.PATCH.name();
        res.tags = new HashSet<>();
        res.keyValues = new HashMap<>();
        res.counter = System.currentTimeMillis();

        // inflate document until its compact json representation reaches given size
        // in KB. "8" is good enough an approximation. Even if you find better one, don't change
        // unless you want compare with results from before.
        for (int i = 0; i < sizeInKb * 8; i++) {
            res.tags.add(UUID.randomUUID().toString());
            res.keyValues.put(UUID.randomUUID().toString(), res.documentSelfLink);
        }

        return res;
    }

    public static byte[] randomBytes(int len) {
        byte[] res = new byte[len];
        random.nextBytes(res);
        return res;
    }

    public static String randomString(int len) {
        byte[] buffer = new byte[len / 2];
        random.nextBytes(buffer);
        return javax.xml.bind.DatatypeConverter.printHexBinary(buffer);
    }
}
