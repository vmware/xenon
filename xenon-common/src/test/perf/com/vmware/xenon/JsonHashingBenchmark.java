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

package com.vmware.xenon;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.UUID;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;

@State(Scope.Thread)
public class JsonHashingBenchmark {

    private QueryValidationServiceState document;

    private ServiceDocumentDescription desc;

    @Setup(Level.Trial)
    public void setup() {
        document = VerificationHost.buildQueryValidationState();
        desc = Builder.create()
                .buildDescription(QueryValidationServiceState.class);

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

    }

    @Benchmark
    public void hashJson() {
        Utils.computeSignature(document, desc);
    }
}
