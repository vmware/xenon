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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

@State(Scope.Thread)
public class JsonHashingBenchmark {

    private ServiceDocumentDescription desc;

    private ServiceDocument smallDocument;
    private ServiceDocument bigDocument;

    @Setup(Level.Trial)
    public void setup() {
        desc = Builder.create().buildDescription(ExampleServiceState.class);
        this.bigDocument = BenchmarkDataFactory.makeDocOfJsonSizeKb(25);
        this.smallDocument = BenchmarkDataFactory.makeDocOfJsonSizeKb(1);
    }

    @Benchmark
    public String hashSmallDoc() {
        return Utils.computeSignature(smallDocument, desc);
    }

    @Benchmark
    public String hashBigDoc() {
        return Utils.computeSignature(bigDocument, desc);
    }
}
