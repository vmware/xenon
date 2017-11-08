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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

@State(Scope.Thread)
public class GsonBenchmark {

    private ExampleServiceState bigDocument;
    private String bigRepr;

    private ExampleServiceState smallDoc;
    private String smallRepr;

    @Setup(Level.Trial)
    public void setup() {
        this.bigDocument = BenchmarkDataFactory.makeDocOfJsonSizeKb(25);
        this.bigRepr = Utils.toJson(this.bigDocument);

        this.smallDoc = BenchmarkDataFactory.makeDocOfJsonSizeKb(1);
        this.smallRepr = Utils.toJson(this.smallDoc);
    }

    @Benchmark
    public String serializeSmallDoc() {
        return Utils.toJson(smallDoc);
    }

    @Benchmark
    public Object deserializeSmallDoc() {
        return Utils.fromJson(smallRepr, smallDoc.getClass());
    }

    @Benchmark
    public String serializeBigDoc() {
        return Utils.toJson(bigDocument);
    }

    @Benchmark
    public Object deserializeBigDoc() {
        return Utils.fromJson(bigRepr, bigDocument.getClass());
    }
}
