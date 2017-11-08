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

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.serialization.KryoSerializers;

@State(Scope.Thread)
public class KryoBenchmark {
    private ServiceDocument smallDocument;
    private byte[] smallBinary;

    private ServiceDocument bigDocument;
    private byte[] bigBinary;

    @Setup(Level.Trial)
    public void setup() {
        this.smallDocument = BenchmarkDataFactory.makeDocOfJsonSizeKb(1);
        this.smallBinary = KryoSerializers.serializeAsDocument(smallDocument, 32 * 1024).toBytes();

        this.bigDocument = BenchmarkDataFactory.makeDocOfJsonSizeKb(25);
        this.bigBinary = KryoSerializers.serializeAsDocument(bigDocument, 32 * 1024).toBytes();
    }

    @Benchmark
    public Object serializeSmallObject() {
        return KryoSerializers.serializeAsDocument(smallDocument, 32 * 1024);
    }


    @Benchmark
    public Object deserializeSmallObject() {
        return KryoSerializers.deserializeDocument(smallBinary, 0, smallBinary.length);
    }

    @Benchmark
    public Object serializeBigObject() {
        return KryoSerializers.serializeAsDocument(bigDocument, 32 * 1024);
    }

    @Benchmark
    public Object deserializeBigObject() {
        return KryoSerializers.deserializeDocument(bigBinary, 0, bigBinary.length);
    }
}
