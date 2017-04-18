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

package com.vmware.xenon.common.serialization;

import java.io.IOException;
import java.io.Writer;

import com.google.gson.stream.JsonWriter;

import com.vmware.xenon.common.FNVHash;

/**
 * Instead of writing, this class computes a running hash.
 */
public final class HashingJsonWriter extends JsonWriter {

    private static final int BEG_ARRAY = 1;
    private static final int END_ARRAY = -1;

    private static final int BEG_OBJ = 2;
    private static final int END_OBJ = -2;

    private static final int NAME = 3;
    private static final int STRING = 4;
    private static final int NULL = 5;
    private static final int NUMBER = 6;
    private static final int BOOL = 7;

    private long hash;

    private static final Writer UNWRITABLE_WRITER = new Writer() {
        @Override
        public void write(char[] buffer, int offset, int counter) {
            throw new AssertionError();
        }

        @Override
        public void flush() throws IOException {
            throw new AssertionError();
        }

        @Override
        public void close() throws IOException {
            throw new AssertionError();
        }
    };

    public HashingJsonWriter(long hash) {
        super(UNWRITABLE_WRITER);
        setLenient(true);
        setSerializeNulls(false);
        this.hash = hash;
    }

    @Override
    public JsonWriter beginArray() throws IOException {
        this.hash = FNVHash.compute(BEG_ARRAY, this.hash);
        return this;
    }

    @Override
    public JsonWriter endArray() throws IOException {
        this.hash = FNVHash.compute(END_ARRAY, this.hash);
        return this;
    }

    @Override
    public JsonWriter beginObject() throws IOException {
        this.hash = FNVHash.compute(BEG_OBJ, this.hash);
        return this;
    }

    @Override
    public JsonWriter endObject() throws IOException {
        this.hash = FNVHash.compute(END_OBJ, this.hash);
        return this;
    }

    @Override
    public JsonWriter name(String name) throws IOException {
        long h = this.hash;
        h = FNVHash.compute(NAME, h);
        h = FNVHash.compute(name, h);
        this.hash = h;
        return this;
    }

    @Override
    public JsonWriter value(String value) throws IOException {
        long h = this.hash;
        h = FNVHash.compute(STRING, h);
        if (value != null) {
            h = FNVHash.compute(value, h);
        } else {
            h = FNVHash.compute(NULL, h);
        }
        this.hash = h;
        return this;
    }

    @Override
    public JsonWriter nullValue() throws IOException {
        this.hash = FNVHash.compute(NULL, this.hash);
        return this;
    }

    @Override
    public JsonWriter value(boolean value) throws IOException {
        long h = this.hash;
        h = FNVHash.compute(BOOL, h);
        h = FNVHash.compute(Boolean.hashCode(value), h);
        this.hash = h;
        return this;
    }

    @Override
    public JsonWriter value(double value) throws IOException {
        long h = this.hash;
        h = FNVHash.compute(NUMBER, h);
        long bits = Double.doubleToLongBits(value);
        h = FNVHash.compute((int) bits, h);
        h = FNVHash.compute((int) (bits >> 32), h);
        this.hash = h;
        return this;
    }

    @Override
    public JsonWriter value(Boolean value) throws IOException {
        long h = this.hash;
        h = FNVHash.compute(BOOL, h);
        if (value != null) {
            h = FNVHash.compute(Boolean.hashCode(value), h);
        } else {
            h = FNVHash.compute(NULL, h);
        }
        this.hash = h;
        return this;
    }

    @Override
    public JsonWriter value(long value) throws IOException {
        long h = this.hash;
        h = FNVHash.compute(NUMBER, h);
        h = FNVHash.compute((int) value, h);
        h = FNVHash.compute((int) (value >> 32), h);
        this.hash = h;
        return this;
    }

    @Override
    public JsonWriter value(Number value) throws IOException {
        long h = this.hash;
        h = FNVHash.compute(NUMBER, h);
        if (value != null) {
            h = FNVHash.compute(value.toString(), h);
        } else {
            h = FNVHash.compute(NULL, h);
        }
        this.hash = h;
        return this;
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    public long getHash() {
        return this.hash;
    }
}
