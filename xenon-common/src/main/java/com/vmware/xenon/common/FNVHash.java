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

/**
 * Implementation of FNV hashing function. We avoid use of BigInteger class
 * for performance reasons, so we are truncating the FNV_OFFSET value by 8 bits.
 * The code below also adds an additional shift and OR of each byte so its present
 * twice before we XOR with the existing hash.
 *
 * See https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
 */
public class FNVHash {
    public static final long FNV_PRIME = 1099511628211L;
    public static final long FNV_OFFSET_MINUS_MSB = 4695981039346656037L;

    private FNVHash() {
    }


    public static long compute(CharSequence data) {
        return compute(data, FNV_OFFSET_MINUS_MSB);
    }

    public static long compute(CharSequence data, long hash) {
        int length = data.length();
        for (int pos = 0; pos < length; pos++) {
            int code = data.charAt(pos);
            hash ^= code + code << 8;
            hash *= FNV_PRIME;
        }
        return hash;
    }

    public static int compute32(CharSequence data) {
        return compute32(data, FNV_OFFSET_MINUS_MSB);
    }

    public static int compute32(CharSequence data, long hash) {
        hash = compute(data, hash);
        // fold 64 bit
        return (int) ((hash >> 31) | (0x00000000FFFFFFFFL & hash));
    }

    public static long compute(byte[] data, int offset, int length) {
        return compute(data, offset, length, FNV_OFFSET_MINUS_MSB);
    }

    public static long compute(byte[] data, int offset, int length, long hash) {
        for (int i = offset; i < length; i++) {
            int code = data[i];
            hash ^= code;
            hash *= FNV_PRIME;
        }
        return hash;
    }

}