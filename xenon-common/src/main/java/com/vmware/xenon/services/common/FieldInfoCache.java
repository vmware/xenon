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

package com.vmware.xenon.services.common;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;

/**
 * Maintains a cache of FieldInfo and FieldInfos.
 */
final class FieldInfoCache {
    private static final int MAX_FIELD_INFO_COUNT = 1500;

    private static final Field fiValues;

    private static final Field fiByNumberMap;

    static {
        try {
            fiValues = FieldInfos.class.getDeclaredField("values");
            fiValues.setAccessible(true);

            fiByNumberMap = FieldInfos.class.getDeclaredField("byNumberMap");
            fiByNumberMap.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final class FieldInfoKey {
        private final FieldInfo fieldInfo;

        private FieldInfoKey(FieldInfo fieldInfo) {
            this.fieldInfo = fieldInfo;
        }

        @Override
        public int hashCode() {
            return FieldInfoCache.hashCode(this.fieldInfo);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof FieldInfoKey)) {
                return false;
            }

            FieldInfoKey that = (FieldInfoKey) obj;
            return FieldInfoCache.equals(this.fieldInfo, that.fieldInfo);
        }
    }

    public static int hashCode(FieldInfo fi) {
        int h = 17;
        h = h * 31 + fi.number;
        h = h * 31 + fi.name.hashCode();
        // skip attributes on purpose for performance
        return h;
    }

    public static boolean equals(FieldInfo a, FieldInfo b) {
        return a.number == b.number &&
                a.name.equals(b.name) &&
                a.getPointDimensionCount() == b.getPointDimensionCount() &&
                a.getPointNumBytes() == b.getPointNumBytes() &&
                a.getDocValuesGen() == b.getDocValuesGen() &&
                a.getIndexOptions() == b.getIndexOptions() &&
                a.hasPayloads() == b.hasPayloads() &&
                a.hasVectors() == b.hasVectors() &&
                a.omitsNorms() == b.omitsNorms() &&
                a.hasNorms() == b.hasNorms();
    }

    /**
     * Stores FieldInfo, all unique in terms of parameters
     */
    private final ConcurrentLinkedHashMap<FieldInfoKey, FieldInfo> infoCache;

    public FieldInfoCache() {
        this.infoCache = new ConcurrentLinkedHashMap.Builder<FieldInfoKey, FieldInfo>()
                .maximumWeightedCapacity(MAX_FIELD_INFO_COUNT)
                .build();
    }

    public FieldInfo dedupFieldInfo(FieldInfo fi) {
        FieldInfoKey key = new FieldInfoKey(fi);
        FieldInfo old = this.infoCache.putIfAbsent(key, fi);

        if (old != null) {
            return old;
        } else {
            // fi just got into the cache, validate as it was in the original code
            fi.checkConsistency();
        }

        return fi;
    }

    public FieldInfos dedupFieldInfos(FieldInfo[] infos) {
        FieldInfos res = new FieldInfos(infos);
        trimFieldInfos(res);
        return res;
    }

    /**
     * A bug in lucene keeps a treemap for every non-sparse FieldInfos unnecessary so.
     * This method does a copy of the field * {@link FieldInfos#values} allowing the underlying Map to be gc'ed.
     *
     * @see FieldInfos
     * @param fieldInfos
     */
    @SuppressWarnings("unchecked")
    private void trimFieldInfos(FieldInfos fieldInfos) {
        try {
            Object obj = fiByNumberMap.get(fieldInfos);
            if (obj != null) {
                return;
            }

            Collection<FieldInfo> values = (Collection<FieldInfo>) fiValues.get(fieldInfos);
            fiValues.set(fieldInfos, Collections.unmodifiableList(new ArrayList<>(values)));
        } catch (ReflectiveOperationException ignore) {

        }
    }
}
