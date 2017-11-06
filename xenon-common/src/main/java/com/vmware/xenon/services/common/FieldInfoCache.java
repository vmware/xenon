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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;

/**
 * Maintains a cache of FieldInfo and FieldInfos.
 */
final class FieldInfoCache {
    private static final int MAX_FIELD_INFO_COUNT = 1500;

    private static final Field fiValues;

    private static final Field fiByNumberTable;

    static {
        // remove this when upgraded to lucene 7.5/8.0
        try {
            fiValues = FieldInfos.class.getDeclaredField("values");
            fiValues.setAccessible(true);

            fiByNumberTable = FieldInfos.class.getDeclaredField("byNumberTable");
            fiByNumberTable.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final class FieldInfoKey {
        String name;
        int fieldNumber;
        boolean storeTermVector;
        boolean omitNorms;
        boolean storePayloads;
        IndexOptions indexOptions;
        DocValuesType docValuesType;
        long dvGen;
        Map<String, String> attributes;
        int pointDimensionCount;
        int pointNumBytes;

        private FieldInfoKey() {
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = h * 31 + this.fieldNumber;
            h = h * 31 + this.name.hashCode();
            // skip attributes on purpose for performance
            return h;
        }

        public FieldInfo toFieldInfo() {
            FieldInfo fi = new FieldInfo(this.name, this.fieldNumber, this.storeTermVector, this.omitNorms,
                    this.storePayloads,
                    this.indexOptions, this.docValuesType, this.dvGen, this.attributes,
                    this.pointDimensionCount, this.pointNumBytes);
            fi.checkConsistency();
            return fi;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof FieldInfoKey)) {
                return false;
            }

            FieldInfoKey that = (FieldInfoKey) obj;
            return this.fieldNumber == that.fieldNumber &&
                    this.name.equals(that.name) &&
                    this.attributes.equals(that.attributes) &&
                    this.pointDimensionCount == that.pointDimensionCount &&
                    this.pointNumBytes == that.pointNumBytes &&
                    this.dvGen == that.dvGen &&
                    this.indexOptions == that.indexOptions &&
                    this.storePayloads == that.storePayloads &&
                    this.storeTermVector == that.storeTermVector &&
                    this.omitNorms == that.omitNorms;
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
    private final ConcurrentMap<FieldInfoKey, FieldInfo> infoCache;

    /**
     * The longest FieldInfos ever seen. Harmless race reading/writing this.
     */
    private FieldInfos longest;

    public FieldInfoCache() {
        this.infoCache = new ConcurrentHashMap<>();
    }

    /**
     * At the end there will be a single segment with all the fields. So it makes sense to cache the longest
     * list of infos every encountered.
     *
     * @param infos
     * @return
     */
    public FieldInfos dedupFieldInfos(FieldInfo[] infos) {
        FieldInfos cached = this.longest;
        if (cached == null || cached.size() < infos.length) {
            cached = new FieldInfos(infos);
            trimFieldInfos(cached);
            this.longest = cached;
            return cached;
        }

        if (cached.size() == infos.length) {
            for (FieldInfo a : infos) {
                FieldInfo b = cached.fieldInfo(a.number);
                if (b == null || !FieldInfoCache.equals(a, b)) {
                    FieldInfos update = new FieldInfos(infos);
                    trimFieldInfos(update);
                    this.longest = update;
                    return update;
                }
            }

            return cached;
        }

        FieldInfos update = new FieldInfos(infos);
        trimFieldInfos(update);
        return update;
    }

    /**
     * A bug in lucene keeps a treemap for every non-sparse FieldInfos unnecessary so.
     * This method does a copy of the field * {@link FieldInfos#values} allowing the underlying Map to be gc'ed.
     *
     * @see FieldInfos
     * @param fieldInfos
     */
    @SuppressWarnings("unchecked")
    void trimFieldInfos(FieldInfos fieldInfos) {
        try {
            Object obj = fiByNumberTable.get(fieldInfos);
            if (obj == null) {
                // it is sparse, nothing to clean-up
                return;
            }

            // value is a TreeMap view
            Collection<FieldInfo> values = (Collection<FieldInfo>) fiValues.get(fieldInfos);

            // instead, copy the view
            values = Collections.unmodifiableList(new ArrayList<>(values));
            fiValues.set(fieldInfos, values);
        } catch (ReflectiveOperationException ignore) {

        }
    }

    public FieldInfo dedupFieldInfo(String name, int fieldNumber, boolean storeTermVector, boolean omitNorms,
            boolean storePayloads, IndexOptions indexOptions, DocValuesType docValuesType, long dvGen,
            Map<String, String> attributes, int pointDimensionCount, int pointNumBytes) {

        FieldInfoKey key = new FieldInfoKey();
        key.name = name;
        key.fieldNumber = fieldNumber;
        key.storeTermVector = storeTermVector;
        key.omitNorms = omitNorms;
        key.storePayloads = storePayloads;
        key.indexOptions = indexOptions;
        key.docValuesType = docValuesType;
        key.dvGen = dvGen;
        key.attributes = attributes;
        key.pointDimensionCount = pointDimensionCount;
        key.pointNumBytes = pointNumBytes;

        return this.infoCache.computeIfAbsent(key, FieldInfoKey::toFieldInfo);
    }

    public void handleMaintenance() {
        if (this.infoCache.size() > MAX_FIELD_INFO_COUNT) {
            this.infoCache.clear();
        }
    }
}
