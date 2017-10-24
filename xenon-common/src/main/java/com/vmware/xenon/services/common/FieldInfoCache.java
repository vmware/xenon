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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;

/**
 * Maintains a cache of FieldInfo and FieldInfos.
 */
final class FieldInfoCache {
    private static final int MAX_FIELD_INFO_COUNT = 1500;

    private static final int MAX_FIELD_INFOS_COUNT = 500;

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
                a.hasNorms() == b.hasNorms() &&
                a.attributes().equals(b.attributes());
    }

    private static final class FieldInfosKey {
        private final FieldInfo[] infos;

        private FieldInfosKey(FieldInfo[] infos) {
            this.infos = infos;
        }

        @Override
        public int hashCode() {
            int h = 17;
            for (FieldInfo info : this.infos) {
                h = h * 31 + FieldInfoCache.hashCode(info);
            }

            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof FieldInfosKey)) {
                return false;
            }

            FieldInfosKey that = (FieldInfosKey) obj;
            if (this.infos.length != that.infos.length) {
                return false;
            }
            for (int i = 0; i < this.infos.length; i++) {
                FieldInfo my = this.infos[i];
                FieldInfo their = that.infos[i];
                if (!FieldInfoCache.equals(my, their)) {
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * Stores FieldInfo, all unique in terms of parameters
     */
    private final ConcurrentLinkedHashMap<FieldInfoKey, FieldInfo> infoCache;

    /**
     * Stores FieldInfos keyed by the infos they contains. Values may or may not be
     * entries from {@link #infosCache}.
     */
    private final ConcurrentLinkedHashMap<FieldInfosKey, FieldInfos> infosCache;

    public FieldInfoCache() {
        this.infoCache = new ConcurrentLinkedHashMap.Builder<FieldInfoKey, FieldInfo>()
                .maximumWeightedCapacity(MAX_FIELD_INFO_COUNT)
                .build();

        this.infosCache = new ConcurrentLinkedHashMap.Builder<FieldInfosKey, FieldInfos>()
                .maximumWeightedCapacity(MAX_FIELD_INFOS_COUNT)
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
        FieldInfosKey key = new FieldInfosKey(infos);
        return this.infosCache.computeIfAbsent(key, k -> new FieldInfos(k.infos));
    }
}
