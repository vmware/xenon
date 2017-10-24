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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Copy of {@link org.apache.lucene.codecs.lucene60.Lucene60FieldInfosFormat}. The original is final and hooks
 * cannot be attached. The only hook needed is caching of FieldInfo instances. The changed code is delimited
 * with ///////////////////.
 *
 */
public final class Lucene60FieldInfosFormatWithCache extends FieldInfosFormat {

    ////////////////////////////////
    private static final int MAX_FIELD_INFO_COUNT = 1500;

    private static final int MAX_FIELD_INFOS_COUNT = 500;

    private static final class FieldInfoKey {
        private final FieldInfo fieldInfo;

        private FieldInfoKey(FieldInfo fieldInfo) {
            this.fieldInfo = fieldInfo;
        }

        @Override
        public int hashCode() {
            return hashCode(this.fieldInfo);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof FieldInfoKey)) {
                return false;
            }

            FieldInfoKey that = (FieldInfoKey) obj;
            return equals(this.fieldInfo, that.fieldInfo);
        }

        static int hashCode(FieldInfo fi) {
            int h = 17;
            h = h * 31 + fi.number;
            h = h * 31 + fi.name.hashCode();
            // skip attributes on purpose for performance
            // h = h * 31 + fi.getPointDimensionCount();
            // h = h * 31 + fi.getPointNumBytes();
            // h = h * 31 + (int) fi.getDocValuesGen();
            // h = h * 31 + fi.getIndexOptions().hashCode();
            // h = h * 31 + (fi.hasPayloads() ? 1 : -1);
            // h = h * 31 + (fi.hasVectors() ? 1 : -1);
            // h = h * 31 + (fi.omitsNorms() ? 1 : -1);
            // h = h * 31 + (fi.hasNorms() ? 1 : -1);
            // h = h * 31 + fi.attributes().hashCode();
            return h;
        }

        static boolean equals(FieldInfo me, FieldInfo them) {
            return me.number == them.number &&
                    me.name.equals(them.name) &&
                    me.getPointDimensionCount() == them.getPointDimensionCount() &&
                    me.getPointNumBytes() == them.getPointNumBytes() &&
                    me.getDocValuesGen() == them.getDocValuesGen() &&
                    me.getIndexOptions() == them.getIndexOptions() &&
                    me.hasPayloads() == them.hasPayloads() &&
                    me.hasVectors() == them.hasVectors() &&
                    me.omitsNorms() == them.omitsNorms() &&
                    me.hasNorms() == them.hasNorms() &&
                    me.attributes().equals(them.attributes());
        }
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
                h = h * 31 + FieldInfoKey.hashCode(info);
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
                if (!FieldInfoKey.equals(my, their)) {
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

    private FieldInfo dedupFieldInfo(FieldInfo fi) {
        FieldInfoKey key = new FieldInfoKey(fi);
        FieldInfo old = this.infoCache.putIfAbsent(key, fi);

        if (old != null) {
            return old;
        }

        return fi;
    }

    private FieldInfos dedupFieldInfos(FieldInfo[] infos) {
        FieldInfosKey key = new FieldInfosKey(infos);
        return this.infosCache.computeIfAbsent(key, k -> new FieldInfos(k.infos));
    }

    ////////////////////////////////////////////////////////////////

    /** Sole constructor. */
    public Lucene60FieldInfosFormatWithCache() {
        //////////////////////////////
        this.infoCache = new ConcurrentLinkedHashMap.Builder<FieldInfoKey, FieldInfo>()
                .maximumWeightedCapacity(MAX_FIELD_INFO_COUNT)
                .build();

        this.infosCache = new ConcurrentLinkedHashMap.Builder<FieldInfosKey, FieldInfos>()
                .maximumWeightedCapacity(MAX_FIELD_INFOS_COUNT)
                .build();
        //////////////////////////////
    }

    @Override
    public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext context) throws
            IOException {
        final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, EXTENSION);
        try (ChecksumIndexInput input = directory.openChecksumInput(fileName, context)) {
            Throwable priorE = null;
            FieldInfo[] infos = null;
            try {
                CodecUtil.checkIndexHeader(input,
                        Lucene60FieldInfosFormatWithCache.CODEC_NAME,
                        Lucene60FieldInfosFormatWithCache.FORMAT_START,
                        Lucene60FieldInfosFormatWithCache.FORMAT_CURRENT,
                        segmentInfo.getId(), segmentSuffix);

                final int size = input.readVInt(); //read in the size
                infos = new FieldInfo[size];

                // previous field's attribute map, we share when possible:
                Map<String, String> lastAttributes = Collections.emptyMap();

                for (int i = 0; i < size; i++) {
                    String name = input.readString();
                    final int fieldNumber = input.readVInt();
                    if (fieldNumber < 0) {
                        throw new CorruptIndexException(
                                "invalid field number for field: " + name + ", fieldNumber=" + fieldNumber, input);
                    }
                    byte bits = input.readByte();
                    boolean storeTermVector = (bits & STORE_TERMVECTOR) != 0;
                    boolean omitNorms = (bits & OMIT_NORMS) != 0;
                    boolean storePayloads = (bits & STORE_PAYLOADS) != 0;

                    final IndexOptions indexOptions = getIndexOptions(input, input.readByte());

                    // DV Types are packed in one byte
                    final DocValuesType docValuesType = getDocValuesType(input, input.readByte());
                    final long dvGen = input.readLong();
                    Map<String, String> attributes = input.readMapOfStrings();
                    // just use the last field's map if its the same
                    if (attributes.equals(lastAttributes)) {
                        attributes = lastAttributes;
                    }
                    lastAttributes = attributes;
                    int pointDimensionCount = input.readVInt();
                    int pointNumBytes;
                    if (pointDimensionCount != 0) {
                        pointNumBytes = input.readVInt();
                    } else {
                        pointNumBytes = 0;
                    }

                    try {
                        FieldInfo fi = new FieldInfo(name, fieldNumber, storeTermVector, omitNorms, storePayloads,
                                indexOptions, docValuesType, dvGen, attributes,
                                pointDimensionCount, pointNumBytes);
                        fi.checkConsistency();

                        //////////////////////
                        infos[i] = dedupFieldInfo(fi);
                        //////////////////////
                    } catch (IllegalStateException e) {
                        throw new CorruptIndexException(
                                "invalid fieldinfo for field: " + name + ", fieldNumber=" + fieldNumber, input, e);
                    }
                }
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(input, priorE);
            }
            //////////////////////
            return dedupFieldInfos(infos);
            //////////////////////
        }
    }

    static {
        // We "mirror" DocValues enum values with the constants below; let's try to ensure if we add a new DocValuesType while this format is
        // still used for writing, we remember to fix this encoding:
        assert DocValuesType.values().length == 6;
    }

    private static byte docValuesByte(DocValuesType type) {
        switch (type) {
        case NONE:
            return 0;
        case NUMERIC:
            return 1;
        case BINARY:
            return 2;
        case SORTED:
            return 3;
        case SORTED_SET:
            return 4;
        case SORTED_NUMERIC:
            return 5;
        default:
            // BUG
            throw new AssertionError("unhandled DocValuesType: " + type);
        }
    }

    private static DocValuesType getDocValuesType(IndexInput input, byte b) throws IOException {
        switch (b) {
        case 0:
            return DocValuesType.NONE;
        case 1:
            return DocValuesType.NUMERIC;
        case 2:
            return DocValuesType.BINARY;
        case 3:
            return DocValuesType.SORTED;
        case 4:
            return DocValuesType.SORTED_SET;
        case 5:
            return DocValuesType.SORTED_NUMERIC;
        default:
            throw new CorruptIndexException("invalid docvalues byte: " + b, input);
        }
    }

    static {
        // We "mirror" IndexOptions enum values with the constants below; let's try to ensure if we add a new IndexOption while this format is
        // still used for writing, we remember to fix this encoding:
        assert IndexOptions.values().length == 5;
    }

    private static byte indexOptionsByte(IndexOptions indexOptions) {
        switch (indexOptions) {
        case NONE:
            return 0;
        case DOCS:
            return 1;
        case DOCS_AND_FREQS:
            return 2;
        case DOCS_AND_FREQS_AND_POSITIONS:
            return 3;
        case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
            return 4;
        default:
            // BUG:
            throw new AssertionError("unhandled IndexOptions: " + indexOptions);
        }
    }

    private static IndexOptions getIndexOptions(IndexInput input, byte b) throws IOException {
        switch (b) {
        case 0:
            return IndexOptions.NONE;
        case 1:
            return IndexOptions.DOCS;
        case 2:
            return IndexOptions.DOCS_AND_FREQS;
        case 3:
            return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        case 4:
            return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        default:
            // BUG
            throw new CorruptIndexException("invalid IndexOptions byte: " + b, input);
        }
    }

    @Override
    public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos,
            IOContext context) throws IOException {
        final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, EXTENSION);
        try (IndexOutput output = directory.createOutput(fileName, context)) {
            CodecUtil.writeIndexHeader(output, Lucene60FieldInfosFormatWithCache.CODEC_NAME,
                    Lucene60FieldInfosFormatWithCache.FORMAT_CURRENT, segmentInfo.getId(), segmentSuffix);
            output.writeVInt(infos.size());
            for (FieldInfo fi : infos) {
                fi.checkConsistency();

                output.writeString(fi.name);
                output.writeVInt(fi.number);

                byte bits = 0x0;
                if (fi.hasVectors()) {
                    bits |= STORE_TERMVECTOR;
                }
                if (fi.omitsNorms()) {
                    bits |= OMIT_NORMS;
                }
                if (fi.hasPayloads()) {
                    bits |= STORE_PAYLOADS;
                }
                output.writeByte(bits);

                output.writeByte(indexOptionsByte(fi.getIndexOptions()));

                // pack the DV type and hasNorms in one byte
                output.writeByte(docValuesByte(fi.getDocValuesType()));
                output.writeLong(fi.getDocValuesGen());
                output.writeMapOfStrings(fi.attributes());
                int pointDimensionCount = fi.getPointDimensionCount();
                output.writeVInt(pointDimensionCount);
                if (pointDimensionCount != 0) {
                    output.writeVInt(fi.getPointNumBytes());
                }
            }
            CodecUtil.writeFooter(output);
        }
    }

    /** Extension of field infos */
    static final String EXTENSION = "fnm";

    // Codec header
    static final String CODEC_NAME = "Lucene60FieldInfos";
    static final int FORMAT_START = 0;
    static final int FORMAT_CURRENT = FORMAT_START;

    // Field flags
    static final byte STORE_TERMVECTOR = 0x1;
    static final byte OMIT_NORMS = 0x2;
    static final byte STORE_PAYLOADS = 0x4;
}
