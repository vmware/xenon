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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * This class takes care of lucene bugs related to FieldInfos. FieldInfo instances are cached and reused,
 * while FieldInfos instances are just cleared of their internal TreeMap overhead.
 */
class LuceneCodecWithFixes extends FilterCodec {
    private final FieldInfosFormat fieldInfosFormat;

    private final StoredFieldsFormat storedFieldsFormat;

    protected LuceneCodecWithFixes(Codec original, FieldInfoCache fieldInfoCache) {
        super(original.getName(), original);

        this.fieldInfosFormat = new Lucene60FieldInfosFormatWithCache(fieldInfoCache);

        // no change in behavior, just clear treemaps
        this.storedFieldsFormat = new StoredFieldsFormat() {
            final StoredFieldsFormat delegate = original.storedFieldsFormat();

            @Override
            public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn,
                    IOContext context) throws IOException {
                // this is the only change: trimming the TreeMap
                fieldInfoCache.trimFieldInfos(fn);
                return this.delegate.fieldsReader(directory, si, fn, context);
            }

            @Override
            public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context)
                    throws IOException {
                return this.delegate.fieldsWriter(directory, si, context);
            }
        };
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return this.fieldInfosFormat;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return this.storedFieldsFormat;
    }
}
