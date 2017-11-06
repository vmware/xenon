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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;

/**
 * This class takes care of lucene bugs related to FieldInfos. FieldInfo instances are cached and reused,
 * while FieldInfos instances are just cleared of their internal TreeMap overhead.
 */
class LuceneCodecWithFixes extends FilterCodec {
    private final FieldInfosFormat fieldInfosFormat;

    protected LuceneCodecWithFixes(Codec original, FieldInfoCache fieldInfoCache) {
        super(original.getName(), original);

        this.fieldInfosFormat = new Lucene60FieldInfosFormatWithCache(fieldInfoCache);
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return this.fieldInfosFormat;
    }
}
