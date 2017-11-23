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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

import com.vmware.xenon.common.Utils;

/**
 * Keep a searcher with the associated metadata together.
 */
public final class IndexSearcherWithMeta extends IndexSearcher {
    public long updateTimeMicros;
    public long createTimeMicros;

    public IndexSearcherWithMeta(IndexReader r) {
        super(r);
        this.createTimeMicros = Utils.getNowMicrosUtc();
        this.updateTimeMicros = this.createTimeMicros;
    }
}
