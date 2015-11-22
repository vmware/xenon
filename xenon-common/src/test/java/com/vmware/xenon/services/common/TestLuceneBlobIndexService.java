/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
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

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.vmware.xenon.common.BasicReportTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.LuceneBlobIndexService.BlobIndexOption;

public class TestLuceneBlobIndexService extends BasicReportTestCase {
    /**
     * Parameter that specifies number of concurrent update requests
     */
    public int updateCount = 100;

    @Test
    public void postAndGet() throws Throwable {
        Set<String> keys = doPost(null);
        this.host.testStart(keys.size());
        for (String key : keys) {
            Operation get = LuceneBlobIndexService.createGet(this.host, key);
            this.host.send(get.setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();
        this.host.logThroughput();
    }

    @Override
    public void beforeHostStart(VerificationHost host) {
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(100));
    }

    @Test
    public void postAndGetSingleVersion() throws Throwable {
        Set<String> keys = doPost(BlobIndexOption.SINGLE_USE_KEYS);
        this.host.testStart(keys.size());
        for (String key : keys) {
            Operation get = ServiceContextIndexService.createGet(this.host, key);
            this.host.send(get.setCompletion(this.host.getCompletion()));
        }

        this.host.testWait();
        this.host.logThroughput();
        // use the same keys, to post a new blob version. Since the index is single version the total
        // document count should remain keys.size(), not keys.size() * 2
        MinimalTestService s = new MinimalTestService();
        this.host.startServiceAndWait(s, UUID.randomUUID().toString(), null);

        this.host.testStart(keys.size());
        for (String key : keys) {
            Operation post = ServiceContextIndexService.createPost(this.host, key, s);
            this.host.send(post.setCompletion(this.host.getCompletion()));
        }

        this.host.testWait();
        this.host.logThroughput();

        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            ServiceStats st = this.host.getServiceState(null, ServiceStats.class,
                    UriUtils.buildStatsUri(UriUtils.buildUri(host,
                            ServiceContextIndexService.SELF_LINK)));

            ServiceStat totalDocCount = st.entries
                    .get(LuceneDocumentIndexService.STAT_NAME_INDEXED_DOCUMENT_COUNT);
            if (totalDocCount == null) {
                Thread.sleep(50);
                continue;
            }

            if (totalDocCount.latestValue != keys.size()) {
                Thread.sleep(50);
                continue;
            }
            return;
        }

        throw new TimeoutException();
    }

    private Set<String> doPost(BlobIndexOption option) throws Throwable {
        MinimalTestService s = new MinimalTestService();
        this.host.startServiceAndWait(s, UUID.randomUUID().toString(), null);

        this.host.testStart(this.updateCount);
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < this.updateCount; i++) {
            String key = Utils.getNowMicrosUtc() + "";
            keys.add(key);
            Operation post = null;
            if (option == BlobIndexOption.SINGLE_USE_KEYS) {
                post = ServiceContextIndexService.createPost(this.host, key, s);
            } else {
                post = LuceneBlobIndexService.createPost(this.host, key, s);
            }
            this.host.send(post.setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        this.host.logThroughput();
        return keys;
    }

}
