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

package com.vmware.xenon.services.samples;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Service generates cluster wide unique IDs from the range it received from IdRangeService.
 */
public class IdGeneratorService extends StatelessService {
    public static final String SELF_LINK = ServiceUriPaths.SAMPLES
            + "/unique-id-int64";
    public static final int RANGE_INTERVAL_SIZE = IdRangeService.RANGE_INTERVAL;

    private static final Object object = new Object();

    private RangeCollection rangeCollection = new RangeCollection();

    public static class IdGeneratorResponse extends ServiceDocument{
        public long uniqueId;
    }

    private class RangeCollection {

        private int indexInRangeList = 0;

        // Current used Id. This is a value in one of the range in the ranges list.
        private long id = -1;

        // Sum of ranges list, minus the Ids returned to the callers.
        private AtomicLong totalIds = new AtomicLong(0);

        // List of ranges fetched from IdRangeService.
        private final List<IdRangeService.State> ranges = new CopyOnWriteArrayList<>();

        long getNextId() {
            Long total = this.totalIds.decrementAndGet();

            // If we do not have any Ids then asynchronously get a new range
            // and return -1 to the caller.
            if (total < 0) {
                getNewRange();
                return -1;
            }

            // We are half way through in our range internal, fetch a new
            // range asynchronously while we are consuming remaining Ids.
            if (total < RANGE_INTERVAL_SIZE / 2) {
                getNewRange();
            }

            // Protect id and indexInRangeList from concurrent requests.
            synchronized (object) {
                if (this.id == -1) {
                    if (total > 0) {
                        this.id = this.ranges.get(this.indexInRangeList).minId;
                    } else {
                        return -1;
                    }
                } else {
                    this.id++;
                }

                if (this.ranges.get(this.indexInRangeList).maxId < this.id) {
                    this.indexInRangeList++;
                    if (this.indexInRangeList >= this.ranges.size()) {
                        return -1;
                    }
                    this.id = this.ranges.get(this.indexInRangeList).minId;
                }
                return this.id;
            }
        }

        /**
         * Fetch a new range from IdRangeService.
         */
        private void getNewRange() {
            URI uri = UriUtils.buildUri(getHost(), IdRangeService.FACTORY_LINK);
            uri = UriUtils.extendUri(uri, UriUtils.convertPathCharsFromLink(getSelfLink()));
            UriUtils.appendQueryParam(uri, IdRangeService.RANGE_INTERVAL_QUERY_PARAM, String.valueOf(RANGE_INTERVAL_SIZE));
            Operation op = Operation.createGet(uri);
            DeferredResult<Operation> deferredResult = sendWithDeferredResult(op);
            deferredResult.whenComplete((o, e) -> {
                if (e != null) {
                    getHost().log(Level.WARNING, "GET failed - %s", e.getMessage());
                    return;
                }
                getHost().log(Level.WARNING, "GET passed - %s", getHost().getId());

                IdRangeService.State range = o.getBody(IdRangeService.State.class);
                this.ranges.add(range);
                synchronized (object) {
                    if (this.totalIds.get() < 0) {
                        this.totalIds.set(0);
                    }
                    this.totalIds.addAndGet(range.maxId - range.minId);
                }
            });
        }
    }

    public IdGeneratorService() {
    }

    @Override
    public void handleStart(Operation start) {
        IdRangeService.State initialState = new IdRangeService.State();
        initialState.maxId = 0;
        initialState.maxId = 0;
        initialState.documentSelfLink = UriUtils.convertPathCharsFromLink(this.getSelfLink());
        Operation post = Operation.createPost(this.getHost(), IdRangeService.FACTORY_LINK)
                .setBody(initialState)
                .setRetryCount(3)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.log(Level.WARNING, "POST failed %s - %s", initialState.documentSelfLink, e.getMessage());
                    }
                    this.log(Level.WARNING, "POST completed %s ", initialState.documentSelfLink);
                    this.rangeCollection.getNextId();
                });
        this.sendRequest(post);
        start.complete();
    }

    @Override
    public void handleGet(Operation get) {
        long id = this.rangeCollection.getNextId();
        IdGeneratorResponse response = new IdGeneratorResponse();
        response.uniqueId = id;
        response.documentOwner = this.getHost().getId();
        get.setBody(response);
        get.complete();
    }
}
