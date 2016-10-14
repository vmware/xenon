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

package com.vmware.xenon.services.samples;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.NodeGroupBroadcastResponse;
import com.vmware.xenon.services.common.NodeGroupBroadcastResult;
import com.vmware.xenon.services.common.NodeGroupBroadcastResult.PeerNodeResult;
import com.vmware.xenon.services.common.NodeGroupUtils;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Sample service to count the words of all the documents across all the nodes. <br/>
 * Demonstrates the usage of {@link DeferredResult} to coordinate complex workflows.
 * @see LocalWordCountingSampleService
 */
public class WordCountingSampleService extends StatelessService {
    public static final String SELF_LINK = ServiceUriPaths.SAMPLES + "/word-counts";

    public static class WordCountsResponse {
        public Map<String, Integer> wordCounts;

        public Integer failedDocsCount;

        static WordCountsResponse fromWordCounts(Map<String, Integer> wordCounts) {
            WordCountsResponse result = new WordCountsResponse();
            result.wordCounts = wordCounts;
            result.failedDocsCount = 0;
            return result;
        }

        static WordCountsResponse fromFailure() {
            WordCountsResponse result = new WordCountsResponse();
            result.wordCounts = Collections.emptyMap();
            result.failedDocsCount = 1;
            return result;
        }
    }

    @Override
    public void handleGet(Operation get) {
        this.countWordsInDocuments()
                .thenAccept(response -> get.setBody(response))
                .whenCompleteNotify(get);
    }

    /**
     * Business logic entry point
     * @return
     */
    private DeferredResult<WordCountsResponse> countWordsInDocuments() {
        return this.sendBroadcastQuery()
                .thenApply(WordCountingSampleService::aggregateResponses);
    }

    /**
     * Forward the query to {@link LocalWordCountingSampleService} at each node.
     * @return
     */
    private DeferredResult<List<WordCountsResponse>> sendBroadcastQuery() {
        URI localServiceURI = UriUtils.buildUri(this.getHost(), LocalWordCountingSampleService.SELF_LINK);
        URI forwardURI = UriUtils.buildBroadcastRequestUri(localServiceURI, ServiceUriPaths.DEFAULT_NODE_SELECTOR);
        Operation get = Operation.createGet(forwardURI);
        return this.getHost().sendWithDeferredResult(get, NodeGroupBroadcastResponse.class)
                .thenApply(this::extractResponses);
    }

    /**
     * Extract the responses of each local service.
     * @param broadcastResponse
     * @return
     */
    private List<WordCountsResponse> extractResponses(NodeGroupBroadcastResponse broadcastResponse) {
        NodeGroupBroadcastResult broadcastResult = NodeGroupUtils.toBroadcastResult(broadcastResponse);
        return broadcastResult.allResponses.stream()
                .peek(nodeResult -> {
                    if (nodeResult.isFailure()) {
                        this.logWarning("Unable to collect results from %s!", nodeResult.hostId);
                    }
                })
                .filter(PeerNodeResult::isSuccess)
                .map(nodeResult -> nodeResult.castBodyTo(WordCountsResponse.class))
                .collect(Collectors.toList());
    }

    /**
     * Helper method to aggregate response by summing the word counts and the failures.
     * @param maps
     * @return
     */
    static WordCountsResponse aggregateResponses(List<WordCountsResponse> responses) {
        WordCountsResponse result = WordCountsResponse.fromWordCounts(new HashMap<>());
        responses.forEach(response -> mergeWordCountsResponses(result, response));
        return result;
    }

    /**
     * Helper method to merge two responses into the first one by summing the word counts and
     * the failures.
     * @param result
     * @param other
     */
    private static void mergeWordCountsResponses(WordCountsResponse result, WordCountsResponse other) {
        other.wordCounts.forEach((word, count) -> {
            result.wordCounts.merge(word, count, Integer::sum);
        });
        result.failedDocsCount += other.failedDocsCount;
    }
}
