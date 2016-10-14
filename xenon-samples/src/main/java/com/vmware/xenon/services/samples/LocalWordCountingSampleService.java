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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.samples.WordCountingSampleService.WordCountsResponse;

/**
 * Sample service to count the words in all the local documents. <br/>
 * Demonstrates the usage of {@link DeferredResult} to coordinate a complex workflow. <br/>
 * The workflow is as follows:
 * <ol>
 * <li> fetch the links for all the local documents </li>
 * <li> for every document (map step):
 *  <ul>
 *      <li> retrieve the contents </li>
 *      <li> count the words </li>
 *  </ul>
 * </li>
 * <li> aggregate the individual results (reduce step) </li>
 * </ol>
 */
public class LocalWordCountingSampleService extends StatelessService {
    public static final String SELF_LINK = ServiceUriPaths.SAMPLES + "/local-word-counts";

    private String docFactoryPath;

    private Function<Operation, String> contentsMapper;

    /**
     * Creates a new instance of the service<br/>
     * We want to keep the implementation agnostic to the data provider, we only
     * assume the data provider is a hosted factory service.
     * @param docFactoryPath The path to the data provider service supplying the documents.
     * @param contentsMapper Function used for extracting the contents from a response to
     * fetch document.
     */
    public LocalWordCountingSampleService(String docFactoryPath,
            Function<Operation, String> contentsMapper) {
        this.docFactoryPath = docFactoryPath;
        this.contentsMapper = contentsMapper;
    }

    @Override
    public void handleGet(Operation get) {
        // As the whenCompleteNotify is guaranteed to be invoked we don't have to explicitly
        // handle intermediate errors to break the workflow and fail the operation.
        countWordsInLocalDocuments()
                .thenAccept(response -> get.setBody(response))
                .whenCompleteNotify(get);
    }

    /**
     * Business logic entry point
     * @return
     */
    private DeferredResult<WordCountsResponse> countWordsInLocalDocuments() {
        return fetchLocalDocumentLinks()
                // map
                .thenCompose(this::processDocuments)
                // reduce
                .thenApply(WordCountingSampleService::aggregateResponses);
    }

    /**
     * Returns the links to all of the documents hosted in this node.
     * @return
     */
    private DeferredResult<List<String>> fetchLocalDocumentLinks() {
        Operation query = Operation.createGet(this, this.docFactoryPath);
        DeferredResult<List<String>> result = this
                .sendWithDeferredResult(query, ServiceDocumentQueryResult.class)
                .thenApply(results -> results.documentLinks)
                .whenComplete((documentLinks, error) -> {
                    // checkpoint logging
                    if (error != null) {
                        // In case of an error we don't recover, the error will be propagated,
                        // skipping normal execution stages, until it reaches a stage that can
                        // handle errors (handle, exceptionally, whenComplete, etc)
                        this.logSevere("Unexpected error", error);
                    } else {
                        this.logInfo("Retrieved %d document links",
                                documentLinks != null ? documentLinks.size() : 0);
                    }
                });
        return result;
    }

    /**
     * Counts the words in the provided documents.
     * @param documentLinks
     * @return
     */
    private DeferredResult<List<WordCountsResponse>> processDocuments(List<String> documentLinks) {
        if (documentLinks == null || documentLinks.isEmpty()) {
            return DeferredResult.completed(Collections.emptyList());
        }
        // Fan-out and process the individual documents in parallel
        List<DeferredResult<WordCountsResponse>> deferredResults = documentLinks.stream()
                .map(this::processDocument)
                .collect(Collectors.toList());
        return DeferredResult.allOf(deferredResults);
    }

    /**
     * Counts the words in a single document.
     * @param documentLink
     * @return
     */
    private DeferredResult<WordCountsResponse> processDocument(String documentLink) {
        return this.fetchDocument(documentLink)
                .thenApply(this::countWords)
                .thenApply(WordCountsResponse::fromWordCounts)
                .exceptionally(error -> {
                    // in case of an error recover by returning empty map
                    this.logWarning("Failure while processing %s, excluding from result!",
                            documentLink);
                    return WordCountsResponse.fromFailure();
                });
    }

    /**
     * Retrieves the document's contents.
     * @param documentLink
     * @return
     */
    private DeferredResult<String> fetchDocument(String documentLink) {
        Operation getDocument = Operation.createGet(this, documentLink);
        return this.sendWithDeferredResult(getDocument)
                .thenApply(this.contentsMapper);
    }

    /**
     * Counts the words in the provided document's contents.
     * @param contents
     * @return
     */
    private Map<String, Integer> countWords(String contents) {
        Map<String, Integer> result = new HashMap<>();
        if (contents == null) {
            return result;
        }

        String[] words = contents.split("[\\s.,!?:;(){}\\[\\]]+");
        for (String word : words) {
            result.merge(word, 1, Integer::sum);
        }

        return result;
    }
}
