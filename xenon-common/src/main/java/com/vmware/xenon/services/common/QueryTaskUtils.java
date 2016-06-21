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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.stream.Collectors;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

/**
 * Query task utility functions
 */
public class QueryTaskUtils {

    /**
     * The maximum depth level of which to expand the {@link TypeName#PODO} and {@link TypeName#COLLECTION}
     * properties when building {@link #getExpandedQueryPropertyNames(ServiceDocumentDescription)}
     */
    private static final int MAX_NEST_LEVEL_EXPAND_PROPERTY = 2;

    private static ServiceDocumentQueryResult mergeCountQueries(
            List<ServiceDocumentQueryResult> dataSources, ServiceDocumentQueryResult result) {
        long highestCount = 0;
        for (int i = 0; i < dataSources.size(); i++) {
            ServiceDocumentQueryResult dataSource = dataSources.get(i);
            if ((dataSource.documentLinks == null || dataSource.documentLinks.isEmpty())
                    && (dataSource.documents == null || dataSource.documents.isEmpty())
                    && dataSource.documentCount != null && dataSource.documentCount > 0) {
                if (highestCount < dataSource.documentCount) {
                    highestCount = dataSource.documentCount;
                }
            }
        }

        result.documentCount = highestCount;
        result.documentLinks = Collections.emptyList();
        return result;
    }

    /**
    * Merges a list of @ServiceDocumentQueryResult that were already <b>sorted</b> on <i>documentLink</i>.
    * The merge will be done in linear time.
    *
    * @param dataSources A list of @ServiceDocumentQueryResult <b>sorted</b> on <i>documentLink</i>.
    * @param isAscOrder  Whether the document links are sorted in ascending order.
    * @return The merging result.
    */
    public static ServiceDocumentQueryResult mergeQueryResults(
            List<ServiceDocumentQueryResult> dataSources, boolean isAscOrder) {
        return mergeQueryResults(dataSources, isAscOrder, EnumSet.noneOf(QueryOption.class));
    }

    /**
    * Merges a list of @ServiceDocumentQueryResult that were already <b>sorted</b> on <i>documentLink</i>.
    * The merge will be done in linear time. It will consider QueryOption.Count where
    * the highest count will be selected.
    *
    * @param dataSources A list of @ServiceDocumentQueryResult <b>sorted</b> on <i>documentLink</i>.
    * @param isAscOrder  Whether the document links are sorted in ascending order.
    * @return The merging result.
    */
    public static ServiceDocumentQueryResult mergeQueryResults(
            List<ServiceDocumentQueryResult> dataSources,
            boolean isAscOrder, EnumSet<QueryOption> queryOptions) {

        // To hold the merge result.
        ServiceDocumentQueryResult result = new ServiceDocumentQueryResult();
        result.documents = new HashMap<>();
        result.documentCount = 0L;

        // handle count queries
        if (queryOptions != null && queryOptions.contains(QueryOption.COUNT)) {
            return mergeCountQueries(dataSources, result);
        }

        // For each list of documents to be merged, a pointer is maintained to indicate which element
        // is to be merged. The initial values are 0s.
        int[] indices = new int[dataSources.size()];

        // Keep going until the last element in each list has been merged.
        while (true) {
            // Always pick the document link that is the smallest or largest depending on "isAscOrder" from
            // all lists to be merged. "documentLinkPicked" is used to keep the winner.
            String documentLinkPicked = null;

            // Ties could happen among the lists. That is, multiple elements could be picked in one iteration,
            // and the lists where they locate need to be recorded so that their pointers could be adjusted accordingly.
            List<Integer> sourcesPicked = new ArrayList<>();

            // In each iteration, the current elements in all lists need to be compared to pick the winners.
            for (int i = 0; i < dataSources.size(); i++) {
                // If the current list still have elements left to be merged, then proceed.
                if (indices[i] < dataSources.get(i).documentCount
                        && !dataSources.get(i).documentLinks.isEmpty()) {
                    String documentLink = dataSources.get(i).documentLinks.get(indices[i]);
                    if (documentLinkPicked == null) {
                        // No document link has been picked in this iteration, so it is the winner at the current time.
                        documentLinkPicked = documentLink;
                        sourcesPicked.add(i);
                    } else {
                        if (isAscOrder && documentLink.compareTo(documentLinkPicked) < 0
                                || !isAscOrder && documentLink.compareTo(documentLinkPicked) > 0) {
                            // If this document link is smaller or bigger (depending on isAscOrder),
                            // then replace the original winner.
                            documentLinkPicked = documentLink;
                            sourcesPicked.clear();
                            sourcesPicked.add(i);
                        } else if (documentLink.equals(documentLinkPicked)) {
                            // If it is a tie, we will need to record this element too so that
                            // it won't be processed in the next iteration.
                            sourcesPicked.add(i);
                        }
                    }
                }
            }

            if (documentLinkPicked != null) {
                // Save the winner to the result.
                result.documentLinks.add(documentLinkPicked);
                ServiceDocumentQueryResult partialResult = dataSources.get(sourcesPicked.get(0));
                if (partialResult.documents != null) {
                    result.documents.put(documentLinkPicked,
                            partialResult.documents.get(documentLinkPicked));
                }
                result.documentCount++;

                // Move the pointer of the lists where the winners locate.
                for (int i : sourcesPicked) {
                    indices[i]++;
                }
            } else {
                // No document was picked, that means all lists had been processed,
                // and the merging work is done.
                break;
            }
        }

        return result;
    }

    public static void expandLinks(ServiceHost host, QueryTask task, Operation op) {
        ServiceDocumentQueryResult result = task.results;
        if (!task.querySpec.options.contains(QueryOption.EXPAND_LINKS) || result == null
                || result.selectedLinksPerDocument == null || result.selectedLinksPerDocument.isEmpty()) {
            op.setBodyNoCloning(task).complete();
            return;
        }

        Map<String, String> uniqueLinkToState = new ConcurrentSkipListMap<>();
        for (Map<String, String> selectedLinksPerDocument : result.selectedLinksPerDocument.values()) {
            for (Entry<String, String> en : selectedLinksPerDocument.entrySet()) {
                uniqueLinkToState.put(en.getValue(), "");
            }
        }

        if (uniqueLinkToState.isEmpty()) {
            // this should not happen, but, defense in depth
            op.setBodyNoCloning(task).complete();
            return;
        }

        AtomicInteger remaining = new AtomicInteger(uniqueLinkToState.size());

        CompletionHandler c = (o, e) -> {
            String link = o.getUri().getPath();

            if (e != null) {
                host.log(Level.WARNING, "Failure retrieving link %s: %s", link,
                        e.toString());
                // serialize the error response and return it in the selectedLinks map
            }

            Object body = o.getBodyRaw();

            try {
                String json = Utils.toJson(body);
                uniqueLinkToState.put(link, json);
            } catch (Throwable ex) {
                host.log(Level.WARNING, "Failure serializing response for %s: %s", link,
                        ex.getMessage());
            }

            int r = remaining.decrementAndGet();
            if (r != 0) {
                return;
            }

            result.selectedDocuments = uniqueLinkToState;
            op.setBodyNoCloning(task).complete();
        };

        for (String link : uniqueLinkToState.keySet()) {
            Operation get = Operation.createGet(UriUtils.buildUri(op.getUri(), link))
                    .setCompletion(c)
                    .transferRefererFrom(op);
            host.sendRequest(get);
        }

    }

    /**
     * Return all searchable properties of the given description. Complex properties
     * {@link TypeName#PODO} and {@link TypeName#COLLECTION} are not returned, but their inner
     * primitive type leaf properties up to a configurable level are. They are returned in
     * {@link QuerySpecification} format.
     *
     * @see {@link QuerySpecification#buildCompositeFieldName(String...)}
     */
    public static Set<String> getExpandedQueryPropertyNames(ServiceDocumentDescription description) {
        if (description == null) {
            throw new IllegalArgumentException("description is required");
        }

        return getExpandedQueryPropertyNames(description.propertyDescriptions,
                MAX_NEST_LEVEL_EXPAND_PROPERTY);
    }

    private static Set<String> getExpandedQueryPropertyNames(
            Map<String, PropertyDescription> propertyDescriptions, int complexFieldNestLevel) {
        Set<String> result = new HashSet<>();

        for (Entry<String, PropertyDescription> entry : propertyDescriptions.entrySet()) {
            result.addAll(getExpandedQueryPropertyNames(entry.getKey(), entry.getValue(),
                    complexFieldNestLevel));
        }

        return result;
    }

    private static Set<String> getExpandedQueryPropertyNames(String propertyName,
            PropertyDescription pd, int complexFieldNestLevel) {
        if ((pd.indexingOptions != null && pd.indexingOptions
                .contains(PropertyIndexingOption.STORE_ONLY)) ||
                pd.usageOptions.contains(PropertyUsageOption.INFRASTRUCTURE)) {
            return Collections.emptySet();
        }

        if (pd.typeName == TypeName.PODO && pd.fieldDescriptions != null) {
            if (complexFieldNestLevel > 0) {
                Set<String> innerPropertyNames = getExpandedQueryPropertyNames(
                        pd.fieldDescriptions, complexFieldNestLevel - 1);

                return innerPropertyNames.stream()
                        .map((p) -> QuerySpecification.buildCompositeFieldName(propertyName, p))
                        .collect(Collectors.toSet());
            } else {
                return Collections.emptySet();
            }
        } else if (pd.typeName == TypeName.COLLECTION) {
            if (complexFieldNestLevel > 0) {
                Set<String> innerPropertyNames = getExpandedQueryPropertyNames(
                        QuerySpecification.COLLECTION_FIELD_SUFFIX, pd.elementDescription,
                        complexFieldNestLevel - 1);

                return innerPropertyNames.stream()
                        .map((p) -> QuerySpecification.buildCompositeFieldName(propertyName, p))
                        .collect(Collectors.toSet());
            } else {
                return Collections.emptySet();
            }
        } else {
            return Collections.singleton(propertyName);
        }
    }

}
