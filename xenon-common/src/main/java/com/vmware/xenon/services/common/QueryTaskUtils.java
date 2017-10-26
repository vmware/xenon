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
import java.util.Collection;
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
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.stream.Collectors;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

/**
 * Query task utility functions
 */
public final class QueryTaskUtils {

    private QueryTaskUtils() {

    }

    /**
     * The maximum depth level of which to expand the {@link TypeName#PODO} and {@link TypeName#COLLECTION}
     * properties when building {@link #getExpandedQueryPropertyNames(ServiceDocumentDescription)}
     */
    private static final int MAX_NEST_LEVEL_EXPAND_PROPERTY = 2;

    private static void mergeCountQueries(
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
        return;
    }

    /**
     * Process the query task results for a broadcast query or a query with read
     * after write semantics (which uses a broadcast query under the covers)
     * @param host The service host on which the query was invoked
     * @param dataSources A list of @ServiceDocumentQueryResult <b>sorted</b> on <i>documentLink</i>.
     * @param isAscOrder  Whether the document links are sorted in ascending order.
     * @param queryOptions Query options on the original query
     * @param nodeGroupResponse Node group response obtained as part of the broadcast query
     * @param result Result object to populate
     * @param onCompletion Consumer to invoke once processing is complete
     */
    public static void processQueryResults(ServiceHost host,
            List<ServiceDocumentQueryResult> dataSources,
            boolean isAscOrder, EnumSet<QueryOption> queryOptions,
            NodeGroupBroadcastResponse nodeGroupResponse, ServiceDocumentQueryResult result,
            BiConsumer<ServiceDocumentQueryResult, Throwable> onCompletion) {
        if (queryOptions != null && queryOptions.contains(QueryOption.READ_AFTER_WRITE_CONSISTENCY)) {
            mergeForReadAfterWriteConsistency(host, dataSources, isAscOrder, queryOptions,
                    nodeGroupResponse, result, onCompletion);
        } else {
            mergeQueryResults(dataSources, isAscOrder, queryOptions, result);
            onCompletion.accept(result, null);
        }
    }
    /**
    * Merges a list of @ServiceDocumentQueryResult that were already <b>sorted</b> on <i>documentLink</i>.
    * The merge will be done in linear time.
    *
    * @param dataSources A list of @ServiceDocumentQueryResult <b>sorted</b> on <i>documentLink</i>.
    * @param isAscOrder  Whether the document links are sorted in ascending order.
    * @return The merging result.
    */
    public static void mergeQueryResults(
            List<ServiceDocumentQueryResult> dataSources, boolean isAscOrder, ServiceDocumentQueryResult result) {
        mergeQueryResults(dataSources, isAscOrder, EnumSet.noneOf(QueryOption.class), result);
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
    public static void mergeQueryResults(
            List<ServiceDocumentQueryResult> dataSources,
            boolean isAscOrder, EnumSet<QueryOption> queryOptions, ServiceDocumentQueryResult result) {

        result.documents = new HashMap<>();
        result.documentCount = 0L;
        // handle count queries
        if (queryOptions != null && queryOptions.contains(QueryOption.COUNT)) {
            mergeCountQueries(dataSources, result);
            return;
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
    }

    /**
    * Merges a list of @ServiceDocumentQueryResult that were already <b>sorted</b> on <i>documentLink</i>.
    * The result will be the latest version of the document across the nodegroup ensuring a read
    * after write consistency for queries
    *
    * @param host The service host on which the query was invoked
    * @param dataSources A list of @ServiceDocumentQueryResult <b>sorted</b> on <i>documentLink</i>.
    * @param isAscOrder  Whether the document links are sorted in ascending order.
    * @param queryOptions Query options on the original query
    * @param nodeGroupResponse Node group response obtained as part of the broadcast query
    * @param onCompletion Consumer to invoke once processing is complete
    * @return Merged result.
    */
    private static void mergeForReadAfterWriteConsistency(
            ServiceHost host,
            List<ServiceDocumentQueryResult> dataSources,
            boolean isAscOrder, EnumSet<QueryOption> queryOptions,
            NodeGroupBroadcastResponse nodeGroupResponse,
            ServiceDocumentQueryResult returnResult,
            BiConsumer<ServiceDocumentQueryResult, Throwable> onCompletion) {

        class VersionObjectPair {
            Long version;
            Object object;

            VersionObjectPair(Long version, Object object) {
                this.version = version;
                this.object = object;
            }
        }

        // if we do not have a majority quorum setting, then it is not possible to
        // ensure read after write consistency
        if (nodeGroupResponse.membershipQuorum < (nodeGroupResponse.nodeCount / 2 + 1 )) {
            onCompletion.accept(null, new IllegalStateException("Membership quorum value should be "
                    + " a majority of the number of nodes"));
            return;
        }
        // track the count of each selfLink and the latest version per selfLink
        Map<String, Integer> linkToCountMap = new HashMap<>();
        Map<String, VersionObjectPair> linkToVersionObjectMap = new HashMap<>();
        for (int i = 0; i < dataSources.size(); i++) {
            ServiceDocumentQueryResult partialResult = dataSources.get(i);
            for ( Object entry: partialResult.documents.values()) {
                ServiceDocument jsonObject = Utils.fromJson(entry, ServiceDocument.class);
                linkToCountMap.compute(jsonObject.documentSelfLink,
                        (k, v) -> (v == null) ? Integer.valueOf(1) : v.intValue() + 1);
                linkToVersionObjectMap.compute(jsonObject.documentSelfLink,
                        (k, v) -> {
                            if (v == null) {
                                return new VersionObjectPair(jsonObject.documentVersion, entry);
                            } else {
                                if ((v.version.intValue() < jsonObject.documentVersion)) {
                                    return new VersionObjectPair(jsonObject.documentVersion, entry);
                                } else {
                                    return v;
                                }
                        }
                    });
            }
        }
        Set<String> getLinks = new HashSet<>();
        Map<String, Object> documents = new HashMap<>();
        for (Entry<String, Integer> entry : linkToCountMap.entrySet()) {
            // if we have received response from a quorum number of nodes then we
            // have the latest version of the doc as at least one node
            // will have the updated doc;
            // else, invoke a GET to fetch the latest version of the doc
            if (entry.getValue() >= nodeGroupResponse.membershipQuorum) {
                documents.put(entry.getKey(),
                        linkToVersionObjectMap.get(entry.getKey()).object);
            } else {
                getLinks.add(entry.getKey());
            }
        }
        Collection<Operation> getOps = new ArrayList<>();
        getLinks.forEach((link) -> {
            getOps.add(Operation
                    .createGet(UriUtils.buildUri(host, link))
                    .setReferer(host.getUri()));
        });
        if (getOps.size() == 0) {
            onCompletion.accept(populateResultObject(documents, queryOptions, returnResult, isAscOrder), null);
            return;
        }
        OperationJoin.create(getOps)
        .setCompletion((os, ts) -> {
            if (ts != null && !ts.isEmpty()) {
                onCompletion.accept(null, ts.values().iterator().next());
                return;
            }
            for (Operation getOp : os.values()) {
                Object rawObject = getOp.getBodyRaw();
                ServiceDocument jsonObject = Utils.fromJson(rawObject, ServiceDocument.class);
                documents.put(jsonObject.documentSelfLink, rawObject);
            }
            onCompletion.accept(populateResultObject(documents, queryOptions, returnResult, isAscOrder), null);
        }).sendWith(host);
    }

    private static ServiceDocumentQueryResult populateResultObject(Map<String, Object> documents,
            EnumSet<QueryOption> queryOptions, ServiceDocumentQueryResult returnResult, boolean isAscOrder) {
        List<String> documentLinks = new ArrayList<>(documents.keySet());
        Collections.sort(documentLinks, isAscOrder ? null : Collections.reverseOrder());
        returnResult.documentLinks = documentLinks;
        returnResult.documentCount = Long.valueOf(documentLinks.size());
        if (queryOptions.contains(QueryOption.EXPAND_CONTENT)) {
            returnResult.documents = documents;
        }
        return returnResult;
    }

    public static void expandLinks(ServiceHost host, QueryTask task, Operation op) {
        ServiceDocumentQueryResult result = task.results;
        if (!task.querySpec.options.contains(QueryOption.EXPAND_LINKS) || result == null
                || result.selectedLinksPerDocument == null || result.selectedLinksPerDocument.isEmpty()) {
            op.setBodyNoCloning(task).complete();
            return;
        }

        Map<String, Object> uniqueLinkToState = new ConcurrentSkipListMap<>();
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
            uniqueLinkToState.put(link, body);
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
                        .map(p -> QuerySpecification.buildCompositeFieldName(propertyName, p))
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
                        .map(p -> QuerySpecification.buildCompositeFieldName(propertyName, p))
                        .collect(Collectors.toSet());
            } else {
                return Collections.emptySet();
            }
        } else {
            return Collections.singleton(propertyName);
        }
    }

    public static void failTask(Operation get, Throwable ex) {
        QueryTask t = new QueryTask();
        t.taskInfo.stage = TaskState.TaskStage.FAILED;
        t.taskInfo.failure = Utils.toServiceErrorResponse(ex);
        get.setBody(t).fail(ex);
    }

}
