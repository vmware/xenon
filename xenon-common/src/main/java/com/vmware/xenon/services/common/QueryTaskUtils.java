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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

    public static void expandLinks(ServiceHost host, QueryTask task, Operation op) {
        ServiceDocumentQueryResult result = task.results;
        if (!task.querySpec.options.contains(QueryOption.EXPAND_LINKS) || result == null
                || result.selectedLinks == null || result.selectedLinks.isEmpty()) {
            op.setBodyNoCloning(task).complete();
            return;
        }

        Map<String, String> uniqueLinkToState = new ConcurrentSkipListMap<>();
        for (Map<String, String> selectedLinksPerDocument : result.selectedLinks.values()) {
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

            Map<String, String> updatedLinkValues = new HashMap<>();

            for (Map<String, String> selectedLinks : result.selectedLinks.values()) {
                for (Entry<String, String> en : selectedLinks.entrySet()) {
                    String state = uniqueLinkToState.get(en.getValue());
                    updatedLinkValues.put(en.getKey(), state);
                }
                for (Entry<String, String> en : updatedLinkValues.entrySet()) {
                    selectedLinks.put(en.getKey(), en.getValue());
                }
            }

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
        } else if (pd.typeName == TypeName.MAP) {
            // Map is not supported at the moment
            return Collections.emptySet();
        } else {
            return Collections.singleton(propertyName);
        }
    }

}
