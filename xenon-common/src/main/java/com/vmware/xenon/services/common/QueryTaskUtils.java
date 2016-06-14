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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

/**
 * Query task utility functions
 */
public class QueryTaskUtils {

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

            int r = remaining.decrementAndGet();
            Object body = o.getBodyRaw();

            try {
                String json = Utils.toJson(body);
                uniqueLinkToState.put(link, json);
                host.log(Level.INFO, "Received response for %s: %s", link, json);
            } catch (Throwable ex) {
                host.log(Level.WARNING, "Failure serializing response for %s: %s", link,
                        ex.getMessage());
            }

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

}
