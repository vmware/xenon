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

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.vmware.xenon.common.ODataQueryVisitor.BinaryVerb;
import com.vmware.xenon.common.ODataToken;
import com.vmware.xenon.common.ODataTokenList;
import com.vmware.xenon.common.ODataTokenizer;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

public class RootNamespaceService extends StatelessService {
    public static final String SELF_LINK = "/";

    public RootNamespaceService() {
        super();
    }

    @Override
    /**
     * Returns a list of services, by default only those with ServiceOption.FACTORY.
     * Query parameter support permits the overriding of which services are returned by ServiceOption
     *   includes=comma-separated-list  ("ALL" wildcard == all options)
     *   excludes=comma-separated-list
     *   matchAllOptions=(true|false) - default false
     */
    public void handleGet(Operation get) {

        EnumSet<ServiceOption> includeOptions = EnumSet.of(ServiceOption.FACTORY);
        EnumSet<ServiceOption> excludeOptions = null;
        boolean matchAllOptions = false;

        URI uri = get.getUri();
        if (uri.getQuery() != null) {
            Map<String, String> queryParams = UriUtils.parseUriQueryParams(uri);
            String[] includes = getValueList(queryParams, "includes");
            if (includes != null) {
                Set<ServiceOption> includeOptionsSet = new HashSet<>();
                for (String option: includes) {
                    if (option.equals("ALL")) {
                        includeOptionsSet.addAll(Arrays.asList(ServiceOption.values()));
                    } else {
                        includeOptionsSet.add(ServiceOption.valueOf(option));
                    }
                }
                includeOptions = EnumSet.copyOf(includeOptionsSet);
            }
            String[] excludes = getValueList(queryParams, "excludes");
            if (excludes != null) {
                Set<ServiceOption> excludeOptionsSet = new HashSet<>();
                for (String option: excludes) {
                    excludeOptionsSet.add(ServiceOption.valueOf(option));
                }
                excludeOptions = EnumSet.copyOf(excludeOptionsSet);
            }
            String matchQueryParam = queryParams.get("matchAllOptions");
            if (matchQueryParam != null) {
                matchAllOptions = Boolean.valueOf(matchQueryParam);
            }
        }

        // backward compatibility allowing use of $filter(options eq 'STATELESS')
        String filter = UriUtils.getODataFilterParamValue(get.getUri());
        if (filter != null) {
            ODataTokenizer tokenizer = new ODataTokenizer(filter);
            ODataTokenList list = tokenizer.tokenize();

            while (list.hasNext()) {
                ODataToken tok = list.next();
                if (!tok.getUriLiteral().equals("options")) {
                    continue;
                }

                if (list.hasNext() && list.next().getUriLiteral().equals(BinaryVerb.EQ.operator)) {
                    if (list.hasNext()) {
                        String value = list.next().getUriLiteral();
                        // remove any single quoting of value
                        if (value.startsWith("'") && value.endsWith("'")) {
                            value = value.substring(1, value.length() - 1);
                        }
                        includeOptions.add(ServiceOption.valueOf(value));
                    }
                    break;
                }
            }
        }

        get.nestCompletion((o, e) -> {
            if (e == null) {
                Collections.sort(o.getBody(ServiceDocumentQueryResult.class).documentLinks);
            }
            get.complete();
        });

        getHost().queryServiceUris(includeOptions, matchAllOptions, get, excludeOptions);
    }

    private static String[] getValueList(Map<String, String> queryParams, String queryParam) {
        String value = queryParams.get(queryParam);
        if (value == null) {
            return null;
        }
        return value.split("[, ]");
    }
}
