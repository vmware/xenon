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

package com.vmware.xenon.common;

import java.net.URI;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

import com.vmware.xenon.common.ServiceStats.ServiceStat;

public final class AuthUtils {

    private static final ConcurrentMap<String, Function<Claims, String>> URI_BUILDERS = new ConcurrentSkipListMap<>();

    private AuthUtils() {
    }

    /**
     * Build the URI for the auth provider host
     */
    public static URI buildAuthProviderHostUri(ServiceHost host, String serviceLink) {
        URI uri = host.getStateNoCloning().authProviderHostURI;
        URI baseUri = uri != null ? uri : host.getUri();
        return UriUtils.extendUri(baseUri, serviceLink);
    }

    /**
     * Register a system-wide function for converting a Claims object to a use selfLink.
     * All hosts in a process will be affected.
     *
     * @param authnServiceLink
     * @param userLinkBuilder
     */
    public static void registerUserLinkBuilder(String authnServiceLink, Function<Claims, String> userLinkBuilder) {
        URI_BUILDERS.putIfAbsent(authnServiceLink, userLinkBuilder);
    }

    /**
     * Builds an URI to the subject of the claims object.
     * Default behavior treats {@link Claims#getSubject()} as a selfLink and is
     * used by the built-in {@link com.vmware.xenon.services.common.authn.BasicAuthenticationService}.
     * This can be customized using {@link #registerUserLinkBuilder(String, Function)} to register
     * a global link builder.
     *
     * @param host
     * @param claims
     * @return
     */
    public static URI buildUserUriFromClaims(ServiceHost host, Claims claims) {
        String authnServiceLink = host.getAuthenticationService().getSelfLink();

        String userLink = claims.getSubject();
        Function<Claims, String> func = URI_BUILDERS.get(authnServiceLink);
        if (func != null) {
            userLink = func.apply(claims);
        }

        return buildAuthProviderHostUri(host, userLink);
    }

    public static void setAuthDurationStat(Service service, String prefix, double value) {
        ServiceStat st = ServiceStatUtils.getOrCreateDailyTimeSeriesHistogramStat(service, prefix,
                ServiceStatUtils.AGGREGATION_TYPE_AVG_MAX);
        service.setStat(st, value);
        st = ServiceStatUtils.getOrCreateHourlyTimeSeriesHistogramStat(service, prefix,
                ServiceStatUtils.AGGREGATION_TYPE_AVG_MAX);
        service.setStat(st, value);
    }
}
