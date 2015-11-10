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

package com.vmware.dcp.common.http.netty;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import io.netty.handler.codec.http.Cookie;

/**
 * The cookie jar keeps all cookies for a client. Upon receiving a Set-Cookie response header, the cookie jar may store
 * the specified cookie. Upon sending a request, the cookie jar can provide a list of cookies that apply.
 *
 * TODO:
 * - implement cookie expiration through maxAge
 * - implement support for "domain" modifier
 * - implement support for "path" modifier
 */
public class CookieJar {
    static final String URI_SCHEME_HTTPS = "https";

    /**
     * Cookies indexed by the remote host that added them to this jar.
     */
    Map<String, Set<Cookie>> cookiesByOrigin = new ConcurrentSkipListMap<>();

    /**
     * Builds an identifier for the origin of a cookie.
     *
     * @param uri remote URI that resulted in a cookie being set
     * @return {@code String}
     */
    static String buildOrigin(URI uri) {
        return String.format("%s:%d", uri.getHost(), uri.getPort());
    }

    /**
     * Adds cookie to the jar.
     *
     * @param uri request URI that resulted in the cookie being set
     * @param cookie
     */
    public void add(URI uri, Cookie cookie) {
        String origin = buildOrigin(uri);
        Set<Cookie> byOrigin = this.cookiesByOrigin.get(origin);
        if (byOrigin == null) {
            this.cookiesByOrigin.putIfAbsent(origin, new ConcurrentSkipListSet<>());
            byOrigin = this.cookiesByOrigin.get(origin);
        }

        byOrigin.add(cookie);
    }

    /**
     * List cookies that apply for the given request URI.
     *
     * @param uri request URI
     */
    public Map<String, String> list(URI uri) {
        Map<String, String> result = new HashMap<>();

        // Find cookies by the request URI
        String origin = buildOrigin(uri);
        Set<Cookie> byOrigin = this.cookiesByOrigin.get(origin);
        if (byOrigin != null) {
            for (Cookie cookie : byOrigin) {
                // Only add "secure" cookies if request URI has https scheme
                if (cookie.isSecure() && !uri.getScheme().equals(URI_SCHEME_HTTPS)) {
                    continue;
                }

                result.put(cookie.name(), cookie.value());
            }
        }

        return result;
    }

    public static String encodeCookies(Map<String, String> cookies) {
        Iterator<Entry<String, String>> it = cookies.entrySet().iterator();
        StringBuilder buf = new StringBuilder();
        for (int i = 0; it.hasNext();) {
            if (i++ > 0) {
                buf.append("; ");
            }
            Entry<String, String> e = it.next();
            buf.append(e.getKey()).append("=").append(e.getValue());
        }
        return buf.toString();
    }

    public static Map<String, String> decodeCookies(String header) {
        Map<String, String> cookies = new HashMap<>();
        String [] cookiePairs = header.split(";");
        if (cookiePairs.length != 0) {
            for (String cookiePair : cookiePairs) {
                String[] keyVal = cookiePair.split("=");
                if (keyVal.length == 2) {
                    cookies.put(keyVal[0].trim(), keyVal[1].trim());
                }
            }
        }
        return cookies;
    }

    public boolean isEmpty() {
        return this.cookiesByOrigin.isEmpty();
    }
}
