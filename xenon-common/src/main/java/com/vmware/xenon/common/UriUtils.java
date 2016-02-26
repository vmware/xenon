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

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.TransactionResolutionService;

/**
 * URI utility functions
 */
public class UriUtils {

    public static enum ForwardingTarget {
        PEER_ID,
        KEY_HASH,
        ALL
    }

    public static final String FORWARDING_URI_PARAM_NAME_QUERY = "query";
    public static final String FORWARDING_URI_PARAM_NAME_TARGET = "target";
    public static final String FORWARDING_URI_PARAM_NAME_PATH = "path";
    public static final String FORWARDING_URI_PARAM_NAME_KEY = "key";
    public static final String FORWARDING_URI_PARAM_NAME_PEER = "peer";

    public static final String URI_PARAM_ODATA_EXPAND = "$expand";
    public static final String URI_PARAM_ODATA_EXPAND_NO_DOLLAR_SIGN = "expand";
    public static final String URI_PARAM_ODATA_FILTER = "$filter";
    public static final String URI_PARAM_ODATA_SKIP = "$skip";
    public static final String URI_PARAM_ODATA_ORDER_BY = "$orderby";
    public static final String URI_PARAM_ODATA_ORDER_BY_VALUE_ASC = "asc";
    public static final String URI_PARAM_ODATA_ORDER_BY_VALUE_DESC = "desc";
    public static final String URI_PARAM_ODATA_TOP = "$top";
    public static final String HTTP_SCHEME = "http";
    public static final String HTTPS_SCHEME = "https";
    public static final int HTTP_DEFAULT_PORT = 80;
    public static final int HTTPS_DEFAULT_PORT = 443;
    public static final String URI_PATH_CHAR = "/";
    public static final String URI_QUERY_CHAR = "?";
    public static final String URI_QUERY_PARAM_LINK_CHAR = "&";
    public static final String URI_WILDCARD_CHAR = "*";
    public static final String URI_QUERY_PARAM_KV_CHAR = "=";
    public static final String URI_PARAM_CAPABILITY = "capability";
    public static final String URI_PARAM_INCLUDE_DELETED = "includeDeleted";
    public static final String FIELD_NAME_SELF_LINK = "SELF_LINK";
    public static final String FIELD_NAME_FACTORY_LINK = "FACTORY_LINK";

    /**
     * Computes the parent path of the specified path.
     *
     * @param path
     * @return the parent of the specified path, or {@code null} if the specified path is
     *         {@code "/"}.
     */
    public static String getParentPath(String path) {
        int parentPathIndex = path.lastIndexOf(UriUtils.URI_PATH_CHAR);
        if (parentPathIndex > 0) {
            return path.substring(0, parentPathIndex);
        }
        if (parentPathIndex == 0 && path.length() > 1) {
            return URI_PATH_CHAR;
        }
        return null;
    }

    /**
     * Determines whether the path represents a child path of the specified path.
     *
     * E.g.
     *
     * isChildPath("/x/y/z", "/x/y") -> true
     *
     * isChildPath("y/z", "y") -> true
     *
     * isChildPath("/x/y/z", "/x/w") -> false
     *
     * isChildPath("y/z", "/x/y") -> false
     *
     * isChildPath("/x/yy/z", "/x/y") -> false
     */
    public static boolean isChildPath(String path, String parentPath) {
        if (parentPath == null || path == null) {
            return false;
        }
        // check if path begins with parent path
        if (!path.startsWith(parentPath)) {
            return false;
        }
        // in the event parent path did not include the path char
        if (!parentPath.endsWith(URI_PATH_CHAR)
                && !path.startsWith(URI_PATH_CHAR, parentPath.length())) {
            return false;
        }
        return true;
    }

    public static URI buildTransactionUri(ServiceHost host, String txid) {
        return buildUri(host.getUri(), ServiceUriPaths.CORE_TRANSACTIONS, txid);
    }

    public static URI buildTransactionResolutionUri(ServiceHost host, String txid) {
        return buildUri(host.getUri(), ServiceUriPaths.CORE_TRANSACTIONS, txid,
                TransactionResolutionService.RESOLUTION_SUFFIX);
    }

    public static URI buildSubscriptionUri(ServiceHost host, String path) {
        return extendUri(host.getUri(),
                UriUtils.buildUriPath(path, ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS));
    }

    public static URI buildSubscriptionUri(URI serviceUri) {
        return extendUri(serviceUri, ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS);
    }

    public static URI buildStatsUri(ServiceHost host, String path) {
        return extendUri(host.getUri(),
                UriUtils.buildUriPath(path, ServiceHost.SERVICE_URI_SUFFIX_STATS));
    }

    public static URI buildStatsUri(URI serviceUri) {
        return extendUri(serviceUri, ServiceHost.SERVICE_URI_SUFFIX_STATS);
    }

    public static URI buildConfigUri(ServiceHost host, String path) {
        return extendUri(host.getUri(),
                UriUtils.buildUriPath(path, ServiceHost.SERVICE_URI_SUFFIX_CONFIG));
    }

    public static URI buildConfigUri(URI serviceUri) {
        return extendUri(serviceUri, ServiceHost.SERVICE_URI_SUFFIX_CONFIG);
    }

    public static URI buildAvailableUri(ServiceHost host, String path) {
        return extendUri(host.getUri(),
                UriUtils.buildUriPath(path, ServiceHost.SERVICE_URI_SUFFIX_AVAILABLE));
    }

    public static URI buildAvailableUri(URI serviceUri) {
        return extendUri(serviceUri, ServiceHost.SERVICE_URI_SUFFIX_AVAILABLE);
    }

    public static URI extendUri(URI uri, String path) {
        String query = null;
        if (path != null && path.contains(UriUtils.URI_QUERY_CHAR)) {
            String[] pathAndQuery = path.split("\\" + UriUtils.URI_QUERY_CHAR);
            path = pathAndQuery[0];
            query = pathAndQuery[1];
        }

        return buildUri(uri.getScheme(), uri.getHost(), uri.getPort(),
                normalizeUriPath(uri.getPath()) + normalizeUriPath(path),
                query);
    }

    public static URI buildUri(String host, int port, String path, String query) {
        return buildUri(HTTP_SCHEME, host, port, path, query);
    }

    public static URI buildUri(ServiceHost host, String path) {
        return buildUri(host, path, null);
    }

    public static URI buildUri(ServiceHost host, String path, String query) {
        URI base = host.getUri();
        return UriUtils.buildUri(base.getScheme(), base.getHost(), base.getPort(), path, query);
    }

    public static URI buildUri(String scheme, String host, int port, String path, String query) {
        try {
            if (path != null && path.contains(UriUtils.URI_QUERY_CHAR)) {
                String[] pathAndQuery = path.split("\\" + UriUtils.URI_QUERY_CHAR);
                path = pathAndQuery[0];
                query = pathAndQuery[1];
            }
            path = normalizeUriPath(path);
            return new URI(scheme, null, host, port, path, query, null).normalize();
        } catch (URISyntaxException e) {
            Utils.log(UriUtils.class, Utils.class.getSimpleName(), Level.SEVERE, "%s",
                    Utils.toString(e));
            return null;
        }
    }


    public static String normalizeUriPath(String path) {
        if (path == null) {
            return "";
        }
        if (path.isEmpty()) {
            return path;
        }
        if (path.endsWith(URI_PATH_CHAR)) {
            path = path.substring(0, path.length() - 1);
        }
        if (path.startsWith(URI_PATH_CHAR)) {
            return path;
        } else {
            return URI_PATH_CHAR + path;
        }
    }

    public static String buildUriPath(String... segments) {
        if (segments.length == 1) {
            return normalizeUriPath(segments[0]);
        }
        StringBuilder sb = new StringBuilder();
        for (String s : segments) {
            s = normalizeUriPath(s);
            if (s.endsWith(URI_PATH_CHAR)) {
                sb.append(s.substring(0, s.length() - 1));
            } else {
                sb.append(s);
            }
        }
        return normalizeUriPath(sb.toString());
    }

    public static URI buildUri(ServiceHost host, Class<? extends Service> type) {
        try {
            Field f = type.getField(FIELD_NAME_SELF_LINK);
            String path = (String) f.get(null);
            return buildUri(host, path);
        } catch (Exception e) {
            Utils.log(Utils.class, Utils.class.getSimpleName(), Level.SEVERE,
                    "%s field not found in class %s: %s", FIELD_NAME_SELF_LINK,
                    type.getSimpleName(),
                    Utils.toString(e));
        }
        return null;
    }

    public static URI buildFactoryUri(ServiceHost host, Class<? extends Service> type) {
        try {
            Field f = type.getField(FIELD_NAME_FACTORY_LINK);
            String path = (String) f.get(null);
            return buildUri(host, path);
        } catch (Exception e) {
            Utils.log(Utils.class, Utils.class.getSimpleName(), Level.SEVERE,
                    "%s field not found in class %s: %s", FIELD_NAME_FACTORY_LINK,
                    type.getSimpleName(),
                    Utils.toString(e));
        }
        return null;
    }

    /**
     * Builds a new URI using the scheme, authority, host and port from the baseUri, and the path
     * from the path argument
     */
    public static URI buildUri(URI baseUri, String... path) {
        String query = null;
        String buildPath = null;
        if (path == null || path.length == 0) {
            return baseUri;
        }
        for (String p : path) {
            if (p == null) {
                continue;
            }
            if (p.contains(UriUtils.URI_QUERY_CHAR)) {
                String[] pathAndQuery = p.split("\\" + UriUtils.URI_QUERY_CHAR);
                p = pathAndQuery[0];
                if (query == null) {
                    query = pathAndQuery[1];
                } else {
                    query += pathAndQuery[1];
                }
            }
            p = normalizeUriPath(p);
            if (buildPath == null) {
                buildPath = p;
            } else {
                buildPath += p;
            }
        }

        try {
            return new URI(baseUri.getScheme(), baseUri.getUserInfo(), baseUri.getHost(),
                    baseUri.getPort(), buildPath, query, null).normalize();
        } catch (Throwable e) {
            Utils.log(Utils.class, Utils.class.getSimpleName(), Level.SEVERE,
                    "Failure building uri %s, %s: %s", baseUri, path,
                    Utils.toString(e));
        }
        return null;
    }

    /**
     * Build new URI based on a canonical URL, i.e., "http://example.com/example")
     */
    public static URI buildUri(String uri) {
        try {
            return new URI(uri);
        } catch (Throwable e) {
            Utils.log(Utils.class, Utils.class.getSimpleName(), Level.SEVERE,
                    "Failure building uri %s: %s", uri, Utils.toString(e));
        }
        return null;
    }

    public static URI extendUriWithQuery(URI u, String... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "keyValues array length must be even, with key and value pairs interleaved");
        }
        StringBuilder sb = new StringBuilder();

        boolean doKey = true;
        boolean isFirst = u.getQuery() == null ? true : false;
        for (String s : keyValues) {
            if (doKey) {
                if (!isFirst) {
                    sb.append("&");
                } else {
                    isFirst = false;
                }
                sb.append(s).append("=");
            } else {
                sb.append(s);
            }
            doKey = !doKey;

        }

        try {
            String query = u.getQuery() == null ? sb.toString() : u.getQuery() + sb.toString();
            u = new URI(u.getScheme(), null, u.getHost(),
                    u.getPort(), u.getPath(), query, null);
            return u;
        } catch (URISyntaxException e) {
            Utils.log(UriUtils.class, Utils.class.getSimpleName(), Level.SEVERE, "%s",
                    Utils.toString(e));
            return null;
        }
    }

    public static URI buildDocumentQueryUri(ServiceHost host,
            String selfLink,
            boolean doExpand,
            boolean includeDeleted,
            EnumSet<ServiceOption> serviceCaps) {
        ServiceOption queryCap = ServiceOption.NONE;
        if (serviceCaps.contains(ServiceOption.PERSISTENCE)) {
            queryCap = ServiceOption.PERSISTENCE;
        }
        return buildDocumentQueryUri(host, selfLink, doExpand, includeDeleted, queryCap);
    }

    public static URI buildDocumentQueryUri(ServiceHost host,
            String selfLink,
            boolean doExpand,
            boolean includeDeleted,
            ServiceOption cap) {

        URI indexUri = host.getDocumentIndexServiceUri();
        return buildIndexQueryUri(indexUri,
                selfLink, doExpand, includeDeleted, cap);
    }

    public static URI buildOperationTracingQueryUri(ServiceHost host,
            String selfLink,
            boolean doExpand,
            boolean includeDeleted,
            ServiceOption cap) {

        URI hostURI = UriUtils.extendUri(host.getUri(), ServiceUriPaths.CORE_OPERATION_INDEX);
        return buildIndexQueryUri(hostURI,
                selfLink, doExpand, includeDeleted, cap);
    }

    public static URI buildIndexQueryUri(URI indexURI,
            String selfLink,
            boolean doExpand,
            boolean includeDeleted,
            ServiceOption cap) {

        if (cap == null) {
            cap = ServiceOption.NONE;
        }
        List<String> queryArgs = new ArrayList<>();
        queryArgs.add(ServiceDocument.FIELD_NAME_SELF_LINK);
        queryArgs.add(selfLink);
        queryArgs.add(URI_PARAM_CAPABILITY);
        queryArgs.add(cap.toString());
        if (includeDeleted) {
            queryArgs.add(URI_PARAM_INCLUDE_DELETED);
            queryArgs.add(Boolean.TRUE.toString());
        }

        if (doExpand) {
            queryArgs.add(URI_PARAM_ODATA_EXPAND);
            queryArgs.add(ServiceDocumentQueryResult.FIELD_NAME_DOCUMENT_LINKS);
        }

        return extendUriWithQuery(indexURI, queryArgs.toArray(new String[0]));
    }

    public static URI appendQueryParam(URI uri, String param, String value) {
        List<String> queryArgs = new ArrayList<>();
        queryArgs.add(param);
        queryArgs.add(value);
        return extendUriWithQuery(uri, queryArgs.toArray(new String[0]));
    }

    public static Map<String, String> parseUriQueryParams(URI uri) {
        Map<String, String> params = new HashMap<String, String>();
        String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return params;
        }

        String[] keyValuePairs = query.split(URI_QUERY_PARAM_LINK_CHAR);
        for (String kvPair : keyValuePairs) {
            String[] kvSplit = kvPair.split(URI_QUERY_PARAM_KV_CHAR);
            if (kvSplit.length < 2) {
                continue;
            }
            String key = kvSplit[0];
            String value = kvSplit[1];
            if (kvSplit.length > 2) {
                StringBuilder sb = new StringBuilder();
                sb.append(value);
                for (int i = 2; i < kvSplit.length; i++) {
                    sb.append(URI_QUERY_PARAM_KV_CHAR + kvSplit[i]);
                }
                value = sb.toString();
            }
            params.put(key, value);
        }
        return params;
    }

    public static URI buildExpandLinksQueryUri(URI factoryServiceUri) {
        return extendUriWithQuery(factoryServiceUri, UriUtils.URI_PARAM_ODATA_EXPAND,
                ServiceDocumentQueryResult.FIELD_NAME_DOCUMENT_LINKS);
    }

    /**
     * Returns true if the host name and port in the URI are the same as in the host instance
     */
    public static boolean isHostEqual(ServiceHost host, URI remoteService) {
        ServiceHostState hostState = host.getState();
        if (hostState == null || !hostState.isStarted) {
            throw new IllegalStateException("Host not in valid state");
        }

        if (host.getState().systemInfo.ipAddresses == null
                || host.getState().systemInfo.ipAddresses.isEmpty()) {
            throw new IllegalStateException("No IP addresses found in host:" + host.toString());
        }

        if (host.getPort() != remoteService.getPort()
                && host.getSecurePort() != remoteService.getPort()) {
            return false;
        }

        for (String address : host.getState().systemInfo.ipAddresses) {
            if (address.equals(remoteService.getHost())) {
                return true;
            }
        }

        return false;
    }

    public static String buildPathWithVersion(String link, Long latestVersion) {
        return link + UriUtils.URI_QUERY_CHAR + ServiceDocument.FIELD_NAME_VERSION
                + UriUtils.URI_QUERY_PARAM_KV_CHAR + latestVersion;
    }

    public static URI buildPublicUri(ServiceHost host, String... path) {
        return buildPublicUri(host, UriUtils.buildUriPath(path), null);
    }

    public static URI buildPublicUri(ServiceHost host, String path, String query) {
        URI baseUri = host.getPublicUri();
        try {
            return new URI(baseUri.getScheme(), baseUri.getUserInfo(), baseUri.getHost(),
                    baseUri.getPort(), path, query, null);
        } catch (Throwable e) {
            Utils.log(Utils.class, Utils.class.getSimpleName(), Level.SEVERE,
                    "Failure building uri %s, %s, %s: %s", baseUri, path, query,
                    Utils.toString(e));
        }
        return null;
    }

    /**
     * Builds a forwarder service URI using the target service path as the node selection key.
     * If the key argument is supplied, it is used instead
     */
    public static URI buildForwardRequestUri(URI targetService, String key, String selectorPath) {
        URI u = UriUtils.buildUri(targetService, UriUtils.buildUriPath(
                selectorPath,
                ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING));
        if (key == null) {
            key = targetService.getPath();
        }

        u = UriUtils.extendUriWithQuery(u, FORWARDING_URI_PARAM_NAME_PATH,
                key,
                FORWARDING_URI_PARAM_NAME_TARGET,
                ForwardingTarget.KEY_HASH.toString());
        return u;
    }

    public static URI buildForwardToPeerUri(URI targetService, String peerId, String selectorPath,
            EnumSet<ServiceOption> caps) {
        URI u = UriUtils.buildUri(targetService, UriUtils.buildUriPath(
                selectorPath,
                ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING));
        String query = targetService.getQuery();
        if (query == null) {
            query = "";
        } else {
            query += "";
        }

        u = UriUtils.extendUriWithQuery(u, FORWARDING_URI_PARAM_NAME_PEER,
                peerId,
                FORWARDING_URI_PARAM_NAME_PATH,
                targetService.getPath(),
                FORWARDING_URI_PARAM_NAME_QUERY,
                query,
                FORWARDING_URI_PARAM_NAME_TARGET,
                ForwardingTarget.PEER_ID.toString());
        return u;
    }

    public static URI buildBroadcastRequestUri(URI targetService, String selectorPath) {
        URI u = UriUtils.buildUri(targetService, UriUtils.buildUriPath(selectorPath,
                ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING));
        u = UriUtils.extendUriWithQuery(u, FORWARDING_URI_PARAM_NAME_PATH,
                targetService.getPath(),
                FORWARDING_URI_PARAM_NAME_TARGET,
                ForwardingTarget.ALL.toString());
        return u;
    }

    public static URI updateUriPort(URI uri, int newPort) {
        if (uri == null) {
            return null;
        }
        if (uri.getPort() == newPort) {
            return uri;
        }
        return UriUtils.buildUri(uri.getScheme(),
                uri.getHost(),
                newPort,
                uri.getPath(),
                uri.getQuery());
    }

    public static String getODataFilterParamValue(URI uri) {
        String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }

        if (!query.contains(URI_PARAM_ODATA_FILTER)) {
            return null;
        }

        Map<String, String> queryParams = parseUriQueryParams(uri);

        String filterParamValue = queryParams.get(URI_PARAM_ODATA_FILTER);
        if (filterParamValue == null || filterParamValue.isEmpty()) {
            return null;
        }

        return filterParamValue;
    }

    public static Integer getODataSkipParamValue(URI uri) {
        String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }

        if (!query.contains(URI_PARAM_ODATA_SKIP)) {
            return null;
        }

        Map<String, String> queryParams = parseUriQueryParams(uri);

        String paramValue = queryParams.get(URI_PARAM_ODATA_SKIP);
        if (paramValue == null || paramValue.isEmpty()) {
            return null;
        }
        return Integer.valueOf(paramValue);
    }

    public static Integer getODataTopParamValue(URI uri) {
        String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }

        if (!query.contains(URI_PARAM_ODATA_TOP)) {
            return null;
        }

        Map<String, String> queryParams = parseUriQueryParams(uri);

        String paramValue = queryParams.get(URI_PARAM_ODATA_TOP);
        if (paramValue == null || paramValue.isEmpty()) {
            return null;
        }
        return Integer.valueOf(paramValue);
    }

    public enum ODataOrder {
        ASC, DESC
    }

    public static class ODataOrderByTuple {
        public ODataOrder order;
        public String propertyName;
    }

    public static ODataOrderByTuple getODataOrderByParamValue(URI uri) {
        String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }

        if (!query.contains(URI_PARAM_ODATA_ORDER_BY)) {
            return null;
        }

        Map<String, String> queryParams = parseUriQueryParams(uri);

        String paramValue = queryParams.get(URI_PARAM_ODATA_ORDER_BY);
        if (paramValue == null || paramValue.isEmpty()) {
            return null;
        }

        ODataOrderByTuple tuple = new ODataOrderByTuple();
        if (paramValue.contains(URI_PARAM_ODATA_ORDER_BY_VALUE_DESC)) {
            tuple.order = ODataOrder.DESC;
            paramValue = paramValue.replace(URI_PARAM_ODATA_ORDER_BY_VALUE_DESC, "");
        } else if (paramValue.contains(URI_PARAM_ODATA_ORDER_BY_VALUE_ASC)) {
            // default is ascending
            tuple.order = ODataOrder.ASC;
            paramValue = paramValue.replace(URI_PARAM_ODATA_ORDER_BY_VALUE_ASC, "");
        } else {
            throw new IllegalArgumentException("invalid expression: " + paramValue);
        }

        paramValue = paramValue.trim();
        paramValue = paramValue.replaceAll("\\+", "");
        paramValue = paramValue.replaceAll(" ", "");
        paramValue = paramValue.replaceAll("0x20", "");

        tuple.propertyName = paramValue;
        return tuple;
    }

    public static boolean hasODataExpandParamValue(URI uri) {
        boolean doExpand = uri.getQuery().contains(UriUtils.URI_PARAM_ODATA_EXPAND);
        if (!doExpand) {
            doExpand = uri.getQuery().contains(UriUtils.URI_PARAM_ODATA_EXPAND_NO_DOLLAR_SIGN);
        }
        return doExpand;
    }

    /**
     * Get the last part of a selflink, excluding any query string
     */
    public static String getLastPathSegment(URI uri) {
        String path = uri.getPath();
        return getLastPathSegment(path);
    }

    /**
     * Returns the last path segment
     */
    public static String getLastPathSegment(String link) {
        if (link == null) {
            throw new IllegalArgumentException("link is required");
        }
        if (link.endsWith(UriUtils.URI_PATH_CHAR)) {
            // degenerate case, link is of the form "root/path1/", instead of "root/path1"
            return "";
        }
        return link.substring(link.lastIndexOf(UriUtils.URI_PATH_CHAR) + 1);
    }
}
