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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * URI utility functions
 */
public final class UriUtils {

    public enum ForwardingTarget {
        PEER_ID, KEY_HASH, ALL
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
    public static final String URI_PARAM_ODATA_ORDER_BY_TYPE = "$orderbytype";
    public static final String URI_PARAM_ODATA_ORDER_BY_VALUE_ASC = "asc";
    public static final String URI_PARAM_ODATA_ORDER_BY_VALUE_DESC = "desc";
    public static final String URI_PARAM_ODATA_TOP = "$top";
    public static final String URI_PARAM_ODATA_LIMIT = "$limit";
    public static final String URI_PARAM_ODATA_COUNT = "$count";
    public static final String URI_PARAM_ODATA_SKIP_TO = "$skipto";
    public static final String URI_PARAM_ODATA_NODE = "$nodeid";
    public static final String URI_PARAM_ODATA_TENANTLINKS = "$tenantLinks";
    public static final String HTTP_SCHEME = "http";
    public static final String HTTPS_SCHEME = "https";
    public static final int HTTP_DEFAULT_PORT = 80;
    public static final int HTTPS_DEFAULT_PORT = 443;
    public static final String URI_PATH_CHAR = "/";
    public static final String URI_QUERY_CHAR = "?";
    public static final String URI_QUERY_PARAM_LINK_CHAR = "&";
    public static final String URI_WILDCARD_CHAR = "*";
    public static final String URI_QUERY_PARAM_KV_CHAR = "=";
    public static final String URI_PATH_PARAM_REGEX = "\\{.*\\}";
    public static final String URI_PARAM_CAPABILITY = "capability";
    public static final String URI_PARAM_INCLUDE_DELETED = "includeDeleted";
    public static final String FIELD_NAME_SELF_LINK = "SELF_LINK";
    public static final String FIELD_NAME_FACTORY_LINK = "FACTORY_LINK";

    public static final Pattern pathParamPattern = Pattern.compile(URI_PATH_PARAM_REGEX);
    private static final Pattern TRIM_PATH_SLASHES_PATTERN = Pattern.compile("^/*|/*$");

    private static final char URI_PATH_CHAR_CONST = '/';
    private static final char URI_QUERY_CHAR_CONST = '?';
    private static final char URI_QUERY_PARAM_LINK_CHAR_CONST = '&';
    private static final char URI_QUERY_PARAM_KV_CHAR_CONST = '=';

    private UriUtils() {
    }

    /**
     * Computes the parent path of the specified path.
     *
     * @param path the path to be parsed
     * @return the parent of the specified path, or {@code null} if the specified path is
     *         {@code "/"}.
     */
    public static String getParentPath(String path) {
        int parentPathIndex = path.lastIndexOf(URI_PATH_CHAR_CONST);
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
                ServiceHost.SERVICE_URI_SUFFIX_CONFIG);
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
        if (path != null) {
            final int indexOfFirstQueryChar = path.indexOf(URI_QUERY_CHAR_CONST);
            if (indexOfFirstQueryChar >= 0) {
                if (indexOfFirstQueryChar < path.length() - 1) {
                    query = path.substring(indexOfFirstQueryChar + 1);
                }
                path = path.substring(0, indexOfFirstQueryChar);
            }
        }
        return buildUri(uri.getScheme(), uri.getHost(), uri.getPort(),
                buildUriPath(uri.getPath(), path), query);
    }

    public static URI buildUri(String host, int port, String path, String query) {
        return buildUri(HTTP_SCHEME, host, port, path, query);
    }

    public static URI buildUri(ServiceHost host, String path) {
        return buildUri(host, path, null);
    }

    public static URI buildUri(ServiceHost host, String path, String query, String userInfo) {
        URI base = host.getUri();
        return UriUtils.buildUri(base.getScheme(), base.getHost(), base.getPort(), path, query,
                userInfo);
    }

    public static URI buildUri(ServiceHost host, String path, String query) {
        return buildUri(host, path, query, null);
    }

    /**
     * Builds a fully qualified URI. Attempts no normalization, assumes well formed path and query
     */
    public static URI buildServiceUri(String scheme, String host, int port, String path,
            String query,
            String userInfo) {
        try {
            return new URI(scheme, userInfo, host, port, path, query, null);
        } catch (URISyntaxException e) {
            Utils.log(UriUtils.class, Utils.class.getSimpleName(), Level.SEVERE, "%s",
                    Utils.toString(e));
            return null;
        }
    }

    public static URI buildUri(String scheme, String host, int port, String path, String query,
            String userInfo) {
        try {
            if (path != null) {
                final int indexOfFirstQueryChar = path.indexOf(URI_QUERY_CHAR_CONST);
                if (indexOfFirstQueryChar >= 0) {
                    if (indexOfFirstQueryChar < path.length() - 1) {
                        query = path.substring(indexOfFirstQueryChar + 1);
                    }
                    path = path.substring(0, indexOfFirstQueryChar);
                }
            }
            return new URI(scheme, userInfo, host, port, normalizeUriPath(path), query, null)
                    .normalize();
        } catch (URISyntaxException e) {
            Utils.log(UriUtils.class, Utils.class.getSimpleName(), Level.SEVERE, "%s",
                    Utils.toString(e));
            return null;
        }
    }

    public static URI buildUri(String scheme, String host, int port, String path, String query) {
        return buildUri(scheme, host, port, path, query, null);
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
        }
        return URI_PATH_CHAR + path;
    }

    private static void normalizeUriPathTo(String path, StringBuilder sb) {
        if (path == null || path.isEmpty()) {
            return;
        }

        if (path.charAt(0) != URI_PATH_CHAR_CONST) {
            sb.append(URI_PATH_CHAR_CONST);
        }

        sb.append(path);

        int lastIndex = sb.length() - 1;
        if (sb.charAt(lastIndex) == URI_PATH_CHAR_CONST) {
            // Trim trailing slash
            sb.setLength(lastIndex);
        }
    }

    public static String buildUriPath(String... segments) {
        if (segments.length == 1) {
            return normalizeUriPath(segments[0]);
        }
        StringBuilder sb = new StringBuilder();
        for (String s : segments) {
            normalizeUriPathTo(s, sb);
        }
        return sb.toString();
    }

    public static String buildUriQuery(String... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "keyValues array length must be even, with key and value pairs interleaved");
        }
        StringBuilder sb = new StringBuilder();

        boolean doKey = true;
        boolean isFirst = true;
        for (String s : keyValues) {
            if (doKey) {
                if (!isFirst) {
                    sb.append(URI_QUERY_PARAM_LINK_CHAR_CONST);
                } else {
                    isFirst = false;
                }
                sb.append(s).append(URI_QUERY_PARAM_KV_CHAR_CONST);
            } else {
                sb.append(s);
            }
            doKey = !doKey;
        }
        return sb.toString();
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
        if (path == null || path.length == 0) {
            return baseUri;
        }
        String query = null;
        StringBuilder buildPath = null;
        for (String p : path) {
            if (p == null) {
                continue;
            }
            final int indexOfFirstQueryChar = p.indexOf(URI_QUERY_CHAR_CONST);
            if (indexOfFirstQueryChar >= 0) {
                if (indexOfFirstQueryChar < p.length() - 1) {
                    final String curQuery = p.substring(indexOfFirstQueryChar + 1);
                    if (query == null) {
                        query = curQuery;
                    } else {
                        query += curQuery;
                    }
                }
                p = p.substring(0, indexOfFirstQueryChar);
            }
            if (buildPath == null) {
                buildPath = new StringBuilder();
            }
            normalizeUriPathTo(p, buildPath);
        }

        try {
            return new URI(baseUri.getScheme(), baseUri.getUserInfo(), baseUri.getHost(),
                    baseUri.getPort(), buildPath == null ? null : buildPath.toString(), query, null)
                            .normalize();
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
        String query = u.getQuery();
        if (query != null && !query.isEmpty()) {
            query += URI_QUERY_PARAM_LINK_CHAR_CONST + buildUriQuery(keyValues);
        } else {
            query = buildUriQuery(keyValues);
        }

        try {
            return new URI(u.getScheme(), null, u.getHost(),
                    u.getPort(), u.getPath(), query, null);
        } catch (URISyntaxException e) {
            Utils.log(UriUtils.class, Utils.class.getSimpleName(), Level.SEVERE, "%s",
                    Utils.toString(e));
        }
        return null;
    }

    public static URI buildDocumentQueryUri(ServiceHost host,
            String selfLink,
            boolean doExpand,
            boolean includeDeleted,
            EnumSet<ServiceOption> serviceCaps) {
        ServiceOption queryCap = ServiceOption.NONE;
        if (serviceCaps.contains(ServiceOption.IMMUTABLE)) {
            queryCap = ServiceOption.IMMUTABLE;
        } else if (serviceCaps.contains(ServiceOption.PERSISTENCE)) {
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

    public static URI buildDefaultDocumentQueryUri(URI hostUri,
            String selfLink,
            boolean doExpand,
            boolean includeDeleted,
            ServiceOption cap) {

        URI indexUri = UriUtils.buildUri(hostUri, ServiceUriPaths.CORE_DOCUMENT_INDEX);
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

        return extendUriWithQuery(indexURI, queryArgs.toArray(new String[queryArgs.size()]));
    }

    public static URI appendQueryParam(URI uri, String param, String value) {
        return extendUriWithQuery(uri, param, value);
    }

    public static Map<String, String> parseUriQueryParams(URI uri) {
        String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return new HashMap<>();
        }

        String[] keyValuePairs = query.split(URI_QUERY_PARAM_LINK_CHAR);
        Map<String, String> params = new HashMap<>(keyValuePairs.length);
        for (String kvPair : keyValuePairs) {
            String key;
            String value;
            int i = kvPair.indexOf(URI_QUERY_PARAM_KV_CHAR_CONST);
            if (i != -1) {
                key = kvPair.substring(0, i);
                value = kvPair.substring(i + 1);
            } else {
                key = kvPair;
                value = "";
            }

            if (key.isEmpty()) {
                continue;
            }

            params.put(key, value);
        }
        return params;
    }

    /**
     * Utility method to parse path parameters from a supplied template and URI
     * To be used with services with option {@link ServiceOption#URI_NAMESPACE_OWNER}
     *
     * template - /example-service/{name}/keyValues/{myKey}
     * uri - http://localhost:8000/example-service/sample/keyValues/key1
     * response - name -> sample, myKey -> key1
     * Note: If the uri passed deviates from the template, the method returns
     *       with params parsed so far.
     */
    public static Map<String, String> parseUriPathSegments(URI uri, String templatePath) {
        Map<String, String> params = new HashMap<>();
        String path = uri.getPath();
        if (path == null || path.isEmpty()) {
            return params;
        }

        String[] pathSplit = path.split(URI_PATH_CHAR);
        String[] templatePathSplit = templatePath.split(URI_PATH_CHAR);

        for (int index = 0; index < templatePathSplit.length && index < pathSplit.length; index++) {
            String templateStr = templatePathSplit[index];
            Matcher matcher = pathParamPattern.matcher(templateStr);
            if (matcher.matches()) {
                String pathParam = templateStr.subSequence(1, templateStr.length() - 1).toString();
                params.put(pathParam, pathSplit[index]);
            } else if (!templatePathSplit[index].equals(pathSplit[index])) {
                break;
            }
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
        return host.isHostEqual(remoteService);
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
        if (key == null) {
            key = targetService.getPath();
        }
        URI u = UriUtils.buildUri(targetService, UriUtils.buildUriPath(
                selectorPath,
                ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING));

        return UriUtils.extendUriWithQuery(u, FORWARDING_URI_PARAM_NAME_PATH,
                key,
                FORWARDING_URI_PARAM_NAME_TARGET,
                ForwardingTarget.KEY_HASH.toString());
    }

    public static URI buildForwardToPeerUri(URI targetService, String peerId, String selectorPath,
            EnumSet<ServiceOption> caps) {
        URI u = UriUtils.buildUri(targetService, UriUtils.buildUriPath(
                selectorPath,
                ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING));
        String query = targetService.getQuery();
        if (query == null) {
            query = "";
        }

        return UriUtils.extendUriWithQuery(u, FORWARDING_URI_PARAM_NAME_PEER,
                peerId,
                FORWARDING_URI_PARAM_NAME_PATH,
                targetService.getPath(),
                FORWARDING_URI_PARAM_NAME_QUERY,
                query,
                FORWARDING_URI_PARAM_NAME_TARGET,
                ForwardingTarget.PEER_ID.toString());
    }

    /**
     * Broadcasts the request to the service on all nodes associated with the node selector group.
     * If the node selector is using limited replication, use the broadcast method that requires a
     * selection key, instead of this one.
     */
    public static URI buildBroadcastRequestUri(URI targetService, String selectorPath) {
        URI u = UriUtils.buildUri(targetService, UriUtils.buildUriPath(selectorPath,
                ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING));
        return UriUtils.extendUriWithQuery(u, FORWARDING_URI_PARAM_NAME_PATH,
                targetService.getPath(),
                FORWARDING_URI_PARAM_NAME_TARGET,
                ForwardingTarget.ALL.toString());
    }

    /**
     * Broadcasts the request to the service on all nodes associated with the node selector group,
     * using the selection key to pick the nodes. This is applicable for node selectors with limited
     * replication.
     */
    public static URI buildBroadcastRequestUri(URI targetService,
            String selectorPath, String selectionKey) {
        URI u = UriUtils.buildUri(targetService, UriUtils.buildUriPath(selectorPath,
                ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING));
        return UriUtils.extendUriWithQuery(u,
                FORWARDING_URI_PARAM_NAME_KEY,
                selectionKey,
                FORWARDING_URI_PARAM_NAME_PATH,
                targetService.getPath(),
                FORWARDING_URI_PARAM_NAME_TARGET,
                ForwardingTarget.ALL.toString());
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

    public static String getPathParamValue(URI uri) {
        return getODataParamValueAsString(uri, FORWARDING_URI_PARAM_NAME_PATH);
    }

    public static String getPeerParamValue(URI uri) {
        return getODataParamValueAsString(uri, FORWARDING_URI_PARAM_NAME_PEER);
    }

    public static boolean hasODataQueryParams(URI uri) {
        if (uri.getQuery() == null) {
            return false;
        }
        String q = uri.getQuery();
        return q.contains(URI_PARAM_ODATA_TOP)
                || q.contains(URI_PARAM_ODATA_COUNT)
                || q.contains(URI_PARAM_ODATA_LIMIT)
                || q.contains(URI_PARAM_ODATA_SKIP)
                || q.contains(URI_PARAM_ODATA_FILTER);
    }

    public static Integer getODataSkipParamValue(URI uri) {
        return getODataParamValue(uri, URI_PARAM_ODATA_SKIP);
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

    public static Integer getODataTopParamValue(URI uri) {
        return getODataParamValue(uri, URI_PARAM_ODATA_TOP);
    }

    public static boolean getODataCountParamValue(URI uri) {
        String paramValue = getODataParamValueAsString(uri, URI_PARAM_ODATA_COUNT);
        return Boolean.parseBoolean(paramValue);
    }

    public static Integer getODataLimitParamValue(URI uri) {
        return getODataParamValue(uri, URI_PARAM_ODATA_LIMIT);
    }

    public static String extendQueryPageLinkWithQuery(String pageLink, String queryKeyValues) {
        final String querySegmentPrefix = UriUtils.URI_QUERY_PARAM_LINK_CHAR
                + UriUtils.FORWARDING_URI_PARAM_NAME_QUERY
                + UriUtils.URI_QUERY_PARAM_KV_CHAR;
        int querySegmentIndex = pageLink.indexOf(querySegmentPrefix);
        if (queryKeyValues == null || queryKeyValues.isEmpty()) {
            throw new IllegalArgumentException("query is required");
        }

        if (!pageLink.contains(ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING)
                || !pageLink.contains(UriUtils.FORWARDING_URI_PARAM_NAME_TARGET)) {
            // this is not a multiple node capable page link
            return pageLink + UriUtils.URI_QUERY_CHAR + queryKeyValues;
        }

        // assume the link looks like so:
        // /core/node-selectors/default/forwarding?peer=host-1&path=/core/query-page/1469733619696001&query=&target=PEER_ID
        // Notice that the &query= segment might, or might not have a query specified. We splice
        // the string and insert the supplied query right after the "="
        int charCountToEqualsSign = UriUtils.FORWARDING_URI_PARAM_NAME_QUERY.length() + 2;
        querySegmentIndex += charCountToEqualsSign;
        StringBuilder sb = new StringBuilder(pageLink.length());
        sb.append(pageLink.substring(0, querySegmentIndex));
        sb.append(queryKeyValues);
        sb.append(pageLink.substring(querySegmentIndex));
        return sb.toString();
    }

    public static String getODataSkipToParamValue(URI uri) {
        return getODataParamValueAsString(uri, URI_PARAM_ODATA_SKIP_TO);
    }

    public static String getODataNodeParamValue(URI uri) {
        return getODataParamValueAsString(uri, URI_PARAM_ODATA_NODE);
    }

    public static String getODataTenantLinksParamValue(URI uri) {
        return getODataParamValueAsString(uri, URI_PARAM_ODATA_TENANTLINKS);
    }

    public static Integer getODataParamValue(final URI uri, final String uriParamOdataType) {
        String paramValue = getODataParamValueAsString(uri, uriParamOdataType);
        return paramValue != null ? Integer.valueOf(paramValue) : null;
    }

    public static String getODataParamValueAsString(final URI uri, final String uriParamOdataType) {
        String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }

        if (!query.contains(uriParamOdataType)) {
            return null;
        }

        Map<String, String> queryParams = parseUriQueryParams(uri);

        String paramValue = queryParams.get(uriParamOdataType);
        if (paramValue == null || paramValue.isEmpty()) {
            return null;
        }
        return paramValue;
    }

    public enum ODataOrder {
        ASC, DESC
    }

    public static class ODataOrderByTuple {
        public ODataOrder order;
        public String propertyName;
        public String propertyType;
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

        String orderByType = queryParams.get(URI_PARAM_ODATA_ORDER_BY_TYPE);
        if (orderByType != null) {
            tuple.propertyType = orderByType.trim();
        }
        return tuple;
    }

    /**
     * Infrastructure use only.
     *
     * Converts path characters '/' into '-'. Returns null if malformed paths.
     *
     * Examples:
     *     /core/examples/ -> core-examples
     *     /core/local-query-tasks -> core-local-query-tasks
     *     /core/examples?expand&count=1 -> core-examples
     */
    public static String convertPathCharsFromLink(String path) {
        try {
            String uriPath = (new URI(path)).getPath();
            return trimPathSlashes(uriPath).replace(URI_PATH_CHAR_CONST, '-');
        } catch (URISyntaxException ex) {
            return null;
        }
    }

    public static String trimPathSlashes(String path) {
        // removes beginning and trailing slashes from a URI path.
        return TRIM_PATH_SLASHES_PATTERN.matcher(path).replaceAll("");
    }

    public static boolean hasODataExpandParamValue(URI uri) {
        String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return false;
        }

        return query.contains(UriUtils.URI_PARAM_ODATA_EXPAND)
                || query.contains(UriUtils.URI_PARAM_ODATA_EXPAND_NO_DOLLAR_SIGN);
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

    /**
     * Requests a random server socket port to be created, closes it, and returns the port picked
     * as a potentially available port. Note, that this is not an atomic probe and acquire, so the port
     * might be taken by the time a bind occurs.
     */
    public static int findAvailablePort() {
        int port = 0;
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
            Logger.getAnonymousLogger().info("port candidate:" + port);
        } catch (Throwable e) {
            Logger.getAnonymousLogger().severe(e.toString());
        } finally {
            try {
                if (socket != null) {
                    socket.close();
                }
            } catch (IOException e) {
            }
        }
        return port;
    }
}
