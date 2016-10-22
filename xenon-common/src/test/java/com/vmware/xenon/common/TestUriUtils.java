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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestUriUtils {

    @Test
    public void normalizePath() throws Throwable {
        String nr = UriUtils.normalizeUriPath("/foo/");
        assertEquals("/foo", nr);
        nr = UriUtils.normalizeUriPath("/foo");
        assertEquals("/foo", nr);
        nr = UriUtils.normalizeUriPath("");
        assertEquals("", nr);
    }

    @Test
    public void updateUriPort() {
        int port = 8000;
        int newPort = 10000;
        URI original = UriUtils.buildUri("http://localhost:8000/somePath?someQuery");

        assertEquals(null, UriUtils.updateUriPort(null, 0));
        assertEquals(original.getPort(), UriUtils.updateUriPort(original, port).getPort());
        URI newUri = UriUtils.updateUriPort(original, newPort);
        assertEquals(newPort, newUri.getPort());
        assertEquals(original.getHost(), newUri.getHost());
        assertEquals(original.getScheme(), newUri.getScheme());
        assertEquals(original.getPath(), newUri.getPath());
        assertEquals(original.getQuery(), newUri.getQuery());
    }

    @Test
    public void extendQueryPageLinkWithQuery() throws Throwable {
        final String basicPageLink = "/core/query-page/1469733619696001";
        URI u = new URI("http://localhost:8000" + basicPageLink);
        URI forwarderUri = UriUtils.buildForwardToPeerUri(u, UUID.randomUUID().toString(),
                ServiceUriPaths.DEFAULT_NODE_SELECTOR, EnumSet.noneOf(ServiceOption.class));
        String pageLink = forwarderUri.getPath() + UriUtils.URI_QUERY_CHAR
                + forwarderUri.getQuery();
        String query = UriUtils.buildUriQuery(UriUtils.URI_PARAM_ODATA_LIMIT, "5");

        String basicLinkWithQuery = UriUtils.extendQueryPageLinkWithQuery(basicPageLink, query);
        assertEquals(basicPageLink + "?" + query, basicLinkWithQuery);

        String pageLinkWithQuery = UriUtils.extendQueryPageLinkWithQuery(pageLink, query);
        assertTrue(
                pageLinkWithQuery.contains(UriUtils.FORWARDING_URI_PARAM_NAME_QUERY + "=" + query));
    }

    @Test
    public void extendUriWithQuery() throws URISyntaxException {
        String query = "key1=value1&key2=value2";
        String basePath = UriUtils.buildUriPath("some", UUID.randomUUID().toString());
        URI baseUri = new URI("http://localhost:8000");
        URI baseHttpsUri = new URI("https://localhost:8000");
        URI httpBase = UriUtils.buildUri(baseUri, basePath);
        URI httpsBase = UriUtils.buildUri(baseHttpsUri, basePath);

        URI u = UriUtils.extendUriWithQuery(httpBase, "key1", "value1", "key2", "value2");
        assertEquals("http", u.getScheme());
        assertEquals(basePath, u.getPath());
        assertEquals(u.getQuery(), query);

        u = UriUtils.extendUriWithQuery(httpsBase, "key1", "value1", "key2", "value2");
        assertEquals("https", u.getScheme());
        assertEquals(basePath, u.getPath());
        assertEquals(8000, u.getPort());
        assertEquals(u.getQuery(), query);
    }

    @Test
    public void buildUri() throws URISyntaxException {
        URI baseUri = new URI("http://localhost:8000/something");
        URI baseHttpsUri = new URI("https://localhost:8000/something");

        String pathSegment = UUID.randomUUID().toString();
        String pathSegment2 = UUID.randomUUID().toString();
        URI u = UriUtils.buildUri(baseUri, pathSegment);
        assertEquals("http", u.getScheme());
        assertEquals("/" + pathSegment, u.getPath());
        assertTrue(u.getQuery() == null);

        u = UriUtils.buildUri(baseUri, pathSegment, pathSegment2);
        assertEquals("http", u.getScheme());
        assertEquals("/" + pathSegment + "/" + pathSegment2, u.getPath());
        assertTrue(u.getQuery() == null);

        u = UriUtils.buildUri(baseHttpsUri, pathSegment);
        assertEquals("https", u.getScheme());
        assertEquals("/" + pathSegment, u.getPath());
        assertTrue(u.getQuery() == null);
        assertEquals(8000, u.getPort());

        String query = "key=value&key2=value2";
        String pathWithQuery = pathSegment + "?" + query;
        u = UriUtils.buildUri(baseUri, pathWithQuery, pathSegment2);
        assertEquals("/" + pathSegment + "/" + pathSegment2, u.getPath());
        assertEquals(query, u.getQuery());

        pathWithQuery = pathSegment + "?" + query;
        String pathWithQuery2 = pathSegment2 + "?" + query;
        String combinedQuery = query + query;
        u = UriUtils.buildUri(baseUri, pathWithQuery, pathWithQuery2);
        assertEquals("/" + pathSegment + "/" + pathSegment2, u.getPath());
        assertEquals(combinedQuery, u.getQuery());

        final String queryContainingQueryChar = "key=value&url=/url/with?query=true";
        String pathWithMutlipleQueryChars = pathSegment + "?" + queryContainingQueryChar;
        u = UriUtils.buildUri(baseUri, pathWithMutlipleQueryChars);
        assertEquals("/" + pathSegment, u.getPath());
        assertEquals(queryContainingQueryChar, u.getQuery());

        String pathEndingWithQueryChar = pathSegment + "?";
        u = UriUtils.buildUri(baseUri, pathEndingWithQueryChar);
        assertEquals("/" + pathSegment, u.getPath());
        assertEquals(null, u.getQuery());

        String queryContainingOnlyQueryChars = "??";
        String pathEndingWithQueryChars = pathSegment + "?" + queryContainingOnlyQueryChars;
        u = UriUtils.buildUri(baseUri, pathEndingWithQueryChars);
        assertEquals("/" + pathSegment, u.getPath());
        assertEquals(queryContainingOnlyQueryChars, u.getQuery());

        URI abNormalUri = new URI("http://localhost:8000/foo/./../bar");
        u = UriUtils.buildUri(abNormalUri, abNormalUri.getPath());
        assertEquals("/bar", u.getPath());

        abNormalUri = new URI("http://localhost:8000/foo/./bar");
        u = UriUtils.buildUri(abNormalUri, abNormalUri.getPath());
        assertEquals("/foo/bar", u.getPath());
    }

    @Test
    public void buildUriFromParts() throws URISyntaxException {
        String scheme = "http";
        String host = "host";
        int port = 8080;
        String path = "path/to/somewhere";
        String query = "key1=value1&key2=value2";

        URI u = UriUtils.buildUri(scheme, host, port, path, query);

        assertEquals(scheme, u.getScheme());
        assertEquals(host, u.getHost());
        assertEquals(port, u.getPort());
        assertEquals("/" + path, u.getPath());
        assertEquals(query, u.getQuery());

        String pathEndingWithQueryChar = path + "?";
        u = UriUtils.buildUri(scheme, host, port, pathEndingWithQueryChar, query);

        assertEquals(scheme, u.getScheme());
        assertEquals(host, u.getHost());
        assertEquals(port, u.getPort());
        assertEquals("/" + path, u.getPath());
        assertEquals(query, u.getQuery());



        String queryContainingQueryChar = "key=value&url=/url/with?query=true";
        u = UriUtils.buildUri(scheme, host, port, pathEndingWithQueryChar, queryContainingQueryChar);

        assertEquals(scheme, u.getScheme());
        assertEquals(host, u.getHost());
        assertEquals(port, u.getPort());
        assertEquals("/" + path, u.getPath());
        assertEquals(queryContainingQueryChar, u.getQuery());
    }

    @Test
    public void extendUri() throws URISyntaxException {
        String basePath = UriUtils.buildUriPath("some", UUID.randomUUID().toString());
        URI baseUri = new URI("http://localhost:8000");
        URI baseHttpsUri = new URI("https://localhost:8000");
        URI httpBase = UriUtils.buildUri(baseUri, basePath);
        URI httpsBase = UriUtils.buildUri(baseHttpsUri, basePath);

        String path = UUID.randomUUID().toString();
        URI u = UriUtils.extendUri(httpBase, path);
        assertEquals("http", u.getScheme());
        assertEquals(basePath + "/" + path, u.getPath());
        assertTrue(u.getQuery() == null);

        u = UriUtils.extendUri(httpsBase, path);
        assertEquals("https", u.getScheme());
        assertEquals(basePath + "/" + path, u.getPath());
        assertTrue(u.getQuery() == null);
        assertEquals(8000, u.getPort());

        String query = "key=value&key2=value2";
        String pathWithQuery = path + "?" + query;
        u = UriUtils.extendUri(httpBase, pathWithQuery);
        assertEquals(basePath + "/" + path, u.getPath());
        assertEquals(query, u.getQuery());

        String queryContainingQueryChar = "key=value&url=/url/with?query=true";
        String pathWithQueryChars = path + "?" + queryContainingQueryChar;
        u = UriUtils.extendUri(httpBase, pathWithQueryChars);
        assertEquals(basePath + "/" + path, u.getPath());
        assertEquals(queryContainingQueryChar, u.getQuery());

        String pathEndingWithQueryChar = path + "?";
        u = UriUtils.extendUri(httpBase, pathEndingWithQueryChar);
        assertEquals(basePath + "/" + path, u.getPath());
        assertEquals(null, u.getQuery());

        String queryContainingOnlyQueryChars = "??";
        String pathEndingWithQueryChars = path + "?" + queryContainingOnlyQueryChars;
        u = UriUtils.extendUri(httpBase, pathEndingWithQueryChars);
        assertEquals(basePath + "/" + path, u.getPath());
        assertEquals(queryContainingOnlyQueryChars, u.getQuery());

        u = UriUtils.extendUri(httpBase, "/bar/./../foo");
        assertEquals(httpBase.getPath() + "/foo", u.getPath());

        u = UriUtils.extendUri(httpBase, "/bar/./foo");
        assertEquals(httpBase.getPath() + "/bar/foo", u.getPath());
    }

    @Test
    public void getParentPath() {
        assertEquals("/a/b", UriUtils.getParentPath("/a/b/c"));
        assertEquals("/a", UriUtils.getParentPath("/a/b"));
        assertEquals("/", UriUtils.getParentPath("/a"));
        assertEquals(null, UriUtils.getParentPath("/"));
    }

    @Test
    public void buildUriFromString() throws Throwable {
        assertEquals(null, UriUtils.buildUri("http://finance.yahoo.com/q/h?s=^IXIC"));
        assertEquals(new URI(""), UriUtils.buildUri(""));
        assertEquals(new URI("http://example.com/example"),
                UriUtils.buildUri("http://example.com/example"));
        assertEquals(new URI("/one/two/three"), UriUtils.buildUri("/one/two/three"));
    }

    @Test
    public void testConvertUriPathCharsFromLink() {
        assertEquals("core-examples",
                UriUtils.convertPathCharsFromLink("///core/examples///"));
        assertEquals("core-examples",
                UriUtils.convertPathCharsFromLink("///core/examples?expand&count=100"));
        assertEquals("core-local-query-tasks",
                UriUtils.convertPathCharsFromLink("///core/local-query-tasks"));
        assertEquals(null,
                UriUtils.convertPathCharsFromLink("@*#(@&@#"));
    }

    @Test
    public void testTrimPathSlashes() {
        assertEquals("a", UriUtils.trimPathSlashes("//a//"));
        assertEquals("a", UriUtils.trimPathSlashes("///a"));
        assertEquals("a", UriUtils.trimPathSlashes("a///"));
        assertEquals("a", UriUtils.trimPathSlashes("/a/"));
        assertEquals("a", UriUtils.trimPathSlashes("a"));
        assertEquals("a/b", UriUtils.trimPathSlashes("/a/b/"));
        assertEquals("a/b", UriUtils.trimPathSlashes("////a/b/////"));
        assertEquals("a/b", UriUtils.trimPathSlashes("////a/b"));
        assertEquals("a/b", UriUtils.trimPathSlashes("a/b/////"));
        assertEquals("a/b", UriUtils.trimPathSlashes("a/b"));
    }

    @Test
    public void isChildPath() {
        assertFalse(UriUtils.isChildPath(null, "/a/b"));
        assertFalse(UriUtils.isChildPath("/a/b/c", null));
        assertTrue(UriUtils.isChildPath("/a/b/c", "/a/b"));
        assertTrue(UriUtils.isChildPath("/a/b/c", "/a/b/"));
        assertFalse(UriUtils.isChildPath("/a/bb/c", "/a/b"));
        assertTrue(UriUtils.isChildPath("b/c", "b"));
        assertFalse(UriUtils.isChildPath("b/c", "/a/b"));
    }

    @Test
    public void getLastPathSegment() {
        assertEquals("example", UriUtils.getLastPathSegment(UriUtils.buildUri("http://example.com/example")));
        assertEquals("example", UriUtils.getLastPathSegment(UriUtils.buildUri("http://example.com:8000/example?one=1")));
        assertEquals("", UriUtils.getLastPathSegment(UriUtils.buildUri("http://example.com:8000")));
        assertEquals("", UriUtils.getLastPathSegment(UriUtils.buildUri("http://example.com:8000/example/")));
    }

    @Test
    public void hasODataExpandParamValue() {
        URI u = UriUtils.buildUri("http://example.com:8000?$expand=documentLinks");
        assertTrue(UriUtils.hasODataExpandParamValue(u));
        u = UriUtils.buildUri("http://example.com:8000?expand=documentLinks");
        assertTrue(UriUtils.hasODataExpandParamValue(u));
        u = UriUtils.buildUri("http://example.com:8000?$blablabla=documentLinks");
        assertFalse(UriUtils.hasODataExpandParamValue(u));
        u = UriUtils.buildUri("http://example.com:8000");
        assertFalse(UriUtils.hasODataExpandParamValue(u));
        u = UriUtils.buildUri("http://example.com:8000?");
        assertFalse(UriUtils.hasODataExpandParamValue(u));
    }

    @Test
    public void testSimplePathParamParsing() {
        String template = "/vmware/offices/{location}/sites/{site}";
        URI u = UriUtils.buildUri("http://example.com:8000/vmware/offices/pa/sites/prom-e");
        Map<String, String> pathParams = UriUtils.parseUriPathSegments(u, template);
        assertEquals(2, pathParams.size());
        assertTrue(pathParams.keySet().contains("location"));
        assertTrue(pathParams.keySet().contains("site"));
        assertTrue(pathParams.get("location").equals("pa"));
        assertTrue(pathParams.get("site").equals("prom-e"));
    }
}
