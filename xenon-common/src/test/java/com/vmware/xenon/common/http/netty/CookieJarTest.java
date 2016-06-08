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

package com.vmware.xenon.common.http.netty;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import org.junit.Before;
import org.junit.Test;

public class CookieJarTest {
    CookieJar cookieJar;

    @Before
    public void createCookieJar() {
        this.cookieJar = new CookieJar();
    }

    @Test
    public void cookieByOrigin() throws URISyntaxException {
        URI uri = URI.create("http://domain.com/path");
        Cookie cookie = new DefaultCookie("key", "value");
        this.cookieJar.add(uri, cookie);

        uri = URI.create("http://domain.com/path");
        Map<String, String> cookies = this.cookieJar.list(uri);
        assertEquals(1, cookies.size());
        assertEquals("value", cookies.get("key"));
    }

    @Test
    public void cookieByOriginDifferentPath() throws URISyntaxException {
        URI uri = URI.create("http://domain.com/path");
        Cookie cookie = new DefaultCookie("key", "value");
        this.cookieJar.add(uri, cookie);

        uri = URI.create("http://domain.com/otherpath");
        Map<String, String> cookies = this.cookieJar.list(uri);
        assertEquals(1, cookies.size());
        assertEquals("value", cookies.get("key"));
    }

    @Test
    public void cookieMarkedAsSecure() throws URISyntaxException {
        URI uri = URI.create("https://domain.com/path");
        Cookie cookie = new DefaultCookie("key", "value");
        cookie.setSecure(true);
        this.cookieJar.add(uri, cookie);

        uri = URI.create("http://domain.com/otherpath");
        assertEquals(0, this.cookieJar.list(uri).size());

        uri = URI.create("https://domain.com/otherpath");
        assertEquals(1, this.cookieJar.list(uri).size());
    }

    @Test
    public void testCookieDecode() {
        String cookieString = "key1=val1; key2=val2";
        Map<String, String> cookies = CookieJar.decodeCookies(cookieString);
        assertEquals(cookies.get("key1"), "val1");
        assertEquals(cookies.get("key2"), "val2");
        cookieString = "foo=bar";
        cookies = CookieJar.decodeCookies(cookieString);
        assertEquals(cookies.get("foo"), "bar");
        cookieString = "";
        cookies = CookieJar.decodeCookies(cookieString);
        assertEquals(cookies.size(), 0);
        cookieString = ";";
        cookies = CookieJar.decodeCookies(cookieString);
        assertEquals(cookies.size(), 0);
        cookieString = "foo=;foo=bar";
        cookies = CookieJar.decodeCookies(cookieString);
        assertEquals(cookies.size(), 1);
        assertEquals(cookies.get("foo"), "bar");

    }
}
