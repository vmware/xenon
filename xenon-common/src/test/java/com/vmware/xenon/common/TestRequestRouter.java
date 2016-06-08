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
import static org.junit.Assert.fail;

import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.Service.Action;

class RequestBody {
    public enum Kind {
        X, Y
    }

    public Kind kind;
}

public class TestRequestRouter {

    private RequestRouter router;
    private int xCount;
    private int yCount;
    private int zCount;


    @Before
    public void setUp() throws Exception {
        this.router = new RequestRouter();
        this.xCount = 0;
        this.yCount = 0;
        this.zCount = 0;
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testUriMatcher() throws Exception {
        int NUM = 10;

        this.router.register(Action.PATCH, new RequestRouter.RequestUriMatcher("action=doX"),
                this::doX, "perform X");
        this.router.register(Action.PATCH, new RequestRouter.RequestUriMatcher("action=doY"),
                this::doY, "perform Y");

        for (int i = 0; i < NUM; i++) {
            String uri = i % 2 == 0 ? "http://localhost/?action=doX"
                    : "http://localhost/?action=doY";
            if (this.router.test(Operation.createPatch(new URI(uri)))) {
                fail("route not found");
            }
        }

        assertEquals(NUM / 2, this.xCount);
        assertEquals(NUM / 2, this.yCount);
    }

    @Test
    public void testBodyMatcher() throws Exception {
        int NUM = 9;

        this.router.register(Action.PATCH,
                new RequestRouter.RequestBodyMatcher<RequestBody>(RequestBody.class, "kind", RequestBody.Kind.X),
                this::doX, "perform X");
        this.router.register(Action.PATCH,
                new RequestRouter.RequestBodyMatcher<RequestBody>(RequestBody.class, "kind", RequestBody.Kind.Y),
                this::doY, "perform Y");

        for (int i = 0; i < NUM; i++) {
            RequestBody body = new RequestBody();
            switch (i % 3) {
            case 0:
                body.kind = RequestBody.Kind.X;
                break;
            case 1:
                body.kind = RequestBody.Kind.Y;
                break;
            default:
                break;
            }

            if (this.router.test(Operation.createPatch(new URI("http://localhost/")).setBody(body))) {
                this.zCount++;
            }
        }

        assertEquals(NUM / 3, this.xCount);
        assertEquals(NUM / 3, this.yCount);
        assertEquals(NUM / 3, this.zCount);
    }

    private void doX(Operation op) {
        this.xCount++;
    }

    private void doY(Operation op) {
        this.yCount++;
    }
}
