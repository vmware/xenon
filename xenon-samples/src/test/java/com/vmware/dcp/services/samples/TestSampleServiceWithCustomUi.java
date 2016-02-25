/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.dcp.services.samples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.samples.SampleFactoryServiceWithCustomUi;
import com.vmware.xenon.services.samples.SampleServiceWithCustomUi.SampleServiceWithCustomUiState;

public class TestSampleServiceWithCustomUi extends BasicReusableHostTestCase {

    private static final String THE_SERVICE_URI = SampleFactoryServiceWithCustomUi.SELF_LINK + "/name";

    @Before
    public void prepare() throws Throwable {
        URI factoryUri = UriUtils.buildUri(this.host, SampleFactoryServiceWithCustomUi.class);
        this.host.startService(
                Operation.createPost(factoryUri),
                new SampleFactoryServiceWithCustomUi());

        createService();
    }

    private void createService() throws Throwable {
        URI factoryUri = UriUtils.buildUri(this.host, SampleFactoryServiceWithCustomUi.class);

        SampleServiceWithCustomUiState initialState = new SampleServiceWithCustomUiState();
        initialState.name = THE_SERVICE_URI;
        initialState.documentSelfLink = THE_SERVICE_URI;

        this.host.testStart(1);

        Operation createPost = Operation
                .createPost(factoryUri)
                .setBody(initialState).setCompletion((o, e) -> {
                    this.host.completeIteration();
                });

        this.host.send(createPost);
        this.host.testWait();
    }

    @Test
    public void testGetUi() throws Throwable {
        Operation op = Operation
                .createGet(UriUtils.buildUri(this.host, THE_SERVICE_URI + "/ui"))
                .setCompletion(getSafeHandler((o, e) -> {
                    assertNull(e);
                    assertEquals("Did not receive temporary redirect", Operation.STATUS_CODE_MOVED_TEMP, o.getStatusCode());
                    assertTrue("Redirected url does not end with /", o.getResponseHeader("location").endsWith("/"));
                }));

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();
    }

    @Test
    public void testGetUiWithSlash() throws Throwable {
        Operation op = Operation
                .createGet(URI.create(UriUtils.buildUri(this.host, THE_SERVICE_URI + "/ui") + "/"))
                .setCompletion(getSafeHandler((o, e) -> {
                    assertNull(e);
                    assertEquals("Expected 200 OK", Operation.STATUS_CODE_OK, o.getStatusCode());
                    assertTrue("Expected content of the index.html", o.getBody(String.class).contains("DOCTYPE"));
                }));

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();
    }

    @Test
    public void testGetResource() throws Throwable {
        Operation op = Operation
                .createGet(URI.create(UriUtils.buildUri(this.host, THE_SERVICE_URI + "/ui/README.txt") + "/"))
                .setCompletion(getSafeHandler((o, e) -> {
                    assertNull(e);
                    assertEquals("Expected 200 OK",Operation.STATUS_CODE_OK, o.getStatusCode());
                    assertTrue("Expected the contents of the README.txt", o.getBody(String.class).contains("unit-test"));
                }));

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();
    }
}
