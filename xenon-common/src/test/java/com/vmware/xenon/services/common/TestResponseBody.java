/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;

public class TestResponseBody extends BasicReusableHostTestCase {

    @Test
    public void localCall() throws Throwable {
        String body = "whatever";

        Operation op = Operation
                .createPost(this.host, ExampleService.FACTORY_LINK)
                .setBody(body);

        TestRequestSender sender = this.host.getTestRequestSender();
        TestRequestSender.FailureResponse failure = sender.sendAndWaitFailure(op);

        assertNotNull(failure.failure);
        assertEquals(Operation.MEDIA_TYPE_APPLICATION_JSON, failure.op.getContentType());
        assertEquals(Operation.STATUS_CODE_BAD_REQUEST, failure.op.getStatusCode());

        Object bodyRaw = failure.op.getBodyRaw();

        assertTrue(bodyRaw instanceof ServiceErrorResponse);

        String bodyRawString = Utils.toJson(bodyRaw);

        // getting the body as ServiceErrorResponse works fine
        ServiceErrorResponse serBody = failure.op.getBody(ServiceErrorResponse.class);
        assertNotNull(serBody);

        // but if we read it as String we get the request body,
        // this is changed behavior because it was returning empty string before
        String stringBody = failure.op.getBody(String.class);

        // expected json representation of the error, but the request body is returned?
        assertEquals(bodyRawString, stringBody);
    }

    @Test
    public void remoteCall() throws Throwable {
        this.host = VerificationHost.create(0);
        this.host.start();

        String body = "whatever";

        Operation op = Operation
                .createPost(this.host, ExampleService.FACTORY_LINK)
                .forceRemote()
                .setBody(body);

        TestRequestSender sender = this.host.getTestRequestSender();
        TestRequestSender.FailureResponse failure = sender.sendAndWaitFailure(op);

        assertNotNull(failure.failure);
        assertEquals(Operation.MEDIA_TYPE_APPLICATION_JSON, failure.op.getContentType());
        assertEquals(Operation.STATUS_CODE_BAD_REQUEST, failure.op.getStatusCode());

        Object bodyRaw = failure.op.getBodyRaw();

        assertTrue(bodyRaw instanceof String);

        String bodyRawString = Utils.toJson(bodyRaw);

        // getting the body as ServiceErrorResponse works fine
        ServiceErrorResponse serBody = failure.op.getBody(ServiceErrorResponse.class);
        assertNotNull(serBody);

        // but if we read it as String we get the request body,
        // this is changed behavior because it was returning empty string before
        String stringBody = failure.op.getBody(String.class);

        // expected json representation of the error, but the request body is returned?
        assertEquals(bodyRawString, stringBody);
    }
}
