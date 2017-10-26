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

import java.net.URI;
import java.util.Locale;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.OperationProcessingChain.Filter;
import com.vmware.xenon.common.OperationProcessingChain.FilterReturnCode;
import com.vmware.xenon.common.OperationProcessingChain.OperationProcessingContext;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.services.common.ExampleService;

public class TestLocalizableValidationException extends BasicReusableHostTestCase {

    public static final String ERROR_MESSAGE = "Random test error message: {0}";
    public static final String ERROR_MESSAGE_GERMAN = "Random test error message in German!: argValue";
    private static final String ERROR_MESSAGE_CODE = "random.message.code";
    private static final String ARG_VALUE = "argValue";

    private static LocalizableValidationException ex =
            new LocalizableValidationException(ERROR_MESSAGE, ERROR_MESSAGE_CODE, ARG_VALUE);

    @Before
    public void setUp() throws Throwable {
        if (host.getServiceStage(TestFailingStatefulService.FACTORY_LINK) == null) {
            host.startServiceAndWait(TestFailingStatefulService.class, TestFailingStatefulService.FACTORY_LINK);
        }
    }

    @Test
    public void returnsLocalizedServiceResponseTest() throws Throwable {
        URI uri = UriUtils.buildUri(host, TestFailingStatefulService.FACTORY_LINK);

        Operation get = Operation
                .createGet(uri)
                .setReferer(host.getReferer())
                .addRequestHeader(Operation.ACCEPT_LANGUAGE_HEADER, Locale.GERMAN.getLanguage());

        TestRequestSender sender = this.host.getTestRequestSender();
        FailureResponse result = sender.sendAndWaitFailure(get);

        ServiceErrorResponse response = result.op.getErrorResponseBody();

        assertEquals(ERROR_MESSAGE_GERMAN, response.message);
        assertEquals(Operation.STATUS_CODE_BAD_REQUEST, response.statusCode);
    }

    @Test
    public void returnsLocalizedServiceResponseTestWithInnerOperation() throws Throwable {
        URI uri = UriUtils.buildUri(host, TestFailingStatefulService.FACTORY_LINK);

        Operation post = Operation
                .createPost(uri)
                .setReferer(host.getReferer())
                .addRequestHeader(Operation.ACCEPT_LANGUAGE_HEADER, Locale.GERMAN.getLanguage());

        TestRequestSender sender = this.host.getTestRequestSender();
        FailureResponse result = sender.sendAndWaitFailure(post);

        ServiceErrorResponse response = result.op.getErrorResponseBody();

        assertEquals(ERROR_MESSAGE_GERMAN, response.message);
    }

    @Test
    public void returnsLocalizedServiceResponseTestWithOperationProcessingChain() throws Throwable {
        URI uri = UriUtils.buildUri(host, TestFailingStatefulService.FACTORY_LINK);

        Operation post = Operation
                .createDelete(uri)
                .setReferer(host.getReferer())
                .addRequestHeader(Operation.ACCEPT_LANGUAGE_HEADER, Locale.GERMAN.getLanguage());

        TestRequestSender sender = this.host.getTestRequestSender();
        FailureResponse result = sender.sendAndWaitFailure(post);

        ServiceErrorResponse response = result.op.getErrorResponseBody();

        assertEquals(ERROR_MESSAGE_GERMAN, response.message);
    }

    public static class TestFailingStatefulService extends StatefulService {

        public static final String FACTORY_LINK = "/resources/failing-service";

        public TestFailingStatefulService() {
            super(ServiceDocument.class);
            toggleOption(ServiceOption.CONCURRENT_GET_HANDLING, true);
            toggleOption(ServiceOption.CONCURRENT_UPDATE_HANDLING, true);
            OperationProcessingChain processingChain = constructFailingServiceOperationProcessingChain(
                    this);
            this.setOperationProcessingChain(processingChain);
        }

        @Override
        public void handleGet(Operation get) {
            throw ex;
        }

        @Override
        public void handlePost(Operation post) {
            sendRequest(Operation
                    .createGet(getHost(), ExampleService.FACTORY_LINK)
                    .setCompletion((o, e) -> {
                        post.fail(ex);
                    }));
        }

    }

    private static OperationProcessingChain constructFailingServiceOperationProcessingChain(Service s) {
        return OperationProcessingChain.create(new Filter() {

            @Override
            public FilterReturnCode processRequest(Operation op, OperationProcessingContext context) {
                if (op.getAction() != Action.DELETE) {
                    return FilterReturnCode.CONTINUE_PROCESSING;
                }

                context.setSuspendConsumer(o -> {
                    s.sendRequest(Operation.createGet(s, TestFailingStatefulService.FACTORY_LINK)
                            .setCompletion((oo, e) -> {
                                context.getOpProcessingChain().resumeProcessingRequest(op, context,
                                        FilterReturnCode.FAILED_STOP_PROCESSING, e);
                                op.fail(ex);
                            }));
                });
                return FilterReturnCode.SUSPEND_PROCESSING;
            }
        });
    }

}
