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

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.test.TestContext;

public class TestLocalizableValidationException extends BasicReusableHostTestCase {

    @Before
    public void setUp() throws Throwable {
        host.startServiceAndWait(TestFailingStatefulService.class,
                TestFailingStatefulService.FACTORY_LINK);
    }

    @Test
    public void returnsLocalizedServiceResponseTest() throws Throwable {
        TestContext ctx = testCreate(1);
        URI uri = UriUtils.buildUri(host, TestFailingStatefulService.FACTORY_LINK);
        Operation get = Operation
                .createGet(uri)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)
                .setReferer(host.getReferer())
                .addRequestHeader(Operation.ACCEPT_LANGUAGE_HEADER, "de")
                .setCompletion(
                        (o, e) -> {
                            ServiceErrorResponse response = o.getBody(ServiceErrorResponse.class);
                            if (response.message.equals(TestFailingStatefulService.ERROR_MESSAGE_GERMAN)) {
                                ctx.complete();
                            } else {
                                ctx.fail(new AssertionError(String.format("Error message expected \"%s\", but was: \"%s\"",
                                        TestFailingStatefulService.ERROR_MESSAGE_GERMAN, response.message)));
                            }
                        });

        host.send(get);
        ctx.await();
    }

    public static class TestFailingStatefulService extends StatefulService {

        public static final String ERROR_MESSAGE = "Random test error message: {0}";
        public static final String ERROR_MESSAGE_GERMAN = "Random test error message in German!: argValue";
        private static final String ERROR_MESSAGE_CODE = "random.message.code";
        private static final String ARG_VALUE = "argValue";

        public static final String FACTORY_LINK = "/resources/failing-service";

        public TestFailingStatefulService() {
            super(ServiceDocument.class);
        }

        @Override
        public void handleGet(Operation get) {
            throw new LocalizableValidationException(ERROR_MESSAGE, ERROR_MESSAGE_CODE, new String[] { ARG_VALUE });
        }

    }
}
