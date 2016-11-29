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
import static org.junit.Assert.assertNull;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.Service.Action;

public class TestValidationError extends BasicTestCase {
    private static final String ERROR_MSG_NAME_REQUIRED = "name is required";
    private TestValidationServiceState state;

    @Before
    public void prepare() throws Throwable {
        this.host.testStart(2);
        this.host.startService(
                Operation.createPost(
                        UriUtils.buildUri(host, TestValidationStatelessService.SELF_LINK))
                        .setCompletion(this.host.getCompletion()),
                new TestValidationStatelessService());
        this.host.startService(
                Operation.createPost(
                        UriUtils.buildUri(host, TestValidationFactoryService.SELF_LINK))
                        .setCompletion(this.host.getCompletion()),
                new TestValidationFactoryService());
        this.host.testWait();

        this.state = new TestValidationServiceState();
    }

    @Test
    public void testValidateOnStatefulServiceStartUp() throws Throwable {
        Operation response = doOperation(Action.POST, TestValidationFactoryService.SELF_LINK,
                this.state);
        validateServiceValidationError(response, ERROR_MSG_NAME_REQUIRED);
    }

    @Test
    public void testValidateOnStatefulServicePUT() throws Throwable {
        this.state.name = "testName";
        Operation response = doOperation(Action.POST, TestValidationFactoryService.SELF_LINK,
                this.state);
        assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());

        this.state = response.getBody(TestValidationServiceState.class);
        this.state.name = null;

        response = doOperation(Action.PUT, this.state.documentSelfLink, this.state);
        validateServiceValidationError(response, ERROR_MSG_NAME_REQUIRED);
    }

    @Test
    public void testValidateOnStatelessServicePUT() throws Throwable {
        Operation response = doOperation(Action.PUT, TestValidationStatelessService.SELF_LINK,
                this.state);
        validateServiceValidationError(response, ERROR_MSG_NAME_REQUIRED);
    }

    @Test
    public void testValidateOnStatelessServicePatchWithOperationFailHandling() throws Throwable {
        Operation response = doOperation(Action.PATCH, TestValidationStatelessService.SELF_LINK,
                this.state);
        validateServiceValidationError(response, ERROR_MSG_NAME_REQUIRED);
    }

    @Test
    public void testValidateOnStatefulServicePatchWithOperationFailHandling() throws Throwable {
        this.state.name = "testName";
        Operation response = doOperation(Action.POST, TestValidationFactoryService.SELF_LINK,
                this.state);
        assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());

        this.state = response.getBody(TestValidationServiceState.class);
        this.state.name = null;

        response = doOperation(Action.PATCH, this.state.documentSelfLink, this.state);
        validateServiceValidationError(response, ERROR_MSG_NAME_REQUIRED);
    }

    private void validateServiceValidationError(Operation response, String expectedMsg) {
        assertEquals(Operation.STATUS_CODE_BAD_REQUEST, response.getStatusCode());
        ServiceErrorResponse error = response.getBody(ServiceErrorResponse.class);
        assertEquals(Operation.STATUS_CODE_BAD_REQUEST, error.statusCode);
        assertEquals(expectedMsg, error.message);
        assertNull(error.stackTrace);
    }

    private Operation doOperation(Action action, String selfLink, TestValidationServiceState state)
            throws Throwable {
        Operation[] result = new Operation[] { null };
        host.testStart(1);
        Operation op = new Operation()
                .setAction(action)
                .setUri(UriUtils.buildUri(host, selfLink))
                .setBody(state)
                .setCompletion((o, e) -> {
                    result[0] = o;
                    host.completeIteration();
                });
        host.send(op);
        host.testWait();
        return result[0];
    }

    private static class TestValidationStatelessService extends StatelessService {
        public static final String SELF_LINK = "test-service" + UUID.randomUUID().toString();

        public TestValidationStatelessService() {
            super(TestValidationServiceState.class);
        }

        @Override
        public void handleRequest(Operation op) {
            if (Action.PUT == op.getAction()) {
                handleOperationValidationException(op);
                return;
            } else if (Action.PATCH == op.getAction()) {
                handleOperationFail(op);
                return;
            }
            super.handleRequest(op);
        }
    }

    private static class TestValidationFactoryService extends FactoryService {
        public static final String SELF_LINK = "test-factory" + UUID.randomUUID().toString();

        public TestValidationFactoryService() {
            super(TestValidationServiceState.class);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new TestValidationStatefulService();
        }

    }

    private static class TestValidationStatefulService extends StatefulService {
        public TestValidationStatefulService() {
            super(TestValidationServiceState.class);
        }

        @Override
        public void handleStart(Operation post) {
            handleOperationValidationException(post);
        }

        @Override
        public void handlePut(Operation put) {
            handleOperationValidationException(put);
        }

        @Override
        public void handlePatch(Operation patch) {
            handleOperationFail(patch);
        }

    }

    public static class TestValidationServiceState extends ServiceDocument {
        public String name;
    }

    public static void handleOperationValidationException(Operation op) {
        TestValidationServiceState state = op.getBody(TestValidationServiceState.class);
        if (state.name == null) {
            throw new IllegalArgumentException(ERROR_MSG_NAME_REQUIRED);
        }

        op.complete();
    }

    private static void handleOperationFail(Operation op) {
        TestValidationServiceState state = op.getBody(TestValidationServiceState.class);
        if (state.name == null) {
            op.fail(new IllegalArgumentException(ERROR_MSG_NAME_REQUIRED));
            return;
        }

        op.complete();
    }

}
