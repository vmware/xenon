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

package com.vmware.xenon.common.test;

public enum TestProperty {
    FORCE_REMOTE,
    BASIC_AUTH,
    SINGLE_ITERATION,
    FORCE_FAILURE,
    EXPECT_FAILURE,
    CONCURRENT_SEND,
    DELETE_DURABLE_SERVICE,
    LARGE_PAYLOAD,
    BINARY_PAYLOAD,
    TEXT_RESPONSE,
    SET_EXPIRATION,
    DISABLE_CONTEXT_ID_VALIDATION,
    SET_CONTEXT_ID,
    CALLBACK_SEND,
    PERSISTED,
    BINARY_SERIALIZATION,
    HTTP2
}
