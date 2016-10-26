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

package com.vmware.xenon.services.common;

import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.SerializedOperation;
import com.vmware.xenon.common.Utils;

public class OperationIndexService extends LuceneDocumentIndexService {

    public static final String SELF_LINK = ServiceUriPaths.CORE_OPERATION_INDEX;

    public static final String FILE_PATH = "lucene-operation-index";

    public static final long DEFAULT_EXPIRATION_INTERVAL_MICROS = TimeUnit.DAYS.toMicros(1);

    public OperationIndexService() {
        super(FILE_PATH);
    }

    @Override
    public void handleRequest(Operation op) {

        if (op.getAction() == Action.DELETE) {
            // by pass base class queuing and allow delete directly
            try {
                super.handleDeleteImpl(op);
            } catch (Throwable e) {
                op.fail(e);
            }
            return;
        }

        if (!op.hasBody() || !op.getAction().equals(Action.POST)) {
            super.handleRequest(op);
            return;
        }

        SerializedOperation state = op.getBody(SerializedOperation.class);

        if (state.documentKind != null
                && !state.documentKind.equals(SerializedOperation.KIND)) {
            op.fail(new IllegalArgumentException("documentKind is not SerializedOperation"));
            return;
        }

        state.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                DEFAULT_EXPIRATION_INTERVAL_MICROS);

        state.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        if (state.documentSelfLink == null) {
            state.documentSelfLink = state.documentUpdateTimeMicros + "";
        }

        state.documentUpdateAction = op.getAction().toString();

        UpdateIndexRequest req = new UpdateIndexRequest();
        req.description = SerializedOperation.DESCRIPTION;
        req.document = state;

        op.setBodyNoCloning(req);
        super.handleRequest(op);
    }

}
